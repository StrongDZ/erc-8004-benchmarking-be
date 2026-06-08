package scorerefresh

// replay.go — Pure one-pass O(M) replay of an agent's feedback history.
// Rebuilds the v2 weighted-mean mass accumulators A = Σ wᵢ·dᵢ·vᵢ and B = Σ wᵢ·dᵢ,
// the distinct-client count (Adoption), and the consecutive-fail streak, then derives
// reputation (Quality·Confidence·Reliability) and delta snapshots at T-30d, T-7d, T-24h.
// Also computes the breakdown (adoption/services/publisher/compliance) and the
// renormalized composite score each cycle, yielding AgentScoreStats in a single pass.

import (
	"context"
	"math"
	"strings"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
)

const consistencyWindow = 20

// adoptionWindowDays bounds the distinct-client (MonthUniqueUsers) count to recent
// activity, so Adoption reflects CURRENT breadth rather than all-time (and fades when an
// agent goes quiet, complementing the Confidence-decay anti-squatting on reputation).
const adoptionWindowDays = 30

// replayAgent computes AgentScoreStats for one agent by replaying their feedback history.
// feedbacks must be sorted chronologically (blockNumber asc, logIndex asc).
// now is the Unix timestamp to compute to.
//
// Milestone snapshot logic:
//   - Snapshot at milestone T-X = acc decayed from last event before the milestone to T-X.
//   - If no service feedback exists before a milestone, snapshot = 0 ("thì thôi").
//   - delta = compositeNow − compositeAtMilestone (approximated, see note in body).
func replayAgent(
	ctx context.Context,
	ag *agentrepo.AgentDocument,
	feedbacks []feedbackrepo.FeedbackRecord,
	now int64,
	formulaCfg scoring.FormulaConfig,
	offchainStatusByEndpoint map[string]int,
	publisherProvider scoring.PublisherScoreProvider,
	compositeWeights scoring.CompositeWeights,
	complianceWeights scoring.ComplianceWeights,
) scorestats.AgentScoreStats {
	chainID := ag.ChainID
	agentID := ag.AgentID

	// Milestones in ascending chronological order: T-30d, T-7d, T-24h.
	milestones := [3]int64{now - 30*86400, now - 7*86400, now - 86400}

	lambda := scoring.ComputeDecayRate(formulaCfg.Alpha, formulaCfg.TBaseDays)
	// v2 weighted-mean mass accumulators: A = Σ wᵢ·dᵢ·vᵢ, B = Σ wᵢ·dᵢ.
	a := 0.0
	b := 0.0
	lastTs := int64(0)
	mIdx := 0
	consecFails := int64(0)
	totalTasks := int64(0)
	totalPassed := int64(0)
	totalFailed := int64(0)
	lastEventTs := int64(0)
	distinct := make(map[string]struct{})
	adoptionWindowStart := now - adoptionWindowDays*86400

	var snapA, snapB [3]float64
	var snapFails [3]int64
	var eventScores []float64

	// decayMass decays both mass accumulators forward to toTs (in place).
	decayMass := func(toTs int64) {
		if lastTs < toTs {
			f := math.Exp(-lambda * float64(toTs-lastTs) / 86400.0)
			a *= f
			b *= f
			lastTs = toTs
		}
	}

	for _, fb := range feedbacks {
		if fb.IsRevoked || fb.IsSelfFeedback {
			continue
		}

		// Adoption: count distinct client addresses in the recent window across
		// scored-or-relevant categories (service + config + app_specific), so heavily-used
		// agents whose feedback is all config/app still earn breadth credit. Windowed to the
		// last adoptionWindowDays so Adoption reflects current (not all-time) breadth.
		if fb.Timestamp >= adoptionWindowStart && isAdoptionCategory(feedbackrepo.EffectiveCategory(fb)) {
			distinct[strings.ToLower(fb.ClientAddress)] = struct{}{}
		}

		if feedbackrepo.EffectiveCategory(fb) != string(classifier.CategoryService) {
			continue
		}

		// Recompute vi at runtime from raw value + stored scale (vi is not persisted).
		real, realOK := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals))
		if !realOK {
			continue
		}
		vi := classifier.NormalizeValueWithScale(real, fb.ValueScale)

		ts := fb.Timestamp
		lastEventTs = ts
		totalTasks++

		// Record milestones that fall strictly before this event (mass + streak just
		// before this event). When lastTs == 0 (no prior events), mass stays 0.
		for mIdx < 3 && milestones[mIdx] < ts {
			decayMass(milestones[mIdx])
			snapA[mIdx] = a
			snapB[mIdx] = b
			snapFails[mIdx] = consecFails
			mIdx++
		}

		// Decay to this event's timestamp, then add the contribution.
		decayMass(ts)
		a += fb.Wi * vi
		b += fb.Wi

		if vi < 0.40 {
			consecFails++
			totalFailed++
		} else {
			consecFails = 0
			totalPassed++
		}

		eventScores = append(eventScores, fb.Wi*vi)
	}

	// Advance any remaining milestones after the last event.
	for mIdx < 3 {
		decayMass(milestones[mIdx])
		snapA[mIdx] = a
		snapB[mIdx] = b
		snapFails[mIdx] = consecFails
		mIdx++
	}

	// Final decay to now.
	decayMass(now)

	reputation := scoring.ComputeReputationScore(a, b, consecFails, formulaCfg.C, formulaCfg.Gamma, formulaCfg.Theta)
	qualityPresent := b > 0
	adoption := scoring.ComputeAdoption(len(distinct), formulaCfg.AdoptionURef)

	// Compute the remaining breakdown components and the renormalized composite.
	svcResult, pubScore, compScore := computeComponentScores(
		ctx, ag, offchainStatusByEndpoint, publisherProvider, complianceWeights,
	)
	composite := scoring.ComputeCompositeFromStats(reputation, adoption, svcResult.Score, pubScore, compScore, qualityPresent, compositeWeights)

	// Composite-based deltas. Approximation: adoption / S / P / C are held constant at
	// current values (no historical snapshots retained); only the reputation component
	// varies across the window, which is the dominant short-term delta driver.
	snapComposite := func(i int) float64 {
		repAt := scoring.ComputeReputationScore(snapA[i], snapB[i], snapFails[i], formulaCfg.C, formulaCfg.Gamma, formulaCfg.Theta)
		return scoring.ComputeCompositeFromStats(repAt, adoption, svcResult.Score, pubScore, compScore, snapB[i] > 0, compositeWeights)
	}

	return scorestats.AgentScoreStats{
		ChainID:          chainID,
		AgentID:          agentID,
		ReputationScore:  reputation,
		WeightedScoreSum: a,
		WeightMass:       b,
		Delta24h:         composite - snapComposite(2),
		Delta7d:          composite - snapComposite(1),
		Delta30d:         composite - snapComposite(0),
		Consistency:      computeConsistency(eventScores),
		ScoreUpdateAt:    lastEventTs,
		ConsecutiveFails: consecFails,
		TotalTasks:       totalTasks,
		TotalPassed:      totalPassed,
		TotalFailed:      totalFailed,
		MonthUniqueUsers: len(distinct),
		CompositeScore:   composite,
		ReputationNorm:   reputation,
		AdoptionScore:    adoption,
		ServicesScore:    svcResult.Score,
		PublisherScore:   pubScore,
		ComplianceScore:  compScore,
		ServiceWarnings:  svcResult.Warnings,
		ComputedAt:       now,
	}
}

// isAdoptionCategory reports whether a feedback category counts toward the Adoption
// (distinct-client breadth) signal: service, config, and app-specific feedback.
func isAdoptionCategory(category string) bool {
	switch category {
	case string(classifier.CategoryService), string(classifier.CategoryConfig), string(classifier.CategoryApp):
		return true
	default:
		return false
	}
}

// computeComponentScores returns (svcResult, publisher, compliance) for one agent.
// Requires preloaded offchain status map for that agent's services.
func computeComponentScores(
	ctx context.Context,
	ag *agentrepo.AgentDocument,
	offchainStatusByEndpoint map[string]int,
	publisherProvider scoring.PublisherScoreProvider,
	complianceWeights scoring.ComplianceWeights,
) (svcResult scoring.ServicesScoreResult, publisher, compliance float64) {
	// Services: build health checks from declared services using the preloaded offchain status.
	checks := make([]scoring.ServiceHealthCheck, 0, len(ag.Services))
	for _, s := range ag.Services {
		checks = append(checks, scoring.ServiceHealthCheck{
			Name:   s.Name,
			Status: offchainStatusByEndpoint[s.Endpoint],
		})
	}
	svcResult = scoring.ComputeServicesScore(checks)

	// Publisher: pluggable provider (currently NeutralPublisherProvider returning 50).
	publisher = publisherProvider.Score(ctx, ag.Owner, ag.ChainID)

	// Compliance: field-presence scoring against ERC-8004 card spec.
	compliance = scoring.ComputeComplianceScore(scoring.ComplianceInput{
		AgentURI:       ag.AgentURI,
		Type:           ag.Type,
		Name:           ag.Name,
		Description:    ag.Description,
		Image:          ag.Image,
		Services:       len(ag.Services),
		Registrations:  len(ag.Registrations),
		SupportedTrust: len(ag.SupportedTrust),
		X402Support:    ag.X402Support,
		AgentWallet:    ag.AgentWallet,
		CardUpdatedAt:  ag.CardUpdatedAt,
	}, complianceWeights)

	return svcResult, publisher, compliance
}

// computeConsistency returns a [0,1] consistency metric from event scores.
// 1 = all event scores identical; lower = more variable.
func computeConsistency(scores []float64) float64 {
	n := len(scores)
	if n == 0 {
		return 0.5
	}
	if n > consistencyWindow {
		scores = scores[n-consistencyWindow:]
		n = consistencyWindow
	}
	if n == 1 {
		return 1.0
	}
	var sum, sumSq float64
	for _, s := range scores {
		sum += s
		sumSq += s * s
	}
	mean := sum / float64(n)
	variance := (sumSq / float64(n)) - mean*mean
	if variance < 0 {
		variance = 0
	}
	stddev := math.Sqrt(variance)
	return clamp01(1.0 - stddev/200.0)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
