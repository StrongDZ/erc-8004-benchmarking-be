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
	"erc-8004-benchmarking-be/internal/domain/serviceendpoint"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

// reviewerCounter holds a reviewer wallet's derived valid/junk verdict tally for one
// replay pass, keyed (by the caller) on the wallet document _id. Aggregated across all
// agents in runCycle, then SET onto the wallet via BulkSetFeedbackCounters.
type reviewerCounter struct {
	valid int64
	junk  int64
}

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
//
// Alongside AgentScoreStats it returns, for the score-refresh cycle to persist:
//   - grade backfills: rows whose stored (verdict, wi, qualityScore) differ from the
//     freshly graded values (the API reads the stored grade);
//   - reviewerTally: per reviewer-wallet valid/junk verdict counts keyed by wallet _id.
//
// GradeFeedback is computed exactly once per (non-others) feedback and reused for the
// backfill diff, the reviewer tally, and — for quality feedback — the mass weight.
func replayAgent(
	ctx context.Context,
	ag *agentrepo.AgentDocument,
	feedbacks []feedbackrepo.FeedbackRecord,
	now int64,
	formulaCfg scoring.FormulaConfig,
	offchainStatusByEndpoint map[string]int,
	trustBatch *WalletTrustBatch,
	publisherProvider scoring.PublisherScoreProvider,
	compositeWeights scoring.CompositeWeights,
	complianceWeights scoring.ComplianceWeights,
	qwCfg scoring.QualityWeightConfig,
) (scorestats.AgentScoreStats, []feedbackrepo.GradeBackfill, map[string]reviewerCounter) {
	chainID := ag.ChainID
	agentID := ag.AgentID

	// Milestones in ascending chronological order: T-30d, T-7d, T-24h.
	milestones := [3]int64{now - 30*86400, now - 7*86400, now - 86400}

	lambda := scoring.ComputeDecayRate(formulaCfg.TBaseDays)
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
	serviceIndex := serviceendpoint.BuildIndex(ag.Services)
	type serviceAccumulator struct {
		meta     serviceendpoint.ServiceMeta
		a        float64
		b        float64
		lastTs   int64
		updateAt int64
		consec   int64
		total    int64
		passed   int64
		failed   int64
	}
	serviceAcc := make(map[string]*serviceAccumulator, len(serviceIndex))
	for key, meta := range serviceIndex {
		serviceAcc[key] = &serviceAccumulator{meta: meta}
	}

	var snapA, snapB [3]float64
	var snapFails [3]int64
	var eventScores []float64

	// Cycle-side outputs: grade backfills for changed rows + reviewer verdict tally.
	var backfills []feedbackrepo.GradeBackfill
	reviewerTally := make(map[string]reviewerCounter)

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
		// Only revoked feedback is skipped wholesale. Self-feedback (IsSelfFeedback==true)
		// flows through and is graded as "self"/junk (Gated:true, Wi:0) for the reviewer
		// tally — restoring the anti-self-dealing penalty — but remains excluded from the
		// agent's reputation mass via the quality-category guard below.
		if fb.IsRevoked {
			continue
		}

		effCat := feedbackrepo.EffectiveCategory(fb)

		// Adoption: count distinct client addresses in the recent window across
		// scored-or-relevant categories (quality + quantity), so heavily-used
		// agents whose feedback is all quantity still earn breadth credit. Windowed to the
		// last adoptionWindowDays so Adoption reflects current (not all-time) breadth.
		if fb.Timestamp >= adoptionWindowStart && isAdoptionCategory(effCat) {
			distinct[strings.ToLower(fb.ClientAddress)] = struct{}{}
		}

		// "others" is unresolved (LLM never ran in this replay-only pipeline): no grade,
		// no backfill, no reviewer tally, no mass.
		if effCat == string(classifier.CategoryOthers) {
			continue
		}

		// Grade exactly once; the result drives the backfill diff, the reviewer tally,
		// and (for quality) the mass weight below.
		g := scoring.GradeFeedback(fb, qwCfg)

		// Backfill rows whose stored grade is stale vs the freshly computed one. The API
		// reads stored wi/verdict, so this keeps storage in sync with the replay engine.
		if g.Verdict != fb.ValidationVerdict || g.Wi != fb.Wi || g.QualityScore != fb.QualityScore {
			backfills = append(backfills, feedbackrepo.GradeBackfill{
				ID:           fb.ID,
				Wi:           g.Wi,
				QualityScore: g.QualityScore,
				Verdict:      g.Verdict,
				Reason:       g.Reason,
				ComputedAt:   now,
			})
		}

		// Reviewer tally: a non-gated verdict counts as valid, anything gated as junk.
		wid := walletrepo.WalletDocumentID(fb.ChainID, utils.NormalizeAddress(fb.ClientAddress))
		rc := reviewerTally[wid]
		if g.Gated {
			rc.junk++
		} else {
			rc.valid++
		}
		reviewerTally[wid] = rc

		// Only quality feedback contributes to the reputation mass.
		if effCat != string(classifier.CategoryQuality) {
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

		// Decay to this event's timestamp, then add the contribution. The base weight is
		// the freshly graded g.Wi (not stored fb.Wi); the WalletTrust multiplier is applied
		// on top, exactly as before — only the source of the base weight changed.
		decayMass(ts)
		wi := scoring.EffectiveWi(g.Wi, trustBatch.TrustScore(fb.ChainID, fb.ClientAddress))
		a += wi * vi
		b += wi

		if vi < 0.40 {
			consecFails++
			totalFailed++
		} else {
			consecFails = scoring.DecayConsecutiveFails(consecFails)
			totalPassed++
		}

		eventScores = append(eventScores, wi*vi)
		if matched, ok := serviceendpoint.MatchService(ag.Services, fb.Endpoint); ok {
			acc := serviceAcc[serviceendpoint.Normalize(matched.Endpoint)]
			if acc == nil {
				continue
			}
			if acc.lastTs < ts {
				acc.a, acc.b = scoring.DecayMass(acc.a, acc.b, acc.lastTs, ts, formulaCfg)
				acc.lastTs = ts
			}
			acc.a += wi * vi
			acc.b += wi
			acc.updateAt = ts
			acc.total++
			if vi < 0.40 {
				acc.consec++
				acc.failed++
			} else {
				acc.consec = scoring.DecayConsecutiveFails(acc.consec)
				acc.passed++
			}
		}
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
	svcResult, pubScore, pubPresent, compScore := computeComponentScores(
		ctx, ag, offchainStatusByEndpoint, publisherProvider, complianceWeights,
	)
	composite := scoring.ComputeCompositeFromStats(reputation, adoption, svcResult.Score, pubScore, compScore, qualityPresent, pubPresent, compositeWeights)
	serviceScores := make([]scorestats.ServiceReputationStats, 0, len(ag.Services))
	seenService := make(map[string]struct{}, len(ag.Services))
	for _, svc := range ag.Services {
		key := serviceendpoint.Normalize(svc.Endpoint)
		if key == "" {
			continue
		}
		if _, dup := seenService[key]; dup {
			continue
		}
		seenService[key] = struct{}{}
		acc, ok := serviceAcc[key]
		if !ok {
			continue
		}
		aNow, bNow := scoring.DecayMass(acc.a, acc.b, acc.lastTs, now, formulaCfg)
		serviceScores = append(serviceScores, scorestats.ServiceReputationStats{
			Name:             acc.meta.Name,
			Endpoint:         acc.meta.Endpoint,
			ReputationScore:  scoring.ComputeReputationScore(aNow, bNow, acc.consec, formulaCfg.C, formulaCfg.Gamma, formulaCfg.Theta),
			WeightedScoreSum: aNow,
			WeightMass:       bNow,
			ScoreUpdateAt:    acc.updateAt,
			ConsecutiveFails: acc.consec,
			TotalTasks:       acc.total,
			TotalPassed:      acc.passed,
			TotalFailed:      acc.failed,
		})
	}

	// Composite-based deltas. Approximation: adoption / S / P / C are held constant at
	// current values (no historical snapshots retained); only the reputation component
	// varies across the window, which is the dominant short-term delta driver.
	snapComposite := func(i int) float64 {
		repAt := scoring.ComputeReputationScore(snapA[i], snapB[i], snapFails[i], formulaCfg.C, formulaCfg.Gamma, formulaCfg.Theta)
		return scoring.ComputeCompositeFromStats(repAt, adoption, svcResult.Score, pubScore, compScore, snapB[i] > 0, pubPresent, compositeWeights)
	}

	stats := scorestats.AgentScoreStats{
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
		PublisherPresent: pubPresent,
		ComplianceScore:  compScore,
		ServiceWarnings:  svcResult.Warnings,
		ServiceScores:    serviceScores,
		ComputedAt:       now,
	}
	return stats, backfills, reviewerTally
}

// isAdoptionCategory reports whether a feedback category counts toward the Adoption
// (distinct-client breadth) signal: quality and quantity feedback (not junk/others).
func isAdoptionCategory(category string) bool {
	switch category {
	case string(classifier.CategoryQuality), string(classifier.CategoryQuantity):
		return true
	default:
		return false
	}
}

// computeComponentScores returns (svcResult, publisher, publisherPresent, compliance) for one agent.
// Requires preloaded offchain status map for that agent's services.
func computeComponentScores(
	ctx context.Context,
	ag *agentrepo.AgentDocument,
	offchainStatusByEndpoint map[string]int,
	publisherProvider scoring.PublisherScoreProvider,
	complianceWeights scoring.ComplianceWeights,
) (svcResult scoring.ServicesScoreResult, publisher float64, publisherPresent bool, compliance float64) {
	// Services: build health checks from declared services using the preloaded offchain status.
	checks := make([]scoring.ServiceHealthCheck, 0, len(ag.Services))
	for _, s := range ag.Services {
		checks = append(checks, scoring.ServiceHealthCheck{
			Name:   s.Name,
			Status: offchainStatusByEndpoint[s.Endpoint],
		})
	}
	svcResult = scoring.ComputeServicesScore(checks)

	// Publisher: pluggable provider (currently NeutralPublisherProvider returning 50/true).
	publisher, publisherPresent = publisherProvider.Score(ctx, ag.Owner, ag.ChainID)

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

	return svcResult, publisher, publisherPresent, compliance
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
