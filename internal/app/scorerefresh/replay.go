package scorerefresh

// replay.go — Pure one-pass O(M) replay of an agent's feedback history.
// Computes reputationScore (with penalty baked in) and delta snapshots at
// T-30d, T-7d, T-24h milestones, yielding AgentScoreStats in a single pass.
// Also computes the 4-component breakdown (services/publisher/compliance/repNorm)
// and composite score each cycle.

import (
	"context"
	"math"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
)

const consistencyWindow = 20

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
	rep := 0.0
	lastTs := int64(0)
	mIdx := 0
	consecFails := int64(0)

	var snapshots [3]float64 // raw rep value (interpolated) at each milestone
	var eventScores []float64

	for _, fb := range feedbacks {
		if fb.IsRevoked || fb.IsSelfFeedback {
			continue
		}
		if fb.Classification.Rule.Category != string(classifier.CategoryService) {
			continue
		}

		ts := fb.Timestamp

		// Recompute vi at runtime from raw value + stored scale (vi is not persisted).
		real, realOK := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals))
		if !realOK {
			continue
		}
		vi := classifier.NormalizeValueWithScale(real, fb.ValueScale)

		// Record milestones that fall strictly before this event.
		// Snapshot = rep decayed from lastTs to the milestone (score just before this event).
		// When lastTs == 0 (no prior service events), rep == 0, so snapshots stay 0 ("thì thôi").
		for mIdx < 3 && milestones[mIdx] < ts {
			mt := milestones[mIdx]
			if lastTs < mt {
				deltaDays := float64(mt-lastTs) / 86400.0
				rep *= math.Exp(-lambda * deltaDays)
				lastTs = mt
			}
			snapshots[mIdx] = rep
			mIdx++
		}

		// Decay to this event's timestamp.
		if lastTs < ts {
			deltaDays := float64(ts-lastTs) / 86400.0
			rep *= math.Exp(-lambda * deltaDays)
			lastTs = ts
		}

		// Apply event contribution: effectiveVi = vi - 0.40.
		effectiveVi := vi - 0.40
		rep += fb.Wi * effectiveVi

		// If fail (vi < 0.40): also bake in the progressive penalty.
		if vi < 0.40 {
			consecFails++
			rep -= scoring.ComputePenalty(consecFails, formulaCfg.Gamma, formulaCfg.Theta)
		} else {
			consecFails = 0
		}

		eventScores = append(eventScores, fb.Wi*effectiveVi)
	}

	// Advance any remaining milestones after the last event.
	for mIdx < 3 {
		mt := milestones[mIdx]
		if lastTs < mt {
			deltaDays := float64(mt-lastTs) / 86400.0
			rep *= math.Exp(-lambda * deltaDays)
			lastTs = mt
		}
		snapshots[mIdx] = rep
		mIdx++
	}

	// Final decay to now.
	if lastTs < now {
		deltaDays := float64(now-lastTs) / 86400.0
		rep *= math.Exp(-lambda * deltaDays)
	}

	// Compute 4-component breakdown and composite score.
	svcScore, pubScore, compScore, repNorm := computeComponentScores(
		ctx, ag, rep, offchainStatusByEndpoint,
		publisherProvider, complianceWeights,
	)
	composite := scoring.ComputeCompositeScore(repNorm, svcScore, pubScore, compScore, compositeWeights)

	// Compute composite-based deltas.
	// Approximation: S/P/C components are assumed constant at current values because
	// we don't retain historical snapshots for those components. Only the reputation
	// component varies across the snapshot window, which is the dominant driver of
	// short-term delta anyway. This means delta accurately tracks the reputation
	// component's contribution to composite change.
	snap30dComposite := scoring.ComputeCompositeScore(
		scoring.NormalizeReputation(snapshots[0]), svcScore, pubScore, compScore, compositeWeights,
	)
	snap7dComposite := scoring.ComputeCompositeScore(
		scoring.NormalizeReputation(snapshots[1]), svcScore, pubScore, compScore, compositeWeights,
	)
	snap24hComposite := scoring.ComputeCompositeScore(
		scoring.NormalizeReputation(snapshots[2]), svcScore, pubScore, compScore, compositeWeights,
	)

	return scorestats.AgentScoreStats{
		ChainID:         chainID,
		AgentID:         agentID,
		Score:           rep,
		Delta24h:        composite - snap24hComposite,
		Delta7d:         composite - snap7dComposite,
		Delta30d:        composite - snap30dComposite,
		Consistency:     computeConsistency(eventScores),
		CompositeScore:  composite,
		ReputationNorm:  repNorm,
		ServicesScore:   svcScore,
		PublisherScore:  pubScore,
		ComplianceScore: compScore,
		ComputedAt:      now,
	}
}

// computeComponentScores returns (services, publisher, compliance, normalizedReputation)
// for one agent. Requires preloaded offchain status map for that agent's services.
func computeComponentScores(
	ctx context.Context,
	ag *agentrepo.AgentDocument,
	rawRep float64,
	offchainStatusByEndpoint map[string]int,
	publisherProvider scoring.PublisherScoreProvider,
	complianceWeights scoring.ComplianceWeights,
) (services, publisher, compliance, repNorm float64) {
	repNorm = scoring.NormalizeReputation(rawRep)

	// Services: count how many declared service endpoints have a successful JSON fetch.
	total := len(ag.Services)
	healthy := 0
	for _, s := range ag.Services {
		if offchainStatusByEndpoint[s.Endpoint] == offchain.StatusFetchedJSON {
			healthy++
		}
	}
	services = scoring.ComputeServicesScore(total, healthy)

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

	return services, publisher, compliance, repNorm
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
