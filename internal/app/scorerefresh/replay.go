package scorerefresh

// replay.go — Pure one-pass O(M) replay of an agent's feedback history.
// Computes reputationScore (with penalty baked in) and delta snapshots at
// T-30d, T-7d, T-24h milestones, yielding AgentScoreStats in a single pass.

import (
	"math"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
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
//   - delta = scoreNow − snapshotAtMilestone.
func replayAgent(
	chainID int64,
	agentID string,
	feedbacks []feedbackrepo.FeedbackRecord,
	now int64,
	cfg scoring.FormulaConfig,
) scorestats.AgentScoreStats {
	// Milestones in ascending chronological order: T-30d, T-7d, T-24h.
	milestones := [3]int64{now - 30*86400, now - 7*86400, now - 86400}

	lambda := scoring.ComputeDecayRate(cfg.Alpha, cfg.TBaseDays)
	rep := 0.0
	lastTs := int64(0)
	mIdx := 0
	consecFails := int64(0)

	var snapshots [3]float64 // rep value (interpolated) at each milestone
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
			rep -= scoring.ComputePenalty(consecFails, cfg.Gamma, cfg.Theta)
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

	return scorestats.AgentScoreStats{
		ChainID:     chainID,
		AgentID:     agentID,
		Score:       rep,
		Delta24h:    rep - snapshots[2],
		Delta7d:     rep - snapshots[1],
		Delta30d:    rep - snapshots[0],
		Consistency: computeConsistency(eventScores),
		ComputedAt:  now,
	}
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
