package pipeline

// cluster_a.go — Cụm A: Scoring Core + Adjustments.
// Parameters varied: α, β, K, T_base, γ, θ, S_base, LowConfThreshold,
// LowConfMultiplier, ServiceBonusContent, ServiceBonusProof.

import (
	"math"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterAParamSpecs returns the canonical Cluster-A parameter list with
// ±50% sweep ranges around their defaults.
//
// Adjustment params (LowConf*, ServiceBonus*) sweep over discrete-ish ranges
// chosen to bracket plausible alternative weightings.
func ClusterAParamSpecs() []runner.ParamSpec {
	def := scoring.DefaultFormulaConfig()
	return []runner.ParamSpec{
		{Name: "Alpha", Default: def.Alpha, Low: def.Alpha * 0.5, High: def.Alpha * 1.5},
		{Name: "Beta", Default: def.Beta, Low: def.Beta * 0.5, High: def.Beta * 1.5},
		{Name: "K", Default: def.K, Low: def.K * 0.5, High: def.K * 1.5},
		{Name: "TBaseDays", Default: def.TBaseDays, Low: def.TBaseDays * 0.5, High: def.TBaseDays * 1.5},
		{Name: "Gamma", Default: def.Gamma, Low: def.Gamma * 0.5, High: def.Gamma * 1.5},
		{Name: "Theta", Default: def.Theta, Low: def.Theta * 0.5, High: def.Theta * 1.5},
		{Name: "SBase", Default: def.SBase, Low: -50.0, High: 50.0},
		{Name: "LowConfThreshold", Default: 0.60, Low: 0.40, High: 0.80},
		{Name: "LowConfMultiplier", Default: 0.50, Low: 0.25, High: 0.75},
		{Name: "ServiceBonusContent", Default: 0.20, Low: 0.10, High: 0.40},
		{Name: "ServiceBonusProof", Default: 0.20, Low: 0.10, High: 0.40},
	}
}

// DefaultClusterAConfig returns the baseline config (Default values for every spec).
func DefaultClusterAConfig() map[string]float64 {
	cfg := make(map[string]float64)
	for _, p := range ClusterAParamSpecs() {
		cfg[p.Name] = p.Default
	}
	return cfg
}

// ClusterARecompute returns a RecomputeFn closing over the snapshot data.
// For each agent, it iterates its feedbacks, computes wᵢ (with adjustments),
// the time-decay factor relative to `now`, and aggregates into reputationScore.
//
// Note: Scoring formula here mirrors scoring.ApplyTaskScore but evaluated in batch
// (replay) rather than incrementally, so changing T_base/decay etc. retroactively
// affects all historic feedbacks. This matches "what would the score have been if
// these parameters had been used all along" — the question SA wants to answer.
//
// The ServiceBonus adjustments mirror scoring.ServiceFeedbackBonus, which keys the
// content/proof bonuses off the *presence* of content and proof signals rather than
// applying unconditionally. Snapshots built before Plan-C carry zero quality signals,
// so the bonus terms have no effect there; once signals are populated the bonus
// parameters become live.
func ClusterARecompute(data snapshot.SnapshotData, nowUnix int64) runner.RecomputeFn {
	// Group feedbacks per agent once for fast inner loop.
	byAgent := make(map[string][]snapshot.FeedbackSnapshot, len(data.Agents))
	for _, fb := range data.Feedbacks {
		byAgent[fb.AgentID] = append(byAgent[fb.AgentID], fb)
	}

	return func(cfg map[string]float64) map[string]float64 {
		formula := scoring.FormulaConfig{
			Alpha:     cfg["Alpha"],
			Beta:      cfg["Beta"],
			K:         cfg["K"],
			TBaseDays: cfg["TBaseDays"],
			Gamma:     cfg["Gamma"],
			Theta:     cfg["Theta"],
			SBase:     cfg["SBase"],
		}
		lowConfThr := cfg["LowConfThreshold"]
		lowConfMul := cfg["LowConfMultiplier"]
		bonusContent := cfg["ServiceBonusContent"]
		bonusProof := cfg["ServiceBonusProof"]

		out := make(map[string]float64, len(data.Agents))
		for _, a := range data.Agents {
			score := formula.SBase
			var consecFails int64
			fbs := byAgent[a.ID]
			for _, fb := range fbs {
				wi := scoring.ComputeWi(fb.PriceUSDC, formula.Alpha, formula.Beta, formula.K)
				// Apply LowConf adjustment.
				if fb.ClassifierConfidence < lowConfThr {
					wi *= lowConfMul
				}
				// Service bonus — gated on content/proof presence, mirroring
				// scoring.ServiceFeedbackBonus. Content ≈ reasoning/breakdown present;
				// proof ≈ proof-of-payment present.
				if fb.Category == "service_feedback" {
					if fb.ReasoningLen > 0 || fb.HasRatingBreakdown {
						wi += bonusContent
					}
					if fb.HasProofOfPayment {
						wi += bonusProof
					}
				}

				// Per-feedback decay relative to nowUnix.
				lambda := scoring.ComputeDecayRate(math.Max(wi, formula.Alpha), formula.TBaseDays)
				deltaDays := float64(nowUnix-fb.Timestamp) / 86400.0
				if deltaDays < 0 {
					deltaDays = 0
				}
				decay := scoring.ComputeDecayFactor(lambda, deltaDays)

				// Add the feedback's contribution. vi - 0.40 is the centered validation.
				score += wi * (fb.ValueNormalized - 0.40) * decay

				// Consecutive-fail penalty.
				if fb.ValueNormalized < 0.40 {
					consecFails++
					score -= scoring.ComputePenalty(consecFails, formula.Gamma, formula.Theta)
				} else {
					consecFails = 0
				}
			}
			// Clamp to (-∞, 1000] per §3.1.
			if score > 1000 {
				score = 1000
			}
			out[a.ID] = score
		}
		return out
	}
}
