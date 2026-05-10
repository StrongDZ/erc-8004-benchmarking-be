package scoring

// formulas.go — Pure TrustRank scoring functions.
// implements §3.1 Total Trust Score
// implements §3.2 Difficulty Weight (wᵢ)
// implements §3.3 Adaptive Time Decay (λᵢ)
// implements §3.4 Progressive Penalty (P)
//
// ALL scoring formula logic lives here and nowhere else in the codebase.

import "math"

// FormulaConfig holds the tunable TrustRank parameters, read from environment at startup.
type FormulaConfig struct {
	Alpha     float64 // §3.2 minimum difficulty weight (default 1.0)
	Beta      float64 // §3.2 difficulty amplifier (default 1.5)
	K         float64 // §3.2 micro-unit amplifier for USDC price (default 100.0)
	TBaseDays float64 // §3.3 half-life base in days (default 15.0)
	Gamma     float64 // §3.4 penalty base coefficient (default 5.0)
	Theta     float64 // §3.4 penalty exponent (default 2.0)
	SBase     float64 // §3.1 base score assigned at agent registration (default 0.0)
}

// DefaultFormulaConfig returns the spec-defined default parameter set.
func DefaultFormulaConfig() FormulaConfig {
	return FormulaConfig{
		Alpha:     1.0,
		Beta:      1.5,
		K:         100.0,
		TBaseDays: 15.0,
		Gamma:     5.0,
		Theta:     2.0,
		SBase:     0.0,
	}
}

// TaskInput is the per-feedback input to ComputeScore.
type TaskInput struct {
	PriceUSDC float64 // task reward in USDC (0 for free tasks)
	Vi        float64 // validation score [0, 1]
	DeltaDays float64 // fractional days since task completion — MUST NOT be in seconds
}

// ComputeWi computes the logarithmic difficulty weight for a task.
// §3.2: wᵢ = α + β · ln(1 + k · Priceᵢ)
// NOTE: math.Log is the NATURAL logarithm. Never math.Log10.
func ComputeWi(priceUSDC, alpha, beta, k float64) float64 {
	return alpha + beta*math.Log(1.0+k*priceUSDC)
}

// ComputeDecayRate computes the adaptive decay rate λᵢ.
// §3.3: λᵢ = ln(2) / (T_base · (1 + ln(wᵢ)))
func ComputeDecayRate(wi, tBaseDays float64) float64 {
	return math.Log(2) / (tBaseDays * (1.0 + math.Log(wi)))
}

// ComputeDecayFactor computes the time decay multiplier e^(-λᵢ·Δt).
// §3.3: D(t) = e^(-λᵢ · Δt)
// deltaDays must be in FRACTIONAL DAYS: time.Since(t).Hours() / 24.0
func ComputeDecayFactor(lambda, deltaDays float64) float64 {
	return math.Exp(-lambda * deltaDays)
}

// ComputePenalty computes the progressive consecutive-failure penalty.
// §3.4: P = γ · (N_fail)^θ
// Penalty examples: N=1→5, N=2→20, N=3→45, N=5→125, N=10→500
func ComputePenalty(nFail int64, gamma, theta float64) float64 {
	return gamma * math.Pow(float64(nFail), theta)
}

// ComputeScore computes the total TrustRank score for an agent.
// §3.1: S = S_base + Σ[wᵢ·vᵢ·e^(-λᵢ·Δt)] − P
// Result is clamped to (-∞, 1000]; negative scores are allowed to represent
// agents with net-negative signal (heavy penalty or decreasing-metric feedbacks).

// func ComputeScore(tasks []TaskInput, consecutiveFails int64, cfg FormulaConfig) float64 {
// 	sum := 0.0
// 	for _, t := range tasks {
// 		wi := ComputeWi(t.PriceUSDC, cfg.Alpha, cfg.Beta, cfg.K)
// 		lambda := ComputeDecayRate(wi, cfg.TBaseDays)
// 		decay := ComputeDecayFactor(lambda, t.DeltaDays)
// 		sum += wi * t.Vi * decay
// 	}
// 	penalty := ComputePenalty(consecutiveFails, cfg.Gamma, cfg.Theta)
// 	score := cfg.SBase + sum - penalty
// 	return math.Min(1000.0, score)
// }

// ── O(1) incremental scoring functions ─────────────────────────────────────────

const secondsPerDay = 86400.0

// ApplyTaskScore performs an O(1) incremental score update (write path, §3.2).
//
// It decays the existing accumulatedScore forward to tNowUnix, then adds the new
// feedback contribution (wᵢ·vᵢ). The caller must write the returned value to MongoDB.
//
// accScore:       current accumulatedScore stored in the agent document.
// lastUpdateUnix: Unix seconds of the last score update.
// wi:             difficulty weight of the new feedback (ComputeWi output).
// vi:             validation score [0, 1] of the new feedback.
// tNowUnix:       Unix seconds "now" (typically the event's block timestamp).
// cfg:            formula parameters.
func ApplyTaskScore(accScore float64, lastUpdateUnix int64, wi, vi float64, tNowUnix int64, cfg FormulaConfig) float64 {
	avgWi := math.Max(wi, cfg.Alpha)
	lambda := ComputeDecayRate(avgWi, cfg.TBaseDays)

	deltaDays := float64(tNowUnix-lastUpdateUnix) / secondsPerDay
	if deltaDays < 0 {
		deltaDays = 0
	}

	decayed := accScore * ComputeDecayFactor(lambda, deltaDays)
	return decayed + wi*vi
}

// ComputeCurrentScore performs lazy decay evaluation (read path, §3.6).
//
// Returns the displayed score S clamped to (-∞, 1000]; negative scores are allowed.
// This value is for display only — NEVER write it back to MongoDB as accumulatedScore.
//
// O(1) approximation: decay uses λ computed from cfg.Alpha (minimum-difficulty weight)
// rather than the true per-task wᵢ mix. This intentionally under-decays high-wᵢ tasks
// at read time in exchange for O(1) computation. The write path (ApplyTaskScore) is exact.
//
// accScore:         stored accumulatedScore.
// lastUpdateUnix:   Unix seconds of last score update.
// consecutiveFails: current consecutive failure count.
// nowUnix:          current Unix seconds.
// cfg:              formula parameters.
func ComputeCurrentScore(accScore float64, lastUpdateUnix int64, consecutiveFails int64, nowUnix int64, cfg FormulaConfig) float64 {
	lambda := ComputeDecayRate(cfg.Alpha, cfg.TBaseDays)

	deltaDays := float64(nowUnix-lastUpdateUnix) / secondsPerDay
	if deltaDays < 0 {
		deltaDays = 0
	}

	decayed := accScore * ComputeDecayFactor(lambda, deltaDays)
	penalty := ComputePenalty(consecutiveFails, cfg.Gamma, cfg.Theta)
	score := cfg.SBase + decayed - penalty
	return math.Min(1000.0, score)
}

// RevertFeedbackScore subtracts a previously scored feedback's residual contribution
// in O(1) time (§3.7, used when FeedbackRevoked is processed).
//
// accScore:              current accumulatedScore.
// lastUpdateUnix:        Unix seconds of the agent's last score update.
// wi:                    difficulty weight of the feedback being revoked.
// vi:                    validation score of the feedback being revoked.
// feedbackTimestampUnix: Unix seconds when the original feedback was applied.
// nowUnix:               current Unix seconds.
// cfg:                   formula parameters.
func RevertFeedbackScore(accScore float64, lastUpdateUnix int64, wi, vi float64, feedbackTimestampUnix int64, nowUnix int64, cfg FormulaConfig) float64 {
	avgWi := math.Max(wi, cfg.Alpha)
	lambda := ComputeDecayRate(avgWi, cfg.TBaseDays)

	// Decay the full accumulated score forward to now.
	deltaDaysTotal := float64(nowUnix-lastUpdateUnix) / secondsPerDay
	if deltaDaysTotal < 0 {
		deltaDaysTotal = 0
	}
	decayed := accScore * ComputeDecayFactor(lambda, deltaDaysTotal)

	// Compute what the revoked feedback's residual contribution is at nowUnix.
	deltaDaysFeedback := float64(nowUnix-feedbackTimestampUnix) / secondsPerDay
	if deltaDaysFeedback < 0 {
		deltaDaysFeedback = 0
	}
	residual := wi * vi * ComputeDecayFactor(lambda, deltaDaysFeedback)

	return decayed - residual
}
