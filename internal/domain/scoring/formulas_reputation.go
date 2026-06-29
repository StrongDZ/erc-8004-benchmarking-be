package scoring

// formulas_reputation.go — Reputation v2: bounded weighted-mean model.
//
// Reputation = Quality × Confidence × Reliability × 100   (range [0, 100])
//
//	Quality     = A / B                  bounded weighted mean of vᵢ  ∈ [0,1]
//	Confidence  = B / (C + B)            evidence sufficiency          ∈ [0,1)
//	Reliability = 1 / (1 + γ·N^θ)        consecutive-fail streak       ∈ (0,1]
//
// where A = Σ wᵢ·dᵢ·vᵢ  and  B = Σ wᵢ·dᵢ  (decayed, difficulty-weighted sums).
//
// Quality × Confidence ≡ A / (C + B)  — Bayesian shrinkage toward 0 (prior mean
// m₀ = 0, prior strength C): empty agents score 0, low-evidence agents are
// discounted (anti-whitewash), and idle agents decay toward 0 as B → 0
// (anti-squatting), all without a positive prior or any cron.

import "math"

// ComputeQuality returns the difficulty- and recency-weighted mean of vᵢ, A/B ∈ [0,1].
// Returns 0 when there is no evidence (B ≤ 0) — the caller treats this as "Unrated".
func ComputeQuality(a, b float64) float64 {
	if b <= 0 {
		return 0
	}
	return a / b
}

// ComputeConfidence returns the evidence-sufficiency factor B/(C+B) ∈ [0,1).
// c is the prior strength: B = c gives confidence 0.5; confidence → 1 as B grows.
func ComputeConfidence(b, c float64) float64 {
	if b <= 0 {
		return 0
	}
	return b / (c + b)
}

// ComputeReliability returns the consecutive-failure factor 1/(1 + γ·N^θ) ∈ (0,1].
// N = 0 (no current streak) → 1.0. A passing task decays the streak via
// DecayConsecutiveFails rather than clearing it outright, so the penalty recovers
// gradually over several successes (still fully recoverable).
func ComputeReliability(nFail int64, gamma, theta float64) float64 {
	if nFail <= 0 {
		return 1.0
	}
	return 1.0 / (1.0 + gamma*math.Pow(float64(nFail), theta))
}

// DecayConsecutiveFails returns the new consecutive-fail streak after one passing
// task. Instead of clearing the streak outright, a success removes a third of it
// (round-half-up) but always at least 1, so the Reliability penalty fades over
// several successes rather than vanishing after a single one. Repeated successes
// trace 10 → 7 → 5 → 3 → 2 → 1 → 0. n ≤ 0 returns 0.
func DecayConsecutiveFails(n int64) int64 {
	if n <= 0 {
		return 0
	}
	reduction := int64(math.Round(float64(n) / 3.0))
	if reduction < 1 {
		reduction = 1
	}
	return n - reduction
}

// ComputeReputationScore blends the three factors into a [0,100] reputation.
// a = Σ wᵢ·dᵢ·vᵢ, b = Σ wᵢ·dᵢ (both decayed to the evaluation time), nFail = current
// consecutive-fail streak. c, gamma, theta are the confidence/reliability parameters.
func ComputeReputationScore(a, b float64, nFail int64, c, gamma, theta float64) float64 {
	quality := ComputeQuality(a, b)
	confidence := ComputeConfidence(b, c)
	reliability := ComputeReliability(nFail, gamma, theta)
	return quality * confidence * reliability * 100.0
}

// ComputeAdoption maps the distinct-client count u to a log-scaled, capped [0,100]
// breadth-of-use score: 100 · min(1, ln(1+u)/ln(1+uRef)). u ≥ uRef saturates at 100.
func ComputeAdoption(u, uRef int) float64 {
	if u <= 0 || uRef <= 0 {
		return 0
	}
	ratio := math.Log1p(float64(u)) / math.Log1p(float64(uRef))
	if ratio > 1 {
		ratio = 1
	}
	return 100.0 * ratio
}

// massDecayFactor returns the decay multiplier applied to BOTH A and B between two
// timestamps. A single global rate (λ = ln2/T_base) is used so that A and B decay
// identically — this keeps Quality = A/B stable over idle time while Confidence =
// B/(C+B) shrinks, which is exactly the anti-squatting behaviour we want.
func massDecayFactor(lastUpdateUnix, nowUnix int64, cfg FormulaConfig) float64 {
	deltaDays := float64(nowUnix-lastUpdateUnix) / secondsPerDay
	if deltaDays < 0 {
		deltaDays = 0
	}
	lambda := ComputeDecayRate(cfg.TBaseDays)
	return ComputeDecayFactor(lambda, deltaDays)
}

// DecayMass decays the score mass A and evidence mass B forward to nowUnix.
func DecayMass(a, b float64, lastUpdateUnix, nowUnix int64, cfg FormulaConfig) (float64, float64) {
	f := massDecayFactor(lastUpdateUnix, nowUnix, cfg)
	return a * f, b * f
}

// ApplyFeedbackToMass is the O(1) write-path update: it decays (a,b) forward to the
// feedback's timestamp, then adds the new feedback's contribution wᵢ·vᵢ to A and wᵢ to B.
// Returns the updated (A, B). The caller persists these as WeightedScoreSum / WeightMass.
func ApplyFeedbackToMass(a, b float64, lastUpdateUnix int64, wi, vi float64, tNowUnix int64, cfg FormulaConfig) (float64, float64) {
	a, b = DecayMass(a, b, lastUpdateUnix, tNowUnix, cfg)
	return a + wi*vi, b + wi
}

// ComputeReputationAt is the read-path evaluator: it decays (a,b) to nowUnix, then
// returns the [0,100] reputation. Display only — never persist the decayed mass back.
func ComputeReputationAt(a, b float64, lastUpdateUnix, nowUnix int64, nFail int64, cfg FormulaConfig) float64 {
	a, b = DecayMass(a, b, lastUpdateUnix, nowUnix, cfg)
	return ComputeReputationScore(a, b, nFail, cfg.C, cfg.Gamma, cfg.Theta)
}

// WeightedComponent is one composite input with its base weight and a presence flag.
// A component is "present" only when the agent actually has a signal for it
// (e.g. Quality is absent when the agent has no scored service feedback).
type WeightedComponent struct {
	Score   float64
	Weight  float64
	Present bool
}

// ComputeCompositeRenorm blends the present components as a weighted average over
// their weights only — a missing component's weight is redistributed proportionally
// across the present ones, so agents lacking a signal are not unfairly capped.
// Returns 0 when no component is present. Result is clamped to [0,100].
func ComputeCompositeRenorm(components []WeightedComponent) float64 {
	var weighted, totalWeight float64
	for _, c := range components {
		if !c.Present || c.Weight <= 0 {
			continue
		}
		s := c.Score
		if s < 0 {
			s = 0
		}
		if s > 100 {
			s = 100
		}
		weighted += c.Weight * s
		totalWeight += c.Weight
	}
	if totalWeight <= 0 {
		return 0
	}
	result := weighted / totalWeight
	if result < 0 {
		return 0
	}
	if result > 100 {
		return 100
	}
	return result
}
