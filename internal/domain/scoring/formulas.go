package scoring

// formulas.go — Pure TrustRank scoring functions.
// implements §3.1 Total Trust Score
// implements §3.2 Difficulty Weight (wᵢ)
// implements §3.3 Adaptive Time Decay (λᵢ)
// implements §3.4 Progressive Penalty (P)
// implements composite score blend (reputation + services + publisher + compliance)
//
// ALL scoring formula logic lives here and nowhere else in the codebase.

import (
	"math"
	"strings"
)

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
// It decays the existing reputationScore forward to tNowUnix, then adds the new
// feedback contribution wᵢ·(vᵢ − 0.40). vi = 0.40 is neutral (zero contribution);
// vi > 0.40 increases the score; vi < 0.40 decreases it.
//
// NOTE: This function does NOT apply the consecutive-failure penalty. When vi < 0.40,
// the caller must additionally subtract ComputePenalty(newConsecFails, cfg.Gamma, cfg.Theta).
//
// accScore:       current reputationScore stored in the agent document.
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
	effectiveVi := vi - 0.40
	return decayed + wi*effectiveVi
}

// ComputeCurrentScore performs lazy decay evaluation (read path, §3.6).
//
// Returns the displayed score S clamped to (-∞, 1000]; negative scores are allowed.
// This value is for display only — NEVER write it back to MongoDB as reputationScore.
//
// The consecutive-failure penalty is now baked into reputationScore at write time
// (see ApplyTaskScore + ComputePenalty), so this function applies pure decay only.
//
// O(1) approximation: decay uses λ computed from cfg.Alpha (minimum-difficulty weight).
//
// accScore:       stored reputationScore (penalty already included).
// lastUpdateUnix: Unix seconds of last score update.
// nowUnix:        current Unix seconds.
// cfg:            formula parameters.
func ComputeCurrentScore(accScore float64, lastUpdateUnix int64, nowUnix int64, cfg FormulaConfig) float64 {
	lambda := ComputeDecayRate(cfg.Alpha, cfg.TBaseDays)

	deltaDays := float64(nowUnix-lastUpdateUnix) / secondsPerDay
	if deltaDays < 0 {
		deltaDays = 0
	}

	decayed := accScore * ComputeDecayFactor(lambda, deltaDays)
	return math.Min(1000.0, cfg.SBase+decayed)
}

// ── Composite score blend ───────────────────────────────────────────────────────

// CompositeWeights holds the blend factors for the four composite components.
// defaults: 0.5 reputation + 0.2 services + 0.2 publisher + 0.1 compliance.
type CompositeWeights struct {
	Reputation float64
	Services   float64
	Publisher  float64
	Compliance float64
}

// ComplianceWeights holds tier point totals (must sum to 100; default 80/20).
type ComplianceWeights struct {
	Tier1Total float64 // total points awarded across all 5 tier-1 fields
	Tier2Total float64 // total points awarded across all 5 tier-2 fields
}

// DefaultCompositeWeights returns the spec-defined default blend weights.
func DefaultCompositeWeights() CompositeWeights {
	return CompositeWeights{Reputation: 0.5, Services: 0.2, Publisher: 0.2, Compliance: 0.1}
}

// DefaultComplianceWeights returns the spec-defined default compliance tier weights.
func DefaultComplianceWeights() ComplianceWeights {
	return ComplianceWeights{Tier1Total: 80.0, Tier2Total: 20.0}
}

// NormalizeReputation maps raw reputation [-inf, 1000] → [0, 100].
// negative values floor at 0 (prevents decay-pumping of low scores).
func NormalizeReputation(rawRep float64) float64 {
	if rawRep <= 0 {
		return 0
	}
	if rawRep >= 1000 {
		return 100
	}
	return rawRep / 10.0
}

// ComputeServicesScore returns the healthy-endpoint percentage [0, 100].
// if totalServices == 0 (no declared services), returns 0.
func ComputeServicesScore(totalServices, healthyCount int) float64 {
	if totalServices <= 0 {
		return 0
	}
	if healthyCount < 0 {
		healthyCount = 0
	}
	if healthyCount > totalServices {
		healthyCount = totalServices
	}
	return float64(healthyCount) / float64(totalServices) * 100.0
}

// ExpectedAgentURIType is the canonical ERC-8004 registration type identifier.
const ExpectedAgentURIType = "https://eips.ethereum.org/EIPS/eip-8004#registration-v1"

// ComplianceInput is the minimum field set needed for compliance scoring.
// all fields read directly from AgentDocument (caller passes them in).
type ComplianceInput struct {
	AgentURI       string
	Type           string
	Name           string
	Description    string
	Image          string
	Services       int  // len(agent.Services)
	Registrations  int  // len(agent.Registrations)
	SupportedTrust int  // len(agent.SupportedTrust)
	X402Support    bool
	AgentWallet    string
	CardUpdatedAt  string
}

// ComputeComplianceScore returns the compliance percentage [0, 100].
// gate: if AgentURI is empty, return 0.
// tier-1 core (5 fields, default 80 pts total, 16 pts each):
//   - Type equals ExpectedAgentURIType
//   - Name non-empty
//   - Description non-empty
//   - Services count >= 1
//   - Registrations count >= 1
//
// tier-2 supporting (5 fields, default 20 pts total, 4 pts each):
//   - Image non-empty
//   - SupportedTrust count >= 1
//   - X402Support true
//   - AgentWallet non-empty
//   - CardUpdatedAt non-empty
func ComputeComplianceScore(in ComplianceInput, w ComplianceWeights) float64 {
	if strings.TrimSpace(in.AgentURI) == "" {
		return 0
	}

	t1 := w.Tier1Total / 5
	var tier1Earned float64
	if in.Type == ExpectedAgentURIType {
		tier1Earned += t1
	}
	if strings.TrimSpace(in.Name) != "" {
		tier1Earned += t1
	}
	if strings.TrimSpace(in.Description) != "" {
		tier1Earned += t1
	}
	if in.Services >= 1 {
		tier1Earned += t1
	}
	if in.Registrations >= 1 {
		tier1Earned += t1
	}

	t2 := w.Tier2Total / 5
	var tier2Earned float64
	if strings.TrimSpace(in.Image) != "" {
		tier2Earned += t2
	}
	if in.SupportedTrust >= 1 {
		tier2Earned += t2
	}
	if in.X402Support {
		tier2Earned += t2
	}
	if strings.TrimSpace(in.AgentWallet) != "" {
		tier2Earned += t2
	}
	if strings.TrimSpace(in.CardUpdatedAt) != "" {
		tier2Earned += t2
	}

	return tier1Earned + tier2Earned
}

// ComputeCompositeScore blends the four normalized component scores.
// each component must already be in [0, 100]; result is clamped to [0, 100].
func ComputeCompositeScore(reputation, services, publisher, compliance float64, w CompositeWeights) float64 {
	if reputation < 0 {
		reputation = 0
	}
	if reputation > 100 {
		reputation = 100
	}
	if services < 0 {
		services = 0
	}
	if services > 100 {
		services = 100
	}
	if publisher < 0 {
		publisher = 0
	}
	if publisher > 100 {
		publisher = 100
	}
	if compliance < 0 {
		compliance = 0
	}
	if compliance > 100 {
		compliance = 100
	}

	result := reputation*w.Reputation + services*w.Services + publisher*w.Publisher + compliance*w.Compliance
	if result < 0 {
		result = 0
	}
	if result > 100 {
		result = 100
	}
	return result
}

