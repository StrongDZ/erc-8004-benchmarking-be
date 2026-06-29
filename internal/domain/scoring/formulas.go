package scoring

// formulas.go — Pure TrustRank scoring functions.
// implements §3.1 Total Trust Score
// implements §3.3 Adaptive Time Decay (λᵢ)
// implements §3.4 Progressive Penalty (P)
// implements composite score blend (reputation + services + publisher + compliance)
//
// ALL scoring formula logic lives here and nowhere else in the codebase.

import (
	"math"
	"strings"
)

// FormulaConfig holds the tunable scoring parameters, read from environment at startup.
type FormulaConfig struct {
	Alpha        float64 // base per-feedback weight wᵢ (default 1.0): seeds wᵢ before ApplyAdjustments. Does NOT affect the decay rate (λ = ln2/T_base).
	TBaseDays    float64 // §3.3 half-life base in days (default 15.0)
	C            float64 // confidence prior strength: evidence mass B at which confidence = 0.5 (default 3.0)
	Gamma        float64 // reliability penalty base coefficient (default 0.1)
	Theta        float64 // reliability penalty exponent (default 1.5)
	AdoptionURef int     // distinct-client count that saturates Adoption to 100 (default 5)
	SBase        float64 // legacy base score; unused in the v2 model (kept for config compatibility)
}

// DefaultFormulaConfig returns the spec-defined default parameter set.
func DefaultFormulaConfig() FormulaConfig {
	return FormulaConfig{
		Alpha:        1.0,
		TBaseDays:    15.0,
		C:            3.0,
		Gamma:        0.1,
		Theta:        1.5,
		AdoptionURef: 5,
		SBase:        0.0,
	}
}

// TaskInput is the per-feedback input to ComputeScore.
type TaskInput struct {
	Vi        float64 // validation score [0, 1]
	DeltaDays float64 // fractional days since task completion — MUST NOT be in seconds
}

// ComputeDecayRate computes the global time-decay rate λ from the half-life base T_base.
// λ = ln(2) / T_base, so the reputation mass half-life is exactly T_base days. The
// per-feedback weight wᵢ affects mass magnitude (A += wᵢ·vᵢ), not the decay rate.
func ComputeDecayRate(tBaseDays float64) float64 {
	return math.Log(2) / tBaseDays
}

// ComputeDecayFactor computes the time decay multiplier e^(-λᵢ·Δt).
// §3.3: D(t) = e^(-λᵢ · Δt)
// deltaDays must be in FRACTIONAL DAYS: time.Since(t).Hours() / 24.0
func ComputeDecayFactor(lambda, deltaDays float64) float64 {
	return math.Exp(-lambda * deltaDays)
}

// secondsPerDay is the day length used by the decay helpers.
const secondsPerDay = 86400.0

// ── Composite score blend ───────────────────────────────────────────────────────

// CompositeWeights holds the blend factors for the composite components.
// defaults: 0.40 reputation + 0.15 adoption + 0.15 services + 0.20 publisher + 0.10 compliance.
type CompositeWeights struct {
	Reputation float64
	Adoption   float64
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
	return CompositeWeights{Reputation: 0.40, Adoption: 0.15, Services: 0.15, Publisher: 0.20, Compliance: 0.10}
}

// DefaultComplianceWeights returns the spec-defined default compliance tier weights.
func DefaultComplianceWeights() ComplianceWeights {
	return ComplianceWeights{Tier1Total: 80.0, Tier2Total: 20.0}
}

// JSONRequiredEndpoints must return successfully parsed JSON to count as healthy.
// These are the high-protocol endpoints where a non-JSON body is a meaningful defect.
var JSONRequiredEndpoints = map[string]struct{}{
	"oasf": {},
	"a2a":  {},
	"mcp":  {},
}

// FetchOnlyImportantEndpoints just need to be fetched (any status >= 1) to count as healthy.
var FetchOnlyImportantEndpoints = map[string]struct{}{
	"web":   {},
	"email": {},
}

// IsJSONRequired returns true when the service name (case-insensitive) is in JSONRequiredEndpoints.
func IsJSONRequired(name string) bool {
	_, ok := JSONRequiredEndpoints[strings.ToLower(name)]
	return ok
}

// IsImportantEndpoint returns true when the service name is in either important set.
func IsImportantEndpoint(name string) bool {
	lower := strings.ToLower(name)
	_, ok1 := JSONRequiredEndpoints[lower]
	_, ok2 := FetchOnlyImportantEndpoints[lower]
	return ok1 || ok2
}

// ServiceHealthCheck represents one declared service and its observed offchain status.
type ServiceHealthCheck struct {
	Name   string // service.Name (e.g. "OASF", "MCP", "web", "ENS")
	Status int    // offchain.Status (-1 / 1 / 5)
}

// ServicesScoreResult is returned by ComputeServicesScore.
// Warnings lists names of JSON-required important endpoints that were fetched
// but not valid JSON (status == 1), so the FE can surface a per-service warning.
type ServicesScoreResult struct {
	Score    float64
	Warnings []string // names where JSON expected but status == 1
}

// ComputeServicesScore applies tier-weighted health rules to determine the services score.
//
// Tier classification (case-insensitive name comparison):
//
//	Important/JSON-required (oasf, a2a, mcp):  status 5 = healthy; status 1 = unhealthy + warning; -1 = unhealthy.
//	Important/fetch-only   (web, email):         status 1 or 5 = healthy; -1 = unhealthy.
//	Other (anything else):                       status 1 or 5 = healthy; -1 = unhealthy.
//
// Pool math:
//
//	imp_part = (imp_healthy / imp_total) * 80   (0 when no important services)
//	oth_part = (oth_healthy / oth_total) * 20   (0 when no other services)
//	ServicesScore = imp_part + oth_part           range [0, 100]
func ComputeServicesScore(checks []ServiceHealthCheck) ServicesScoreResult {
	if len(checks) == 0 {
		return ServicesScoreResult{}
	}

	var impTotal, impHealthy, othTotal, othHealthy int
	var warnings []string

	for _, c := range checks {
		if IsImportantEndpoint(c.Name) {
			impTotal++
			if IsJSONRequired(c.Name) {
				switch c.Status {
				case 5:
					impHealthy++
				case 1:
					// fetched but not JSON — unhealthy for JSON-required tier, emit warning
					warnings = append(warnings, c.Name)
					// -1 or 0: unhealthy, no warning
				}
			} else {
				// fetch-only important (web, email)
				if c.Status == 1 || c.Status == 5 {
					impHealthy++
				}
			}
		} else {
			othTotal++
			if c.Status == 1 || c.Status == 5 {
				othHealthy++
			}
		}
	}

	var impPart float64
	if impTotal > 0 {
		impPart = float64(impHealthy) / float64(impTotal) * 80.0
	}
	var othPart float64
	if othTotal > 0 {
		othPart = float64(othHealthy) / float64(othTotal) * 20.0
	}

	return ServicesScoreResult{
		Score:    impPart + othPart,
		Warnings: warnings,
	}
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
	Services       int // len(agent.Services)
	Registrations  int // len(agent.Registrations)
	SupportedTrust int // len(agent.SupportedTrust)
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

// ComputeCompositeFromStats is a convenience wrapper that blends the five v2 components
// using weight renormalization (see ComputeCompositeRenorm). qualityPresent must be true
// only when the agent has at least one scored service feedback; publisherPresent must be
// true when a publisher signal is available (false drops the publisher weight and
// redistributes it to the other components). Each score is expected in [0,100]; result is
// clamped to [0,100].
func ComputeCompositeFromStats(reputation, adoption, services, publisher, compliance float64, qualityPresent, publisherPresent bool, w CompositeWeights) float64 {
	return ComputeCompositeRenorm([]WeightedComponent{
		{Score: reputation, Weight: w.Reputation, Present: qualityPresent},
		{Score: adoption, Weight: w.Adoption, Present: true},
		{Score: services, Weight: w.Services, Present: true},
		{Score: publisher, Weight: w.Publisher, Present: publisherPresent},
		{Score: compliance, Weight: w.Compliance, Present: true},
	})
}
