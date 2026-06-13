package pipeline

// cluster_b.go — Cụm B: Composite Blend (v2, 5 components).
// Parameters: 5 composite weights (Reputation, Adoption, Services, Publisher,
// Compliance — sum-1 constraint).
//
// NOTE: The compliance tier weights (Tier1Total/Tier2Total) are intentionally NOT
// swept here. They are inputs to ComputeComplianceScore (they set the relative pull
// of card-field presence DURING the score), not a post-hoc scaler. The snapshot
// stores only the finished ComplianceScore, not the raw card fields, so they cannot
// be faithfully re-evaluated from a composite snapshot — a tier sweep would require a
// snapshot carrying the raw ComplianceInput.

import (
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterBParamSpecs returns the canonical Cluster-B parameter list.
//
// NOTE: For OAT/Tornado the five weights are each swept independently; the
// recompute closure re-normalises them so they always sum to 1. Sobol uses the
// same recompute (re-normalising); the dedicated simplex mode samples the weight
// simplex directly via Dirichlet.
func ClusterBParamSpecs() []runner.ParamSpec {
	cw := scoring.DefaultCompositeWeights()
	return []runner.ParamSpec{
		{Name: "WReputation", Default: cw.Reputation, Low: cw.Reputation * 0.5, High: cw.Reputation * 1.5},
		{Name: "WAdoption", Default: cw.Adoption, Low: cw.Adoption * 0.5, High: cw.Adoption * 1.5},
		{Name: "WServices", Default: cw.Services, Low: cw.Services * 0.5, High: cw.Services * 1.5},
		{Name: "WPublisher", Default: cw.Publisher, Low: cw.Publisher * 0.5, High: cw.Publisher * 1.5},
		{Name: "WCompliance", Default: cw.Compliance, Low: cw.Compliance * 0.5, High: cw.Compliance * 1.5},
	}
}

// DefaultClusterBConfig returns the baseline config (Default values for every spec).
func DefaultClusterBConfig() map[string]float64 {
	cfg := make(map[string]float64)
	for _, p := range ClusterBParamSpecs() {
		cfg[p.Name] = p.Default
	}
	return cfg
}

// weightsFromConfig reads the five composite weights, normalised to sum 1.
func weightsFromConfig(cfg map[string]float64) scoring.CompositeWeights {
	wRep := cfg["WReputation"]
	wAdo := cfg["WAdoption"]
	wSvc := cfg["WServices"]
	wPub := cfg["WPublisher"]
	wCom := cfg["WCompliance"]
	sum := wRep + wAdo + wSvc + wPub + wCom
	if sum == 0 {
		sum = 1
	}
	return scoring.CompositeWeights{
		Reputation: wRep / sum,
		Adoption:   wAdo / sum,
		Services:   wSvc / sum,
		Publisher:  wPub / sum,
		Compliance: wCom / sum,
	}
}

// ClusterBRecompute returns a RecomputeFn that re-blends each agent's composite
// using the configured weights. Per-component scores are read from snapshot.Baseline
// (the production v2 component scores frozen at snapshot time) and recombined via
// scoring.ComputeCompositeFromStats, which renormalises over the PRESENT components —
// so a missing publisher signal redistributes its weight rather than scoring 0.
//
// qualityPresent is true for every snapshot agent (the snapshot is quality-only, so
// each has scored reputation mass). publisherPresent comes from the baseline.
func ClusterBRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	return func(cfg map[string]float64) map[string]float64 {
		w := weightsFromConfig(cfg)
		out := make(map[string]float64, len(data.Baseline))
		for _, b := range data.Baseline {
			out[b.AgentID] = scoring.ComputeCompositeFromStats(
				b.ReputationScore, b.AdoptionScore, b.ServicesScore, b.PublisherScore, b.ComplianceScore,
				true, b.PublisherPresent, w,
			)
		}
		return out
	}
}

// ClusterBSimplexRunner runs Dirichlet simplex sampling on the 5 weights only,
// keeping Tier1/Tier2 at their defaults. Returns one RunResult per sample, each
// carrying the sampled weight config and the resulting per-agent scores.
func ClusterBSimplexRunner(data snapshot.SnapshotData, nSamples int, seed int64) []runner.RunResult {
	weightNames := []string{"WReputation", "WAdoption", "WServices", "WPublisher", "WCompliance"}
	samples := runner.DirichletSamples(len(weightNames), nSamples, 1.0, seed)
	def := DefaultClusterBConfig()
	recompute := ClusterBRecompute(data)
	out := make([]runner.RunResult, 0, nSamples)
	for _, s := range samples {
		cfg := make(map[string]float64, len(def))
		for k, v := range def {
			cfg[k] = v
		}
		for i, name := range weightNames {
			cfg[name] = s[i]
		}
		out = append(out, runner.RunResult{Config: cfg, Scores: recompute(cfg)})
	}
	return out
}
