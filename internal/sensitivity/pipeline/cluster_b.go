package pipeline

// cluster_b.go — Cụm B: Composite Blend.
// Parameters: 4 composite weights (sum-1 constraint) + Tier1Total, Tier2Total.

import (
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterBParamSpecs returns the canonical Cluster-B parameter list.
//
// NOTE: For OAT/Tornado the four weights are each swept independently; the
// recompute closure re-normalises all four so they always sum to 1. Sobol uses
// the same recompute (re-normalising), while the dedicated simplex mode samples
// the weight simplex directly via Dirichlet.
func ClusterBParamSpecs() []runner.ParamSpec {
	cw := scoring.DefaultCompositeWeights()
	cm := scoring.DefaultComplianceWeights()
	return []runner.ParamSpec{
		{Name: "WReputation", Default: cw.Reputation, Low: cw.Reputation * 0.5, High: cw.Reputation * 1.5},
		{Name: "WServices", Default: cw.Services, Low: cw.Services * 0.5, High: cw.Services * 1.5},
		{Name: "WPublisher", Default: cw.Publisher, Low: cw.Publisher * 0.5, High: cw.Publisher * 1.5},
		{Name: "WCompliance", Default: cw.Compliance, Low: cw.Compliance * 0.5, High: cw.Compliance * 1.5},
		{Name: "Tier1Total", Default: cm.Tier1Total, Low: 50.0, High: 100.0},
		{Name: "Tier2Total", Default: cm.Tier2Total, Low: 0.0, High: 50.0},
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

// ClusterBRecompute returns a RecomputeFn that re-blends each agent's composite
// using the configured weights. Per-component scores (Reputation, Services,
// Publisher, Compliance) are read from snapshot.Baseline — these are the
// production component scores frozen at snapshot time.
//
// When weights don't sum to 1.0 (e.g. one weight swept via OAT), they are
// normalised before blending so the composite stays in [0, 100].
//
// Tier1Total/Tier2Total are modelled as a linear scale on the compliance
// component: complianceScale = (Tier1Total + Tier2Total) / 100. At defaults
// (80 + 20) the scale is 1.0, leaving compliance untouched.
func ClusterBRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	return func(cfg map[string]float64) map[string]float64 {
		wRep := cfg["WReputation"]
		wSvc := cfg["WServices"]
		wPub := cfg["WPublisher"]
		wCom := cfg["WCompliance"]
		// Normalise so the four weights sum to 1.
		sum := wRep + wSvc + wPub + wCom
		if sum == 0 {
			sum = 1
		}
		wRep, wSvc, wPub, wCom = wRep/sum, wSvc/sum, wPub/sum, wCom/sum

		tierScale := (cfg["Tier1Total"] + cfg["Tier2Total"]) / 100.0
		if tierScale == 0 {
			tierScale = 1
		}

		w := scoring.CompositeWeights{
			Reputation: wRep, Services: wSvc, Publisher: wPub, Compliance: wCom,
		}
		out := make(map[string]float64, len(data.Baseline))
		for _, b := range data.Baseline {
			composite := scoring.ComputeCompositeScore(
				b.ReputationScore, b.ServicesScore, b.PublisherScore,
				b.ComplianceScore*tierScale,
				w,
			)
			out[b.AgentID] = composite
		}
		return out
	}
}

// ClusterBSimplexRunner runs Dirichlet simplex sampling on the 4 weights only,
// keeping Tier1/Tier2 at their defaults. Returns one RunResult per sample, each
// carrying the sampled weight config and the resulting per-agent scores.
func ClusterBSimplexRunner(data snapshot.SnapshotData, nSamples int, seed int64) []runner.RunResult {
	weightNames := []string{"WReputation", "WServices", "WPublisher", "WCompliance"}
	samples := runner.DirichletSamples(4, nSamples, 1.0, seed)
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
