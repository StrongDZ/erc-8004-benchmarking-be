package pipeline

// cluster_a.go — Cụm A: Scoring Core (Reputation v2).
//
// Mirrors the AUTHORITATIVE score-refresh replay (scorerefresh/replay.go), which
// rebuilds reputation from the persisted quality weight fb.Wi:
//
//	Reputation = Quality · Confidence · Reliability · 100        ∈ [0,100]
//	  Quality     = A / B            A = Σ fb.Wi·dᵢ·vᵢ, B = Σ fb.Wi·dᵢ
//	  Confidence  = B / (C + B)
//	  Reliability = 1 / (1 + γ·N^θ)  N = current consecutive-fail streak
//
// A and B are decayed forward with a single global rate λ = ComputeDecayRate(α,
// T_base) to each feedback's timestamp, then once more to `now` at read time —
// exactly as ApplyFeedbackToMass / ComputeReputationAt (and replay.go's decayMass).
//
// The reputation weight wᵢ is QUALITY-based (propagation.ComputeWeight, set by the
// trust-graph worker and persisted as fb.Wi) — the price-based difficulty weight was
// removed because task-price data is too sparse to map. So β and K no longer affect
// the score and are NOT swept here; the wᵢ sensitivity lives in Cụm C (quality → wᵢ).
//
// Parameters varied: Alpha (decay), TBaseDays (decay half-life), C (confidence prior),
// Gamma, Theta (reliability).

import (
	"sort"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// passThreshold mirrors the live scorer: vᵢ ≥ 0.40 is a pass, below is a fail that
// extends the consecutive-fail streak feeding Reliability.
const passThreshold = 0.40

// ClusterAParamSpecs returns the canonical Cluster-A parameter list with ±50% sweep
// ranges around their v2 defaults.
func ClusterAParamSpecs() []runner.ParamSpec {
	def := scoring.DefaultFormulaConfig()
	return []runner.ParamSpec{
		{Name: "Alpha", Default: def.Alpha, Low: def.Alpha * 0.5, High: def.Alpha * 1.5},
		{Name: "TBaseDays", Default: def.TBaseDays, Low: def.TBaseDays * 0.5, High: def.TBaseDays * 1.5},
		{Name: "C", Default: def.C, Low: def.C * 0.5, High: def.C * 1.5},
		{Name: "Gamma", Default: def.Gamma, Low: def.Gamma * 0.5, High: def.Gamma * 1.5},
		{Name: "Theta", Default: def.Theta, Low: def.Theta * 0.5, High: def.Theta * 1.5},
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

// ClusterARecompute returns a RecomputeFn closing over the snapshot data. For each
// agent it replays its quality feedbacks in timestamp order, accumulating the (A,B)
// mass from the persisted quality weight fb.Wi with decay, tracking the consecutive-
// fail streak, then decays to `now` and evaluates the v2 reputation.
func ClusterARecompute(data snapshot.SnapshotData, nowUnix int64) runner.RecomputeFn {
	// Group feedbacks per agent once, sorted ascending by timestamp so the mass decay
	// between consecutive feedbacks is replayed in chronological order.
	byAgent := make(map[string][]snapshot.FeedbackSnapshot, len(data.Agents))
	for _, fb := range data.Feedbacks {
		byAgent[fb.AgentID] = append(byAgent[fb.AgentID], fb)
	}
	for id := range byAgent {
		fbs := byAgent[id]
		sort.SliceStable(fbs, func(i, j int) bool { return fbs[i].Timestamp < fbs[j].Timestamp })
	}

	return func(cfg map[string]float64) map[string]float64 {
		formula := scoring.FormulaConfig{
			Alpha:     cfg["Alpha"],
			TBaseDays: cfg["TBaseDays"],
			C:         cfg["C"],
			Gamma:     cfg["Gamma"],
			Theta:     cfg["Theta"],
		}

		out := make(map[string]float64, len(data.Agents))
		for _, a := range data.Agents {
			var massA, massB float64
			var lastUpdate, consecFails int64
			for _, fb := range byAgent[a.ID] {
				massA, massB = scoring.ApplyFeedbackToMass(massA, massB, lastUpdate, fb.Wi, fb.ValueNormalized, fb.Timestamp, formula)
				lastUpdate = fb.Timestamp

				if fb.ValueNormalized >= passThreshold {
					consecFails = 0
				} else {
					consecFails++
				}
			}
			out[a.ID] = scoring.ComputeReputationAt(massA, massB, lastUpdate, nowUnix, consecFails, formula)
		}
		return out
	}
}
