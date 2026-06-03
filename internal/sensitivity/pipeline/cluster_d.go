package pipeline

// cluster_d.go — Cụm D: TrustRank Power Iteration.
// Parameters: Alpha (damping), Epsilon (convergence), MaxIter, SeedThreshold.

import (
	"erc-8004-benchmarking-be/internal/app/trustpropagation"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterDParamSpecs returns the canonical Cluster-D parameter list with sweep ranges.
func ClusterDParamSpecs() []runner.ParamSpec {
	def := trustpropagation.DefaultIterConfig()
	return []runner.ParamSpec{
		{Name: "Alpha", Default: def.Alpha, Low: 0.5, High: 0.99},
		{Name: "Epsilon", Default: def.Epsilon, Low: 1e-6, High: 1e-2},
		{Name: "MaxIter", Default: float64(def.MaxIter), Low: 10, High: 100},
		{Name: "SeedThreshold", Default: def.SeedThreshold, Low: 50, High: 95},
	}
}

// DefaultClusterDConfig returns the baseline config (Default values for every spec).
func DefaultClusterDConfig() map[string]float64 {
	cfg := make(map[string]float64)
	for _, p := range ClusterDParamSpecs() {
		cfg[p.Name] = p.Default
	}
	return cfg
}

// ClusterDRecompute returns per-agent trust scores AFTER running TrustRankPass.
// It builds the trust graph once (nodes = agent owners + client wallets, edges =
// graph_edges) and re-runs power iteration for each parameter configuration.
//
// The propagation graph is owner/wallet-keyed (that is how production TrustRank
// works), so the post-iteration score is looked up by agent owner. When one owner
// holds several agents they share the owner node's score — an inherent property of
// wallet-level propagation, acceptable for parameter-sensitivity measurement.
func ClusterDRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	gd := buildGraphData(data)

	ownerToAgents := make(map[string][]string, len(data.Agents))
	for _, a := range data.Agents {
		ownerToAgents[a.Owner] = append(ownerToAgents[a.Owner], a.ID)
	}

	return func(cfg map[string]float64) map[string]float64 {
		scores, _ := trustpropagation.TrustRankPass(gd, trustpropagation.PropagationIterConfig{
			Alpha:         cfg["Alpha"],
			Epsilon:       cfg["Epsilon"],
			MaxIter:       int(cfg["MaxIter"]),
			SeedThreshold: cfg["SeedThreshold"],
		})

		out := make(map[string]float64, len(data.Agents))
		for nodeID, s := range scores {
			for _, agentID := range ownerToAgents[nodeID] {
				out[agentID] = s
			}
		}
		// Ensure every agent has a score (0 when its owner is absent from the graph).
		for _, a := range data.Agents {
			if _, ok := out[a.ID]; !ok {
				out[a.ID] = 0
			}
		}
		return out
	}
}

// buildGraphData constructs a trustpropagation.GraphData from the snapshot:
// agent owners become agent nodes (seeded by composite score), client wallets from
// the edge set become wallet nodes, and graph_edges become weighted directed edges.
func buildGraphData(data snapshot.SnapshotData) trustpropagation.GraphData {
	baselineByID := make(map[string]snapshot.BaselineScore, len(data.Baseline))
	for _, b := range data.Baseline {
		baselineByID[b.AgentID] = b
	}

	nodes := make([]trustpropagation.GraphNode, 0, len(data.Agents)*2)
	seen := make(map[string]struct{}, len(data.Agents)*2)
	for _, a := range data.Agents {
		if a.Owner == "" {
			continue
		}
		if _, ok := seen[a.Owner]; ok {
			continue
		}
		seen[a.Owner] = struct{}{}
		b := baselineByID[a.ID]
		nodes = append(nodes, trustpropagation.GraphNode{
			ID:             a.Owner,
			Kind:           trustpropagation.NodeKindAgent,
			TrustScore:     b.CompositeScore,
			CompositeScore: b.CompositeScore,
		})
	}
	for _, e := range data.Edges {
		if e.FromWallet == "" {
			continue
		}
		if _, ok := seen[e.FromWallet]; ok {
			continue
		}
		seen[e.FromWallet] = struct{}{}
		nodes = append(nodes, trustpropagation.GraphNode{
			ID:         e.FromWallet,
			Kind:       trustpropagation.NodeKindWallet,
			TrustScore: 50, // neutral midpoint of [0,100]
		})
	}

	edges := make([]trustpropagation.GraphEdge, 0, len(data.Edges))
	for _, e := range data.Edges {
		edges = append(edges, trustpropagation.GraphEdge{
			From:   e.FromWallet,
			To:     e.ToWallet,
			Weight: e.InitialWi,
		})
	}
	return trustpropagation.GraphData{Nodes: nodes, Edges: edges}
}

// ConvergencePoint records the iteration count power iteration took for one Alpha.
type ConvergencePoint struct {
	Alpha      float64
	Iterations int
}

// ConvergenceCurve sweeps Alpha and records how many iterations power iteration
// took to converge, holding epsilon/maxIter/seedThreshold fixed. Points are
// returned in the order of the supplied alphas (caller passes them ascending).
func ConvergenceCurve(data snapshot.SnapshotData, alphas []float64, epsilon float64, maxIter int, seedThreshold float64) []ConvergencePoint {
	gd := buildGraphData(data)
	out := make([]ConvergencePoint, 0, len(alphas))
	for _, a := range alphas {
		_, iters := trustpropagation.TrustRankPass(gd, trustpropagation.PropagationIterConfig{
			Alpha:         a,
			Epsilon:       epsilon,
			MaxIter:       maxIter,
			SeedThreshold: seedThreshold,
		})
		out = append(out, ConvergencePoint{Alpha: a, Iterations: iters})
	}
	return out
}
