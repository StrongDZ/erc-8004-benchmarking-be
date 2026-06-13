package pipeline

// cluster_d.go — Cụm D: EigenTrust Power Iteration + Teleport Prior.
//
// Mirrors the production propagation pass (trustpropagation.EigenTrustPass via
// loader.go): a bipartite client-wallet → agent graph where
//
//	t[j] = α·inflow[j] + (1-α)·p[j]
//	p[wallet] = BlendTeleport(reviewerReliability, externalScore, TeleportGamma)
//	p[agent]  = AgentDirectReputation(reputation, adoption, services, compliance)
//	inflow[owner] = weightMass-weighted mean of owned agents' t[agent]
//
// Parameters: Alpha (damping), Epsilon (convergence), MaxIter, and TeleportGamma —
// the new knob blending reviewer reliability against the external on-chain score in
// the teleport prior. SeedThreshold is gone (the old TrustRank seeding is replaced
// by the EigenTrust teleport vector).

import (
	"erc-8004-benchmarking-be/internal/app/trustpropagation"
	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

const agentNodePrefix = "agent:"

// ClusterDParamSpecs returns the canonical Cluster-D parameter list with sweep ranges.
func ClusterDParamSpecs() []runner.ParamSpec {
	def := trustpropagation.DefaultIterConfig()
	return []runner.ParamSpec{
		{Name: "Alpha", Default: def.Alpha, Low: 0.5, High: 0.99},
		{Name: "Epsilon", Default: def.Epsilon, Low: 1e-6, High: 1e-2},
		{Name: "MaxIter", Default: float64(def.MaxIter), Low: 10, High: 100},
		{Name: "TeleportGamma", Default: extscore.TeleportGamma, Low: 0.0, High: 1.0},
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

// graphTemplate holds the gamma-independent parts of the trust graph so each
// recompute only re-derives the wallet teleport priors for the swept TeleportGamma.
type graphTemplate struct {
	agentNodes    []trustpropagation.GraphNode // DirectRep/Weight/OwnerID fixed
	walletPriors  map[string]walletPrior       // wallet node ID -> (reliability, external)
	edges         []trustpropagation.GraphEdge
	ownerByAgent  map[string]string // agentID -> owner wallet node ID
}

type walletPrior struct {
	reliability float64
	external    float64
}

// ClusterDRecompute returns per-agent trust scores AFTER running EigenTrustPass. The
// per-agent score is its owner wallet's propagated [0,100] (p99-normalized) score —
// agents sharing an owner share that wallet's score, exactly as wallet-level
// propagation works in production.
func ClusterDRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	tpl := buildGraphTemplate(data)

	return func(cfg map[string]float64) map[string]float64 {
		gamma := cfg["TeleportGamma"]
		gd := tpl.materialize(gamma)
		scores, _ := trustpropagation.EigenTrustPass(gd, trustpropagation.IterConfig{
			Alpha:   cfg["Alpha"],
			Epsilon: cfg["Epsilon"],
			MaxIter: int(cfg["MaxIter"]),
		})

		out := make(map[string]float64, len(data.Agents))
		for _, a := range data.Agents {
			out[a.ID] = scores.Rated[tpl.ownerByAgent[a.ID]]
		}
		return out
	}
}

// materialize builds the full GraphData for a given TeleportGamma, blending each
// wallet's reviewer reliability with its external on-chain score into the teleport.
func (t graphTemplate) materialize(gamma float64) trustpropagation.GraphData {
	nodes := make([]trustpropagation.GraphNode, 0, len(t.agentNodes)+len(t.walletPriors))
	nodes = append(nodes, t.agentNodes...)
	for id, wp := range t.walletPriors {
		nodes = append(nodes, trustpropagation.GraphNode{
			ID:        id,
			Kind:      trustpropagation.NodeKindWallet,
			DirectRep: extscore.BlendTeleport(wp.reliability, wp.external, gamma),
		})
	}
	return trustpropagation.GraphData{Nodes: nodes, Edges: t.edges}
}

// buildGraphTemplate constructs the gamma-independent graph from the snapshot:
//   - agent nodes seeded by AgentDirectReputation, weighted by evidence mass, linked
//     to their owner wallet;
//   - wallet priors (reviewer reliability from feedback counts + owner external score);
//   - client→agent edges weighted by valid-feedback count.
func buildGraphTemplate(data snapshot.SnapshotData) graphTemplate {
	cw := scoring.DefaultCompositeWeights()

	baselineByID := make(map[string]snapshot.BaselineScore, len(data.Baseline))
	for _, b := range data.Baseline {
		baselineByID[b.AgentID] = b
	}

	walletPriors := make(map[string]walletPrior)
	ensureWallet := func(id string) {
		if id == "" {
			return
		}
		if _, ok := walletPriors[id]; !ok {
			walletPriors[id] = walletPrior{reliability: 50} // neutral until evidence
		}
	}

	// Agent nodes + owner wallet teleport (external score lives on the owner wallet).
	agentNodes := make([]trustpropagation.GraphNode, 0, len(data.Agents))
	ownerByAgent := make(map[string]string, len(data.Agents))
	for _, a := range data.Agents {
		b := baselineByID[a.ID]
		ownerByAgent[a.ID] = a.Owner
		ensureWallet(a.Owner)
		if a.Owner != "" {
			wp := walletPriors[a.Owner]
			if b.ExternalScore > wp.external {
				wp.external = b.ExternalScore
			}
			walletPriors[a.Owner] = wp
		}
		agentNodes = append(agentNodes, trustpropagation.GraphNode{
			ID:        agentNodePrefix + a.ID,
			Kind:      trustpropagation.NodeKindAgent,
			DirectRep: scoring.AgentDirectReputation(b.ReputationScore, b.AdoptionScore, b.ServicesScore, b.ComplianceScore, b.WeightMass > 0, cw),
			Weight:    b.WeightMass,
			OwnerID:   a.Owner,
		})
	}

	// Reviewer reliability per client wallet, from its valid-feedback count in the
	// (quality-only) snapshot. junkCount is unobservable here, so 0.
	validCount := make(map[string]int64)
	type edgeKey struct{ from, to string }
	edgeCount := make(map[edgeKey]float64)
	for _, fb := range data.Feedbacks {
		if fb.ClientAddress == "" {
			continue
		}
		validCount[fb.ClientAddress]++
		ensureWallet(fb.ClientAddress)
		edgeCount[edgeKey{from: fb.ClientAddress, to: agentNodePrefix + fb.AgentID}]++
	}
	for addr, cnt := range validCount {
		wp := walletPriors[addr]
		wp.reliability = scoring.ReviewerReliability(cnt, 0)
		walletPriors[addr] = wp
	}

	edges := make([]trustpropagation.GraphEdge, 0, len(edgeCount))
	for k, n := range edgeCount {
		edges = append(edges, trustpropagation.GraphEdge{From: k.from, To: k.to, Weight: n})
	}

	return graphTemplate{
		agentNodes:   agentNodes,
		walletPriors: walletPriors,
		edges:        edges,
		ownerByAgent: ownerByAgent,
	}
}

// ConvergencePoint records the iteration count power iteration took for one Alpha.
type ConvergencePoint struct {
	Alpha      float64
	Iterations int
}

// ConvergenceCurve sweeps Alpha and records how many iterations EigenTrust power
// iteration took to converge, holding epsilon/maxIter and the teleport prior
// (default TeleportGamma) fixed. Points are returned in the order of the supplied
// alphas (caller passes them ascending).
func ConvergenceCurve(data snapshot.SnapshotData, alphas []float64, epsilon float64, maxIter int) []ConvergencePoint {
	gd := buildGraphTemplate(data).materialize(extscore.TeleportGamma)
	out := make([]ConvergencePoint, 0, len(alphas))
	for _, a := range alphas {
		_, iters := trustpropagation.EigenTrustPass(gd, trustpropagation.IterConfig{
			Alpha:   a,
			Epsilon: epsilon,
			MaxIter: maxIter,
		})
		out = append(out, ConvergencePoint{Alpha: a, Iterations: iters})
	}
	return out
}
