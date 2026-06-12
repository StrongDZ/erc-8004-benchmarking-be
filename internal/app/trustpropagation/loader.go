package trustpropagation

// loader.go — builds the propagation graph from Mongo.
// Nodes: wallets (DirectRep = reviewer reliability) + agents (DirectRep =
// composite-minus-publisher). Edges: client→agent, weight = count of VALID
// feedbacks per (client,agent). agent.OwnerID set from agents.owner.

import (
	"context"
	"fmt"
	"strings"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrep "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsr "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

type LoaderDeps struct {
	WalletRepo     *walletrep.Repository
	AgentRepo      *agentrep.Repository
	ScoreStatsRepo *scorestatsr.Repository
	FeedbackRepo   *feedbackrep.Repository
}

func LoadGraph(ctx context.Context, deps LoaderDeps, chainID int64) (GraphData, error) {
	cw := scoring.DefaultCompositeWeights()

	wallets, err := deps.WalletRepo.ScanAll(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: wallets: %w", err)
	}
	components, err := deps.ScoreStatsRepo.ScanComponentScores(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: agent components: %w", err)
	}
	ownerEdges, err := deps.AgentRepo.ScanOwnerEdges(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: owner edges: %w", err)
	}
	feedbackEdges, err := deps.FeedbackRepo.ScanValidEdges(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: edges: %w", err)
	}

	nodeMap := make(map[string]GraphNode, len(wallets)+len(components))
	for _, w := range wallets {
		reliability := scoring.ReviewerReliability(w.FeedbackValidCount, w.FeedbackJunkCount)
		nodeMap[w.ID] = GraphNode{ID: w.ID, Kind: NodeKindWallet,
			DirectRep: extscore.BlendTeleport(reliability, w.External.Score, w.External.Present, extscore.TeleportGamma)}
	}
	for _, c := range components {
		id := nodeID(c.ChainID, c.AgentID)
		nodeMap[id] = GraphNode{ID: id, Kind: NodeKindAgent,
			DirectRep: scoring.AgentDirectReputation(c.ReputationNorm, c.AdoptionScore,
				c.ServicesScore, c.ComplianceScore, c.WeightMass > 0, cw),
			Weight: c.WeightMass}
	}
	for _, oe := range ownerEdges {
		ownerAddr := strings.ToLower(oe.Owner)
		if ownerAddr == "" {
			continue
		}
		ownerNode := nodeID(oe.ChainID, ownerAddr)
		agentNode := nodeID(oe.ChainID, oe.AgentID)
		if _, ok := nodeMap[ownerNode]; !ok {
			nodeMap[ownerNode] = GraphNode{ID: ownerNode, Kind: NodeKindWallet, DirectRep: 50}
		}
		ag, ok := nodeMap[agentNode]
		if !ok {
			ag = GraphNode{ID: agentNode, Kind: NodeKindAgent, DirectRep: 50}
		}
		ag.OwnerID = ownerNode
		nodeMap[agentNode] = ag
	}

	type pair struct{ from, to string }
	counts := make(map[pair]float64, len(feedbackEdges))
	for _, e := range feedbackEdges {
		if e.Wi <= 0 {
			continue
		}
		fromID := nodeID(e.ChainID, strings.ToLower(e.ClientAddress))
		toID := nodeID(e.ChainID, e.AgentID)
		if _, ok := nodeMap[fromID]; !ok {
			nodeMap[fromID] = GraphNode{ID: fromID, Kind: NodeKindWallet, DirectRep: 50}
		}
		if _, ok := nodeMap[toID]; !ok {
			nodeMap[toID] = GraphNode{ID: toID, Kind: NodeKindAgent, DirectRep: 50}
		}
		counts[pair{fromID, toID}]++
	}
	edges := make([]GraphEdge, 0, len(counts))
	for p, n := range counts {
		edges = append(edges, GraphEdge{From: p.from, To: p.to, Weight: n})
	}

	nodes := make([]GraphNode, 0, len(nodeMap))
	for _, nd := range nodeMap {
		nodes = append(nodes, nd)
	}
	return GraphData{Nodes: nodes, Edges: edges}, nil
}

func nodeID(chainID int64, id string) string { return fmt.Sprintf("%d:%s", chainID, id) }
