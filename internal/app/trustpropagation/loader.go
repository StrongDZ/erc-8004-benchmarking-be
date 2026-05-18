package trustpropagation

// loader.go — LoadGraph streams wallets, agents, and valid feedback edges from Mongo.

import (
	"context"
	"fmt"

	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrep "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsr "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

// LoaderDeps holds repo dependencies for LoadGraph.
type LoaderDeps struct {
	WalletRepo     *walletrep.Repository
	AgentRepo      *agentrep.Repository
	ScoreStatsRepo *scorestatsr.Repository
	FeedbackRepo   *feedbackrep.Repository
}

// LoadGraph builds a GraphData snapshot from Mongo for the given chainID.
// Pass chainID=0 to load all chains.
func LoadGraph(ctx context.Context, deps LoaderDeps, chainID int64) (GraphData, error) {
	wallets, err := deps.WalletRepo.ScanAll(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: wallets: %w", err)
	}

	agentScores, err := deps.ScoreStatsRepo.ScanCompositeScores(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: agent scores: %w", err)
	}

	feedbackEdges, err := deps.FeedbackRepo.ScanValidEdges(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: edges: %w", err)
	}

	// Build node map.
	nodeMap := make(map[string]GraphNode, len(wallets)+len(agentScores))
	for _, w := range wallets {
		nodeMap[w.ID] = GraphNode{
			ID:         w.ID,
			Kind:       NodeKindWallet,
			TrustScore: w.TrustScore,
		}
	}
	for _, as := range agentScores {
		id := nodeID(as.ChainID, as.AgentID)
		nodeMap[id] = GraphNode{
			ID:             id,
			Kind:           NodeKindAgent,
			TrustScore:     as.CompositeScore,
			CompositeScore: as.CompositeScore,
		}
	}

	// Build edges.
	graphEdges := make([]GraphEdge, 0, len(feedbackEdges)+len(wallets))
	for _, e := range feedbackEdges {
		fromID := nodeID(e.ChainID, e.ClientAddress)
		toID := nodeID(e.ChainID, e.AgentID)
		graphEdges = append(graphEdges, GraphEdge{From: fromID, To: toID, Weight: e.Wi})
		if _, ok := nodeMap[toID]; !ok {
			nodeMap[toID] = GraphNode{ID: toID, Kind: NodeKindAgent, TrustScore: 50}
		}
	}
	// Owner edges (wallet → owned agent, weight = 1.0).
	for _, w := range wallets {
		for _, agentID := range w.OwnedAgentIDs {
			toID := nodeID(w.ChainID, agentID)
			graphEdges = append(graphEdges, GraphEdge{From: w.ID, To: toID, Weight: 1.0})
			if _, ok := nodeMap[toID]; !ok {
				nodeMap[toID] = GraphNode{ID: toID, Kind: NodeKindAgent, TrustScore: 50}
			}
		}
	}

	nodes := make([]GraphNode, 0, len(nodeMap))
	for _, nd := range nodeMap {
		nodes = append(nodes, nd)
	}
	return GraphData{Nodes: nodes, Edges: graphEdges}, nil
}

func nodeID(chainID int64, id string) string {
	return fmt.Sprintf("%d:%s", chainID, id)
}
