package trustpropagation

// loader.go — LoadGraph streams wallets, agents, and feedback edges from Mongo.
//
// Edge sourcing strategy (two-pass):
//   1. ScanAllNonJunkEdges — includes verdict="valid", "legacy", and "" (unprocessed).
//      wi=0 (not yet computed by trust-graph-updater) is replaced with defaultWi=1.0
//      so propagation is meaningful even before trust-graph-updater has run.
//   2. Owner→agent edges are derived from agents.owner (direct scan) so the graph
//      is fully connected even when the wallets collection is empty.
//
// Address normalization: clientAddress and owner are lowercased before constructing
// nodeIDs so they match wallet document _id keys (WalletDocumentID lowercases).

import (
	"context"
	"fmt"
	"strings"

	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrep "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsr "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

const defaultEdgeWi = 1.0 // weight for feedback not yet processed by trust-graph-updater

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

	// Fetch owner→agent links directly from agents collection so the graph is
	// connected even when the wallets collection hasn't been populated yet.
	ownerEdges, err := deps.AgentRepo.ScanOwnerEdges(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: owner edges: %w", err)
	}

	// Use the broad non-junk scan so unprocessed feedback (verdict="" / "legacy")
	// participates in propagation with a default weight.
	feedbackEdges, err := deps.FeedbackRepo.ScanAllNonJunkEdges(ctx, chainID)
	if err != nil {
		return GraphData{}, fmt.Errorf("load graph: edges: %w", err)
	}

	// Build node map: wallets first, then agents from score_stats.
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

	graphEdges := make([]GraphEdge, 0, len(feedbackEdges)+len(ownerEdges))

	// Feedback edges: sender (wallet) → recipient (agent).
	// Normalize clientAddress to lowercase to match WalletDocumentID format.
	for _, e := range feedbackEdges {
		wi := e.Wi
		if wi <= 0 {
			wi = defaultEdgeWi
		}
		fromID := nodeID(e.ChainID, strings.ToLower(e.ClientAddress))
		toID := nodeID(e.ChainID, e.AgentID)

		// Ensure sender node exists (lazy-create wallet stub).
		if _, ok := nodeMap[fromID]; !ok {
			nodeMap[fromID] = GraphNode{ID: fromID, Kind: NodeKindWallet, TrustScore: 10}
		}
		// Ensure recipient node exists.
		if _, ok := nodeMap[toID]; !ok {
			nodeMap[toID] = GraphNode{ID: toID, Kind: NodeKindAgent, TrustScore: 50}
		}
		graphEdges = append(graphEdges, GraphEdge{From: fromID, To: toID, Weight: wi})
	}

	// Owner edges: wallet → agent, weight = 1.0 constant.
	// Derived from agents.owner (authoritative) so we don't need wallet docs.
	// Normalize owner address to lowercase.
	for _, oe := range ownerEdges {
		ownerAddr := strings.ToLower(oe.Owner)
		if ownerAddr == "" {
			continue
		}
		fromID := nodeID(oe.ChainID, ownerAddr)
		toID := nodeID(oe.ChainID, oe.AgentID)

		if _, ok := nodeMap[fromID]; !ok {
			nodeMap[fromID] = GraphNode{ID: fromID, Kind: NodeKindWallet, TrustScore: 10}
		}
		if _, ok := nodeMap[toID]; !ok {
			nodeMap[toID] = GraphNode{ID: toID, Kind: NodeKindAgent, TrustScore: 50}
		}
		graphEdges = append(graphEdges, GraphEdge{From: fromID, To: toID, Weight: 1.0})
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
