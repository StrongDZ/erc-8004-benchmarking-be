package trustpropagation

// writer.go — WritePropagedScores bulk-writes trustScorePropagated to wallets + agents.

import (
	"context"
	"fmt"
	"time"

	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

// WriterDeps holds repo dependencies for WritePropagedScores.
type WriterDeps struct {
	WalletRepo *walletrep.Repository
	AgentRepo  *agentrep.Repository
}

// BuildNodeKindMap returns nodeID → NodeKind from GraphData.
func BuildNodeKindMap(gd GraphData) map[string]NodeKind {
	m := make(map[string]NodeKind, len(gd.Nodes))
	for _, nd := range gd.Nodes {
		m[nd.ID] = nd.Kind
	}
	return m
}

// WritePropagedScores routes propagated scores to the correct collection.
func WritePropagedScores(ctx context.Context, deps WriterDeps, scores map[string]float64, nodeKinds map[string]NodeKind) error {
	now := time.Now().Unix()

	var walletUpdates, agentUpdates []agentrep.PropagatedScore
	for id, score := range scores {
		ps := agentrep.PropagatedScore{ID: id, Score: score, At: now}
		if nodeKinds[id] == NodeKindAgent {
			agentUpdates = append(agentUpdates, ps)
		} else {
			walletUpdates = append(walletUpdates, ps)
		}
	}

	if err := deps.WalletRepo.BulkSetPropagated(ctx, walletUpdates); err != nil {
		return fmt.Errorf("write propagated: wallets: %w", err)
	}
	if err := deps.AgentRepo.BulkSetPropagated(ctx, agentUpdates); err != nil {
		return fmt.Errorf("write propagated: agents: %w", err)
	}
	return nil
}
