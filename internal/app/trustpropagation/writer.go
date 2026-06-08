package trustpropagation

// writer.go — persists propagation results into wallets.trustScore (single
// canonical wallet trust score). Agents are conduits; not persisted.

import (
	"context"
	"fmt"
	"time"

	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

type WriterDeps struct {
	WalletRepo *walletrep.Repository
}

// WriteWalletScores bulk-writes wallet trust scores keyed by wallet node ID.
func WriteWalletScores(ctx context.Context, deps WriterDeps, scores map[string]float64) error {
	now := time.Now().Unix()
	updates := make([]agentrep.WalletScore, 0, len(scores))
	for id, score := range scores {
		updates = append(updates, agentrep.WalletScore{ID: id, Score: score, At: now})
	}
	if err := deps.WalletRepo.BulkSetTrustScore(ctx, updates); err != nil {
		return fmt.Errorf("write wallet scores: %w", err)
	}
	return nil
}
