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

// WriteWalletScores bulk-writes rated wallet trust scores and clears unrated wallets.
func WriteWalletScores(ctx context.Context, deps WriterDeps, ws WalletScores) error {
	now := time.Now().Unix()
	rated := make([]agentrep.WalletScore, 0, len(ws.Rated))
	for id, score := range ws.Rated {
		rated = append(rated, agentrep.WalletScore{ID: id, Score: score, At: now})
	}
	if err := deps.WalletRepo.BulkSetTrustScore(ctx, rated); err != nil {
		return fmt.Errorf("write rated scores: %w", err)
	}
	if err := deps.WalletRepo.BulkSetUnrated(ctx, ws.Unrated, now); err != nil {
		return fmt.Errorf("write unrated scores: %w", err)
	}
	return nil
}
