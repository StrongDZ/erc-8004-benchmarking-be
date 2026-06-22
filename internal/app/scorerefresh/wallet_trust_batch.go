package scorerefresh

import (
	"context"
	"fmt"

	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

const defaultWalletTrustScore = 50.0

// walletTrustLoader loads trust scores for wallet document ids.
type walletTrustLoader interface {
	BulkGetTrustScores(ctx context.Context, ids []string) (map[string]float64, error)
}

// WalletTrustBatch holds per-batch wallet trust scores keyed by wallet _id.
type WalletTrustBatch struct {
	scores map[string]float64
}

// NewEmptyWalletTrustBatch returns a batch with no loaded scores (TrustScore defaults to 50).
func NewEmptyWalletTrustBatch() *WalletTrustBatch {
	return &WalletTrustBatch{scores: map[string]float64{}}
}

// NewWalletTrustBatch bulk-loads trust scores for the given wallet document ids.
func NewWalletTrustBatch(ctx context.Context, repo walletTrustLoader, ids []string) (*WalletTrustBatch, error) {
	scores, err := repo.BulkGetTrustScores(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("wallet trust batch: %w", err)
	}
	if scores == nil {
		scores = map[string]float64{}
	}
	return &WalletTrustBatch{scores: scores}, nil
}

// LoadedCount returns how many wallet ids had a stored trust score in this batch.
func (b *WalletTrustBatch) LoadedCount() int {
	if b == nil {
		return 0
	}
	return len(b.scores)
}

// TrustScore returns the wallet trust for (chainID, address), defaulting to 50 when absent.
func (b *WalletTrustBatch) TrustScore(chainID int64, address string) float64 {
	if address == "" {
		return defaultWalletTrustScore
	}
	id := walletrepo.WalletDocumentID(chainID, utils.NormalizeAddress(address))
	if b != nil {
		if score, ok := b.scores[id]; ok {
			return score
		}
	}
	return defaultWalletTrustScore
}

// PublisherProvider builds a publisher score provider from this batch's loaded scores.
func (b *WalletTrustBatch) PublisherProvider() WalletTrustPublisherProvider {
	if b == nil || len(b.scores) == 0 {
		return NewWalletTrustPublisherProvider(nil)
	}
	rated := make([]walletrepo.RatedTrust, 0, len(b.scores))
	for id, score := range b.scores {
		rated = append(rated, walletrepo.RatedTrust{ID: id, TrustScore: score})
	}
	return NewWalletTrustPublisherProvider(rated)
}

// CollectWalletIDs gathers unique wallet document ids from agent owners and feedback reviewers.
func CollectWalletIDs(agents []agentrepo.AgentDocument, feedbacksByAgent [][]feedbackrepo.FeedbackRecord) []string {
	seen := make(map[string]struct{})
	var ids []string
	add := func(chainID int64, address string) {
		if address == "" {
			return
		}
		id := walletrepo.WalletDocumentID(chainID, utils.NormalizeAddress(address))
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, ag := range agents {
		add(ag.ChainID, ag.Owner)
	}
	for _, fbs := range feedbacksByAgent {
		for _, fb := range fbs {
			if fb.IsRevoked || fb.IsSelfFeedback {
				continue
			}
			add(fb.ChainID, fb.ClientAddress)
		}
	}
	return ids
}
