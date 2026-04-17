package crawler

// queries.go — read helpers for /admin/indexer-status.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// ListByChain returns all crawler states for one chain.
func (r *Repository) ListByChain(ctx context.Context, chainID int64) ([]CrawlerState, error) {
	docs, err := r.Find(ctx, bson.M{"chainId": chainID})
	if err != nil {
		return nil, fmt.Errorf("crawler repo: list by chain: %w", err)
	}
	return docs, nil
}

// ListAll returns all crawler states across all chains.
func (r *Repository) ListAll(ctx context.Context) ([]CrawlerState, error) {
	docs, err := r.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("crawler repo: list all: %w", err)
	}
	return docs, nil
}
