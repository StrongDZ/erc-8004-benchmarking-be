package main

// wallet-external-backfill — one-off seed of erc8004.wallet_external_cache from
// already-enriched analyzed_agents.wallets (external.present == true).
//
// Run once to capture wallets enriched before this cache existed, or again
// before a planned analyzed_agents reset to capture wallets enriched since
// the last run. Idempotent — re-running re-upserts the same values.
//
// Run: go run ./cmd/tools/wallet-external-backfill

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

const backfillBatchSize = 1000

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("wallet-external-backfill: config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("wallet-external-backfill: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	src := wallet.NewRepository(client.Database(cfg.AnalyzedDatabase), cfg.WalletColl)
	cache := wallet.NewRepository(client.Database(cfg.MongoDatabase), cfg.WalletExternalCacheColl)

	wallets, err := src.Find(ctx,
		bson.M{"external.present": true},
		options.Find().SetProjection(bson.M{"_id": 1, "external": 1}).SetBatchSize(10000),
	)
	if err != nil {
		log.Fatalf("wallet-external-backfill: scan wallets: %v", err)
	}
	log.Printf("wallet-external-backfill: found %d enriched wallet(s)", len(wallets))

	for i := 0; i < len(wallets); i += backfillBatchSize {
		end := i + backfillBatchSize
		if end > len(wallets) {
			end = len(wallets)
		}
		updates := make([]wallet.ExternalUpdate, 0, end-i)
		for _, w := range wallets[i:end] {
			updates = append(updates, wallet.ExternalUpdate{ID: w.ID, Doc: w.External})
		}
		if err := cache.BulkSetExternal(ctx, updates); err != nil {
			log.Fatalf("wallet-external-backfill: write cache batch %d-%d: %v", i, end, err)
		}
		log.Printf("wallet-external-backfill: cached %d/%d", end, len(wallets))
	}
	log.Print("wallet-external-backfill: done")
}
