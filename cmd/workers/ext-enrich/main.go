package main

// ext-enrich — external on-chain wallet trust enrichment.
//
// Cheap pass (always runs): batched eth_getBalance + eth_getTransactionCount
// via the RPCs stored in erc8004.contracts. No API key required.
//
// Explorer pass (only if ETHERSCAN_API_KEY is set): Etherscan V2 txlist for
// age + unique counterparties, rate-limited to --rate req/s.
//
// Run: go run ./cmd/workers/ext-enrich [--cheap-only] [--workers=10] [--rate=5]

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"erc-8004-benchmarking-be/internal/app/extenrich"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/extsource/explorer"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	cheapOnly := flag.Bool("cheap-only", false, "run only the cheap RPC pass (balance+nonce), skip explorer")
	workers := flag.Int("workers", 10, "concurrent RPC workers for the cheap pass")
	rate := flag.Float64("rate", 5, "explorer requests per second")
	flag.Parse()

	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("ext-enrich: config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("ext-enrich: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	httpc := &http.Client{Timeout: 15 * time.Second}

	var explorerClient *explorer.Client
	if cfg.EtherscanAPIKey != "" {
		explorerClient = explorer.NewClient(httpc, cfg.EtherscanAPIKey)
	}

	app := extenrich.New(ctx, cfg, client, httpc, explorerClient, *workers, *rate)

	if err := app.RunCheap(ctx); err != nil {
		log.Fatalf("ext-enrich: cheap pass: %v", err)
	}
	log.Print("ext-enrich: cheap pass done")

	if *cheapOnly {
		return
	}
	if err := app.RunExplorer(ctx); err != nil {
		log.Fatalf("ext-enrich: explorer pass: %v", err)
	}
	log.Print("ext-enrich: explorer pass done")
}
