// wallet-enrich is the event-driven external wallet trust enrichment daemon.
//
// It consumes erc8004.wallet_enrich — published whenever UpsertCold or
// ReconcileOwnership inserts a brand-new wallet — micro-batches a cache-first
// cheap RPC pass (balance+nonce), then enqueues rich explorer and ENS work
// onto rate-limited worker pools. A periodic cron sweep (and one run at
// startup) catches any wallet that still needs enrichment, independent of
// the queue.
//
// Required env vars: MONGO_URI, MONGO_DATABASE, MONGO_DATABASE_ANALYZED_AGENTS, RABBITMQ_URI
// Optional: ETHERSCAN_API_KEYS, WALLET_ENRICH_WORKERS, WALLET_ENRICH_PREFETCH,
//
//	WALLET_ENRICH_EXPLORER_RATE, WALLET_ENRICH_ENS_RATE, WALLET_ENRICH_ENS_WORKERS,
//	WALLET_ENRICH_SWEEP_CRON
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/app/extenrich"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("wallet-enrich: config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("wallet-enrich: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	conn, err := amqp.Dial(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("wallet-enrich: rabbitmq connect: %v", err)
	}
	defer conn.Close()

	httpc := &http.Client{Timeout: 15 * time.Second}

	explorerClients := make([]*extenrich.ExplorerClient, 0, len(cfg.EtherscanAPIKeys))
	for _, key := range cfg.EtherscanAPIKeys {
		explorerClients = append(explorerClients, extenrich.NewExplorerClient(httpc, key))
	}

	ensClient := extenrich.NewENSClient(httpc)

	app := extenrich.New(ctx, cfg, client, httpc, explorerClients, cfg.WalletEnrichWorkers, cfg.WalletEnrichExplorerRate, ensClient, cfg.WalletEnrichENSRate, cfg.WalletEnrichENSWorkers, extenrich.DaemonConfig{
		Conn:      conn,
		Prefetch:  cfg.WalletEnrichPrefetch,
		SweepCron: cfg.WalletEnrichSweepCron,
	})

	log.Println("wallet-enrich: starting")
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("wallet-enrich: %v", err)
	}
	log.Println("wallet-enrich: stopped")
}
