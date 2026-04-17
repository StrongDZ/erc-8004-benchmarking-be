package main

// uri-bootstrap — Stream 1: cron daemon that scans decoded events per chain,
// fetches any URI content (IPFS/HTTPS/data), persists to offchain_data,
// and advances per-chain cursor for TrustRank (Stream 2).

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	bootstrapapp "erc-8004-benchmarking-be/internal/app/uri_bootstrap"
	"erc-8004-benchmarking-be/internal/config"
	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	httpclient "erc-8004-benchmarking-be/internal/infra/https"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

type rawFetchAdapter struct {
	c *httpclient.Client
}

func (a *rawFetchAdapter) Fetch(ctx context.Context, url string) ([]byte, error) {
	body, _, _, err := a.c.FetchBody(ctx, url)
	return body, err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	db := mc.Database(cfg.MongoDatabase)

	contractsRepo := configrepo.NewContractsRepository(db, cfg.ContractsColl)
	cfgRepo := configrepo.NewConfigRepository(db, cfg.ConfigColl)
	eventsRepo := eventrepo.NewRepository(db, cfg.EventsColl)
	offchain := offchainrepo.NewRepository(db, cfg.OffchainColl)

	if err := cfgRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("config indexes: %v", err)
	}
	if err := eventsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("events indexes: %v", err)
	}
	if err := offchain.EnsureIndexes(ctx); err != nil {
		log.Fatalf("offchain indexes: %v", err)
	}

	httpCl := httpclient.NewClientWithOptions(httpclient.ClientOptions{
		Timeout:   cfg.HTTPSFetchTimeout,
		UserAgent: cfg.HTTPSUserAgent,
	})
	uriFetcher := domainuri.NewResolver(&rawFetchAdapter{c: httpCl}, cfg.IPFSGateway)

	interval := time.Duration(cfg.URIResolverIntervalSec) * time.Second

	app := bootstrapapp.NewApp(
		contractsRepo,
		cfgRepo,
		eventsRepo,
		offchain,
		uriFetcher,
		int64(cfg.URIBootstrapBatchSize),
		interval,
	)

	log.Printf("uri-resolver started interval=%s", interval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("uri-resolver stopped: %v", err)
	}
}
