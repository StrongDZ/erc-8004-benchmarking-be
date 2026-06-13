package main

// uri-bootstrap — Stream 1: cron daemon that scans decoded events per chain,
// publishes URI messages to uri.{chainID}.event_uri queues, and advances uri_scanned_X
// (producer scan position). uri_fetched_X is updated by EventURIConsumer after each
// terminal offchain_data upsert; TrustRank (Stream 2) reads uri_fetched_X.
//
// Per-chain EventURIConsumers are started lazily on the first cycle that discovers
// each chain, and run for the lifetime of the process.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	bootstrapapp "erc-8004-benchmarking-be/internal/app/uribootstrap"
	"erc-8004-benchmarking-be/internal/config"
	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	httpclient "erc-8004-benchmarking-be/internal/infra/https"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	mqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
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

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{MaxPoolSize: cfg.MongoMaxPoolSize, MinPoolSize: cfg.MongoMinPoolSize, MaxConnIdleTime: cfg.MongoMaxConnIdleMs})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	db := mc.Database(cfg.MongoDatabase)

	contractsRepo := contractsrepo.NewContractsRepository(db, cfg.ContractsColl)
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

	// RabbitMQ — publisher for event_uri messages.
	mqConn, err := mqinfra.NewConn(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer mqConn.Close()

	publisher, err := mqinfra.NewMultiPublisher(mqConn)
	if err != nil {
		log.Fatalf("rabbitmq publisher: %v", err)
	}
	defer publisher.Close()

	// URI resolver used by EventURIConsumers.
	httpCl := httpclient.NewClientWithOptions(httpclient.ClientOptions{
		Timeout:   cfg.HTTPSFetchTimeout,
		UserAgent: cfg.HTTPSUserAgent,
	})
	resolver := domainuri.NewResolver(&rawFetchAdapter{c: httpCl}, cfg.IPFSGateway, cfg.ArweaveGateway)

	// EventURIConsumer factory — started per chain by the app on first discovery.
	consumer := bootstrapapp.NewEventURIConsumer(mqConn, offchain, resolver, cfgRepo, cfg.URIConsumerPrefetch)
	startConsumer := func(ctx context.Context, chainID int64) {
		consumer.EnsureChain(ctx, chainID)
	}

	interval := time.Duration(cfg.URIResolverIntervalSec) * time.Second

	app := bootstrapapp.NewApp(
		contractsRepo,
		cfgRepo,
		eventsRepo,
		publisher,
		int64(cfg.URIBootstrapBatchSize),
		interval,
		startConsumer,
	)

	log.Printf("uri-bootstrap started interval=%s", interval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("uri-bootstrap stopped: %v", err)
	}
}
