package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	indexerapp "erc-8004-benchmarking-be/internal/app/indexer"
	"erc-8004-benchmarking-be/internal/config"
	evmcrawler "erc-8004-benchmarking-be/internal/crawler/evm"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	rabbitmqclient "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	"erc-8004-benchmarking-be/pkg/retry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// --- MongoDB ---
	mongoClient, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{MaxPoolSize: cfg.MongoMaxPoolSize, MinPoolSize: cfg.MongoMinPoolSize, MaxConnIdleTime: cfg.MongoMaxConnIdleMs})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mongoClient.Disconnect(context.Background()) }()

	db := mongoClient.Database(cfg.MongoDatabase)
	contractsRepo := contractsrepo.NewContractsRepository(db, cfg.ContractsColl)
	crawlersRepo := crawlerrepo.NewRepository(db, cfg.CrawlersColl)

	if err := contractsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("contracts indexes: %v", err)
	}
	if err := crawlersRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("crawlers indexes: %v", err)
	}

	// --- RabbitMQ ---
	conn, err := rabbitmqclient.NewConn(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer conn.Close()

	publisher, err := rabbitmqclient.NewPublisher(conn, cfg.RabbitMQQueue)
	if err != nil {
		log.Fatalf("mq publisher: %v", err)
	}
	defer publisher.Close()

	// --- App ---
	crawler := evmcrawler.NewCrawler(
		contractsRepo,
		crawlersRepo,
		publisher,
		cfg.BatchSize,
		cfg.RPCTimeout,
		cfg.CrawlMaxConcurrency,
		cfg.RPCMaxRetries,
		cfg.RetryBackoff,
		retry.ParseBackoffStrategy(cfg.RetryStrategy),
		cfg.ReorgSafetyBlocks,
	)

	app := indexerapp.NewApp(crawler, cfg.PollInterval, cfg.ErrorPollInterval)

	log.Printf("indexer started; live_poll=%s error_poll=%s batch=%d max_concurrency=%d queue=%s",
		cfg.PollInterval, cfg.ErrorPollInterval, cfg.BatchSize, cfg.CrawlMaxConcurrency, cfg.RabbitMQQueue)

	app.Run(ctx)
	log.Printf("indexer shutdown")
}
