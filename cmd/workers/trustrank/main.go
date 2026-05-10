package main

// trustrank — Stream 2: cron, per-chain reads events up to URI cursor,
// processes in strict chronological order, computes TrustRank scores.
//
// Also starts per-chain ServiceURIConsumers that fetch service endpoint URIs
// published by the TrustRank processor during identity event handling.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	trustrankapp "erc-8004-benchmarking-be/internal/app/trustrank"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	domaintrustrank "erc-8004-benchmarking-be/internal/domain/trustrank"
	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	httpclient "erc-8004-benchmarking-be/internal/infra/https"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	mqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"
	tagstatsrepo "erc-8004-benchmarking-be/internal/repository/tagstats"
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
	analyzedDB := mc.Database(cfg.AnalyzedDatabase)

	contractsRepo := contractsrepo.NewContractsRepository(db, cfg.ContractsColl)
	cfgRepo := configrepo.NewConfigRepository(db, cfg.ConfigColl)
	eventsRepo := eventrepo.NewRepository(db, cfg.EventsColl)
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl)
	identities := identityrepo.NewRepository(analyzedDB, cfg.IdentityHistColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	scores := scorerepo.NewRepository(analyzedDB, cfg.ScoreHistColl)
	offchain := offchainrepo.NewRepository(db, cfg.OffchainColl)
	tagStats := tagstatsrepo.NewStatsRepository(analyzedDB, cfg.TagStatsColl)
	tagCorrs := tagstatsrepo.NewCorrectionRepository(analyzedDB, cfg.TagCorrectionsColl)

	if err := contractsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("contracts indexes: %v", err)
	}
	if err := eventsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("events indexes: %v", err)
	}
	if err := agents.EnsureIndexes(ctx); err != nil {
		log.Fatalf("agents indexes: %v", err)
	}
	if err := identities.EnsureIndexes(ctx); err != nil {
		log.Fatalf("identity_history indexes: %v", err)
	}
	if err := feedbacks.EnsureIndexes(ctx); err != nil {
		log.Fatalf("feedback_history indexes: %v", err)
	}
	if err := scores.EnsureIndexes(ctx); err != nil {
		log.Fatalf("score_history indexes: %v", err)
	}
	if err := offchain.EnsureIndexes(ctx); err != nil {
		log.Fatalf("offchain_data indexes: %v", err)
	}

	// RabbitMQ — publisher for service_uri messages + connection for consumers.
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

	// URI resolver used by ServiceURIConsumers.
	httpCl := httpclient.NewClientWithOptions(httpclient.ClientOptions{
		Timeout:   cfg.HTTPSFetchTimeout,
		UserAgent: cfg.HTTPSUserAgent,
	})
	resolver := domainuri.NewResolver(&rawFetchAdapter{c: httpCl}, cfg.IPFSGateway, cfg.ArweaveGateway)

	// ServiceURIConsumer factory — started per chain by the app on first discovery.
	svcConsumer := trustrankapp.NewServiceURIConsumer(mqConn, offchain, resolver, cfg.URIConsumerPrefetch)
	startSvcConsumer := func(ctx context.Context, chainID int64) {
		if err := svcConsumer.RunChain(ctx, chainID); err != nil && err != context.Canceled {
			log.Printf("service_uri_consumer: chain=%d stopped: %v", chainID, err)
		}
	}

	formulaCfg := scoring.FormulaConfig{
		Alpha:     cfg.TrustRankAlpha,
		Beta:      cfg.TrustRankBeta,
		K:         cfg.TrustRankK,
		TBaseDays: cfg.TrustRankTBase,
		Gamma:     cfg.PenaltyGamma,
		Theta:     cfg.PenaltyTheta,
		SBase:     0.0,
	}

	proc := domaintrustrank.NewProcessor(agents, identities, feedbacks, scores, offchain, formulaCfg, publisher, tagStats, tagCorrs, cfg.TagStatsMinSamples)

	app := trustrankapp.NewApp(
		contractsRepo,
		eventsRepo,
		cfgRepo,
		cfg.TrustRankInterval,
		cfg.TrustRankEventBatchSize,
		proc,
		startSvcConsumer,
	)

	log.Printf("trustrank started interval=%s", cfg.TrustRankInterval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("trustrank stopped: %v", err)
	}
}
