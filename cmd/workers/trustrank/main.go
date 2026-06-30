package main

// trustrank — Stream 2: periodic full pass over decoded events per chain (checkpoint → uri_fetched),
// computes TrustRank scores. First pass runs at startup; TRUSTRANK_INTERVAL_SECONDS pauses between passes.
//
// Also starts a shared ServiceURIConsumerPool: one AMQP reader per chain on
// uri.{chainID}.service_uri, with parallel HTTP workers across all chains.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	tagstatsrepo "erc-8004-benchmarking-be/internal/repository/tagstats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
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
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl, cfg.ScoreStatsColl)
	identities := identityrepo.NewRepository(analyzedDB, cfg.IdentityHistColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	offchain := offchainrepo.NewRepository(db, cfg.OffchainColl)
	tagStats := tagstatsrepo.NewStatsRepository(analyzedDB, cfg.TagStatsColl)
	tagCorrs := tagstatsrepo.NewCorrectionRepository(analyzedDB, cfg.TagCorrectionsColl)
	wallets := walletrepo.NewRepository(analyzedDB, envOr("MONGO_COLLECTION_WALLETS", "wallets"))
	coldStartT0 := envFloat("TRUST_WEIGHT_COLD_START_T0", 10.0)

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
	if err := offchain.EnsureIndexes(ctx); err != nil {
		log.Fatalf("offchain_data indexes: %v", err)
	}
	if err := wallets.EnsureIndexes(ctx); err != nil {
		log.Fatalf("wallets indexes: %v", err)
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

	fbPub, err := mqinfra.NewMultiPublisher(mqConn)
	if err != nil {
		log.Fatalf("trustrank: new feedback publisher: %v", err)
	}
	defer fbPub.Close()

	// URI resolver used by the service_uri worker pool.
	httpCl := httpclient.NewClientWithOptions(httpclient.ClientOptions{
		Timeout:   cfg.HTTPSFetchTimeout,
		UserAgent: cfg.HTTPSUserAgent,
	})
	resolver := domainuri.NewResolver(&rawFetchAdapter{c: httpCl}, cfg.IPFSGateway, cfg.ArweaveGateway)

	svcConsumer := trustrankapp.NewServiceURIConsumer(
		mqConn, offchain, resolver, cfg.URIConsumerPrefetch, cfg.ServiceURIConsumerWorkers,
	)

	formulaCfg := scoring.FormulaConfig{
		Alpha:        cfg.TrustRankAlpha,
		TBaseDays:    cfg.TrustRankTBase,
		C:            cfg.ConfidenceC,
		Gamma:        cfg.PenaltyGamma,
		Theta:        cfg.PenaltyTheta,
		AdoptionURef: cfg.AdoptionURef,
		SBase:        0.0,
	}

	proc := domaintrustrank.NewProcessor(agents, identities, feedbacks, offchain, formulaCfg, publisher, fbPub, tagStats, tagCorrs, cfg.TagStatsMinSamples, wallets, coldStartT0)

	app := trustrankapp.NewApp(
		contractsRepo,
		eventsRepo,
		cfgRepo,
		cfg.TrustRankInterval,
		cfg.TrustRankEventBatchSize,
		proc,
		svcConsumer.EnsureChain,
	)

	log.Printf("trustrank started pause_between_passes=%s (first pass runs immediately)", cfg.TrustRankInterval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("trustrank stopped: %v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
