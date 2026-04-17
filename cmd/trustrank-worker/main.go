package main

// trustrank-worker — Stream 2: cron, per-chain reads events up to URI cursor,
// processes in strict chronological order, computes TrustRank scores.

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
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"
)

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
	analyzedDB := mc.Database(cfg.AnalyzedDatabase)

	contractsRepo := configrepo.NewContractsRepository(db, cfg.ContractsColl)
	cfgRepo := configrepo.NewConfigRepository(db, cfg.ConfigColl)
	eventsRepo := eventrepo.NewRepository(db, cfg.EventsColl)
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl)
	identities := identityrepo.NewRepository(analyzedDB, cfg.IdentityHistColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	scores := scorerepo.NewRepository(analyzedDB, cfg.ScoreHistColl)
	offchain := offchainrepo.NewRepository(db, cfg.OffchainColl)

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

	formulaCfg := scoring.FormulaConfig{
		Alpha:     cfg.TrustRankAlpha,
		Beta:      cfg.TrustRankBeta,
		K:         cfg.TrustRankK,
		TBaseDays: cfg.TrustRankTBase,
		Gamma:     cfg.PenaltyGamma,
		Theta:     cfg.PenaltyTheta,
		SBase:     0.0,
	}

	proc := domaintrustrank.NewProcessor(agents, identities, feedbacks, scores, offchain, formulaCfg)

	app := trustrankapp.NewApp(
		contractsRepo,
		eventsRepo,
		cfgRepo,
		cfg.TrustRankInterval,
		cfg.TrustRankEventBatchSize,
		proc,
	)

	log.Printf("trustrank_worker started interval=%s", cfg.TrustRankInterval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("trustrank_worker stopped: %v", err)
	}
}
