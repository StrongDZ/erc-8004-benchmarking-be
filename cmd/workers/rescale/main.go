package main

// rescale — retroactive vi correction worker.
// Polls changed_tag_scales for pending corrections and re-scores affected agents
// using $inc (additive delta), so it runs safely in parallel with live scoring.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	rescaleapp "erc-8004-benchmarking-be/internal/app/rescale"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsrepo "erc-8004-benchmarking-be/internal/repository/scorestats"
	tagstatsrepo "erc-8004-benchmarking-be/internal/repository/tagstats"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{
		MaxPoolSize:    cfg.MongoMaxPoolSize,
		MinPoolSize:    cfg.MongoMinPoolSize,
		MaxConnIdleTime: cfg.MongoMaxConnIdleMs,
	})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	analyzedDB := mc.Database(cfg.AnalyzedDatabase)

	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl, cfg.ScoreStatsColl)
	stats := scorestatsrepo.NewRepository(analyzedDB, cfg.ScoreStatsColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	corrections := tagstatsrepo.NewCorrectionRepository(analyzedDB, cfg.TagCorrectionsColl)
	deltas := tagstatsrepo.NewDeltaRepository(analyzedDB, cfg.RescaleDeltasColl)

	if err := agents.EnsureIndexes(ctx); err != nil {
		log.Fatalf("agents indexes: %v", err)
	}
	if err := feedbacks.EnsureIndexes(ctx); err != nil {
		log.Fatalf("feedback_history indexes: %v", err)
	}

	formulaCfg := scoring.FormulaConfig{
		Alpha:     cfg.TrustRankAlpha,
		Beta:      cfg.TrustRankBeta,
		K:         cfg.TrustRankK,
		TBaseDays: cfg.TrustRankTBase,
		Gamma:     cfg.PenaltyGamma,
		Theta:     cfg.PenaltyTheta,
	}

	interval := time.Duration(cfg.DecayIntervalHours) * time.Hour / 2 // half the decay interval ≈ 2h default

	app := rescaleapp.NewApp(mc, agents, stats, feedbacks, corrections, deltas, formulaCfg, interval)

	log.Printf("rescale started interval=%s", interval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("rescale stopped: %v", err)
	}
}
