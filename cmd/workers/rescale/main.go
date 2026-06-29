package main

// rescale — retroactive ValueScale correction worker.
// Polls changed_tag_scales for pending corrections and updates affected feedback rows
// with the corrected ValueScale + re-derived category (Phase 3).
// Score mass is reconciled on the next score-refresh replay cycle.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	rescaleapp "erc-8004-benchmarking-be/internal/app/rescale"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
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
		MaxPoolSize:     cfg.MongoMaxPoolSize,
		MinPoolSize:     cfg.MongoMinPoolSize,
		MaxConnIdleTime: cfg.MongoMaxConnIdleMs,
	})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	analyzedDB := mc.Database(cfg.AnalyzedDatabase)

	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	corrections := tagstatsrepo.NewCorrectionRepository(analyzedDB, cfg.TagCorrectionsColl)

	if err := feedbacks.EnsureIndexes(ctx); err != nil {
		log.Fatalf("feedback_history indexes: %v", err)
	}

	interval := time.Duration(cfg.DecayIntervalHours) * time.Hour / 2 // half the decay interval ≈ 2h default

	app := rescaleapp.NewApp(feedbacks, corrections, interval)

	log.Printf("rescale started interval=%s", interval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("rescale stopped: %v", err)
	}
}
