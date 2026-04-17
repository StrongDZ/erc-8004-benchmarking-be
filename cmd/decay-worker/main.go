package main

// decay-worker — Luồng 3: mỗi N giờ, materialize time decay cho tất cả agents.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	decayapp "erc-8004-benchmarking-be/internal/app/decay"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
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

	analyzedDB := mc.Database(cfg.AnalyzedDatabase)
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl)
	scores := scorerepo.NewRepository(analyzedDB, cfg.ScoreHistColl)

	if err := agents.EnsureIndexes(ctx); err != nil {
		log.Fatalf("agents indexes: %v", err)
	}
	if err := scores.EnsureIndexes(ctx); err != nil {
		log.Fatalf("score_history indexes: %v", err)
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

	interval := time.Duration(cfg.DecayIntervalHours) * time.Hour

	app := decayapp.NewApp(agents, scores, formulaCfg, interval)

	log.Printf("decay_worker started interval=%s", interval)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("decay_worker stopped: %v", err)
	}
}
