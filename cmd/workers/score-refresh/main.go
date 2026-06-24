package main

// score-refresh — Periodic worker: every ScoreRefreshCron, replays feedback_history
// per agent to compute reputationScore and delta snapshots (24h / 7d / 30d),
// then upserts agent_score_stats and syncs agent.reputationScore + composite breakdown.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	scorerefreshapp "erc-8004-benchmarking-be/internal/app/scorerefresh"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	scorestatsrepo "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
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
	// offchain_data lives in the raw events DB alongside the uri-bootstrap writer, not in analyzed_agents.
	rawDB := mc.Database(cfg.MongoDatabase)
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl, cfg.ScoreStatsColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	scoreStats := scorestatsrepo.NewRepository(analyzedDB, cfg.ScoreStatsColl)
	offchain := offchainrepo.NewRepository(rawDB, cfg.OffchainColl)
	wallets := walletrepo.NewRepository(analyzedDB, cfg.WalletColl)

	conn, err := amqp.Dial(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer conn.Close()

	if err := agents.EnsureIndexes(ctx); err != nil {
		log.Fatalf("agents indexes: %v", err)
	}
	if err := scoreStats.EnsureIndexes(ctx); err != nil {
		log.Fatalf("score_stats indexes: %v", err)
	}

	formulaCfg := scoring.FormulaConfig{
		Alpha:        cfg.TrustRankAlpha,
		TBaseDays:    cfg.TrustRankTBase,
		C:            cfg.ConfidenceC,
		Gamma:        cfg.PenaltyGamma,
		Theta:        cfg.PenaltyTheta,
		AdoptionURef: cfg.AdoptionURef,
		SBase:        0.0,
	}

	compositeWeights := scoring.CompositeWeights{
		Reputation: cfg.ScoreWeightReputation,
		Adoption:   cfg.ScoreWeightAdoption,
		Services:   cfg.ScoreWeightServices,
		Publisher:  cfg.ScoreWeightPublisher,
		Compliance: cfg.ScoreWeightCompliance,
	}

	complianceWeights := scoring.ComplianceWeights{
		Tier1Total: cfg.ComplianceTier1Weight,
		Tier2Total: cfg.ComplianceTier2Weight,
	}

	app := scorerefreshapp.NewApp(
		agents, feedbacks, scoreStats, offchain, wallets,
		formulaCfg, compositeWeights, complianceWeights,
		cfg.ScoreRefreshCron,
		conn,
	)

	log.Printf("score-refresh started cron=%q", cfg.ScoreRefreshCron)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("score-refresh stopped: %v", err)
	}
}
