package main

// feedback-grader consumes erc8004.feedback.classified and live-updates each agent's
// reputation score. For every fully-classified feedback (rule-decided at ingest or
// LLM-resolved by feedback-others), it grades the row and — for quality feedback — applies
// one weighted-mean mass increment to agent_score_stats, mirroring one iteration of the
// score-refresh replay. The verdict (IsGraded marker) is written last. score-refresh stays
// the authoritative engine; these live writes are a fast approximation it subsumes each cycle.
//
// Required env vars: MONGO_URI, MONGO_DATABASE_ANALYZED_AGENTS, RABBITMQ_URI.
// Optional: collection overrides, scoring knobs (TRUST_WI_BASE, etc.),
//
//	FEEDBACK_GRADER_WORKERS, FEEDBACK_GRADER_PREFETCH.

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/app/feedbackgrade"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsrepo "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
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
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl, cfg.ScoreStatsColl)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	scoreStats := scorestatsrepo.NewRepository(analyzedDB, cfg.ScoreStatsColl)
	wallets := walletrepo.NewRepository(analyzedDB, cfg.WalletColl)

	conn, err := amqp.Dial(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer conn.Close()

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

	// Quality-weight config drives GradeFeedback. WiBase is overridable via TRUST_WI_BASE,
	// matching score-refresh so the live grade equals the replay grade.
	qwCfg := scoring.DefaultQualityWeightConfig()
	qwCfg.WiBase = utils.GetenvFloat("TRUST_WI_BASE", qwCfg.WiBase)

	app := feedbackgrade.NewApp(feedbackgrade.Deps{
		Conn:             conn,
		FeedbackRepo:     feedbacks,
		ScoreStatsRepo:   scoreStats,
		AgentRepo:        agents,
		WalletRepo:       wallets,
		Cfg:              feedbackgrade.AppConfig{Workers: utils.GetenvInt("FEEDBACK_GRADER_WORKERS", 8), Prefetch: utils.GetenvInt("FEEDBACK_GRADER_PREFETCH", 10)},
		QWCfg:            qwCfg,
		FormulaCfg:       formulaCfg,
		CompositeWeights: compositeWeights,
	})

	log.Println("feedback-grader: starting")
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("feedback-grader: %v", err)
	}
	log.Println("feedback-grader: stopped")
}
