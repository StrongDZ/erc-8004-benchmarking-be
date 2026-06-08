// trustrank-pass runs EigenTrust-style propagation on all chains every
// TRUST_PROPAGATION_BATCH_INTERVAL_MIN minutes. Each tick also re-publishes
// any feedback rows still missing a validationVerdict so trust-graph-updater
// can grade them.
//
// Required: MONGO_URI, MONGO_DATABASE_ANALYZED_AGENTS
// Optional: RABBITMQ_URI (enables backfill); TRUST_BACKFILL_BATCH_SIZE (default 5000)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"erc-8004-benchmarking-be/internal/app/trustpropagation"
	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
	mqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrep "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsr "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrep "erc-8004-benchmarking-be/internal/repository/wallet"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoClient, err := mongoinfra.NewClient(ctx, mustEnv("MONGO_URI"), mongoinfra.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mongo: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database(mustEnv("MONGO_DATABASE_ANALYZED_AGENTS"))
	walletRepo := walletrep.NewRepository(db, envOr("MONGO_COLLECTION_WALLETS", "wallets"))
	agentRepo := agentrep.NewRepository(db,
		envOr("MONGO_COLLECTION_AGENTS", "agents"),
		envOr("MONGO_COLLECTION_AGENT_SCORE_STATS", "agent_score_stats"))
	statsRepo := scorestatsr.NewRepository(db, envOr("MONGO_COLLECTION_AGENT_SCORE_STATS", "agent_score_stats"))
	fbRepo := feedbackrep.NewRepository(db, envOr("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history"))

	var backfillDeps trustpropagation.BackfillDeps
	if rmqURI := os.Getenv("RABBITMQ_URI"); rmqURI != "" {
		mqConn, err := mqinfra.NewConn(rmqURI)
		if err != nil {
			log.Fatalf("rabbitmq dial: %v", err)
		}
		defer mqConn.Close()
		publisher, err := mqinfra.NewMultiPublisher(mqConn)
		if err != nil {
			log.Fatalf("rabbitmq publisher: %v", err)
		}
		defer publisher.Close()
		backfillDeps = trustpropagation.BackfillDeps{
			FeedbackRepo: fbRepo,
			Publisher:    publisher,
			BatchSize:    envInt("TRUST_BACKFILL_BATCH_SIZE", 5000),
		}
		log.Printf("trustrank-pass: backfill enabled (batch=%d)", backfillDeps.BatchSize)
	} else {
		log.Printf("trustrank-pass: backfill disabled (RABBITMQ_URI unset)")
	}

	cfg := trustpropagation.AppConfig{
		IntervalMin: envInt("TRUST_PROPAGATION_BATCH_INTERVAL_MIN", 10),
		IterConfig: trustpropagation.IterConfig{
			Alpha:   envFloat("TRUST_PROPAGATION_ALPHA", 0.85),
			Epsilon: envFloat("TRUST_PROPAGATION_EPSILON", 1e-6),
			MaxIter: envInt("TRUST_PROPAGATION_MAX_ITER", 50),
		},
		LoaderDeps: trustpropagation.LoaderDeps{
			WalletRepo:     walletRepo,
			AgentRepo:      agentRepo,
			ScoreStatsRepo: statsRepo,
			FeedbackRepo:   fbRepo,
		},
		WriterDeps: trustpropagation.WriterDeps{
			WalletRepo: walletRepo,
		},
		BackfillDeps: backfillDeps,
	}

	log.Printf("trustrank-pass: starting (interval=%dm alpha=%.2f epsilon=%g maxIter=%d)",
		cfg.IntervalMin, cfg.IterConfig.Alpha, cfg.IterConfig.Epsilon, cfg.IterConfig.MaxIter)
	trustpropagation.NewApp(cfg).Run(ctx)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing %s", k)
	}
	return v
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

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
