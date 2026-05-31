package main

// desc-summarizer — Stream X: consume agent-description summary jobs.
//
// Subscribes to QueueAgentDescSummary, calls the AI service /summarize endpoint
// for each unique description, and writes agents.summarizedDescription{,Hash,At}.
// Runs in parallel with the main event pipeline; failure is non-fatal to the
// identity flow (the producer publishes best-effort).

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	descsumapp "erc-8004-benchmarking-be/internal/app/descsummarizer"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	rabbitmqclient "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// MongoDB — agents collection lives in the analyzed_agents database.
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

	if err := agents.EnsureIndexes(ctx); err != nil {
		log.Fatalf("agents indexes: %v", err)
	}

	// RabbitMQ.
	conn, err := rabbitmqclient.NewConn(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer conn.Close()

	queue := envOr("RABBITMQ_DESC_SUMMARY_QUEUE", mq.QueueAgentDescSummary)
	aiURL := envOr("AI_SERVICE_URL", "http://localhost:8000")
	model := os.Getenv("DESC_SUMMARIZER_MODEL")
	prefetch := envInt("DESC_SUMMARIZER_CONCURRENCY", 4)
	timeoutS := envInt("DESC_SUMMARIZER_TIMEOUT_SECONDS", 30)
	maxAttempts := envInt("DESC_SUMMARIZER_MAX_ATTEMPTS", 3)

	app := descsumapp.NewApp(conn, agents, descsumapp.Config{
		AIServiceURL:       aiURL,
		QueueName:          queue,
		Prefetch:           prefetch,
		RequestTimeoutSecs: timeoutS,
		MaxAttempts:        maxAttempts,
		Model:              model,
	})

	log.Printf("desc-summarizer starting queue=%s ai=%s prefetch=%d", queue, aiURL, prefetch)
	if err := app.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("desc-summarizer: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
