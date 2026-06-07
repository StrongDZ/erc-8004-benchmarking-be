// trust-graph-updater consumes erc8004.feedback.classified events,
// validates feedback, computes trust weights and deltas, and updates
// wallet trust scores and feedback weighting fields.
//
// Required env vars: MONGO_URI, MONGO_DATABASE_ANALYZED_AGENTS, RABBITMQ_URI
// Optional: MONGO_COLLECTION_WALLETS, MONGO_COLLECTION_FEEDBACK_HISTORY,
//           TRUST_WEIGHT_COLD_START_T0, TRUST_GRAPH_WORKERS, TRUST_GRAPH_PREFETCH
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/app/trustgraph"
	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/propagation"
	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoClient, err := mongoinfra.NewClient(ctx, mustEnv("MONGO_URI"), mongoinfra.DefaultPoolOptions())
	must(err, "mongo connect")
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database(mustEnv("MONGO_DATABASE_ANALYZED_AGENTS"))
	walletRepo := walletrepo.NewRepository(db, envOr("MONGO_COLLECTION_WALLETS", "wallets"))
	feedbackRepo := feedbackrepo.NewRepository(db, envOr("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history"))

	if err := walletRepo.EnsureIndexes(ctx); err != nil {
		log.Printf("trust-graph-updater: ensure wallet indexes: %v", err)
	}

	conn, err := amqp.Dial(mustEnv("RABBITMQ_URI"))
	must(err, "rabbitmq connect")
	defer conn.Close()

	var hybridClassifier *classifier.HybridClassifier
	if llmURL := envOr("LLM_BASE_URL", ""); llmURL != "" {
		ai := classifier.NewAIClient(classifier.AIClientConfig{
			BaseURL:        llmURL,
			TimeoutSeconds: envInt("LLM_TIMEOUT_SECONDS", 120),
		})
		hybridClassifier = classifier.NewHybridClassifier(ai)
		log.Printf("trust-graph-updater: LLM fallback enabled at %s", llmURL)
	} else {
		log.Println("trust-graph-updater: LLM_BASE_URL not set; LLM fallback disabled (others → requeue)")
	}

	app := trustgraph.NewApp(trustgraph.Deps{
		Conn:         conn,
		FeedbackRepo: feedbackRepo,
		WalletRepo:   walletRepo,
		PropCfg:      propagation.DefaultPropagationConfig(),
		Cfg: trustgraph.AppConfig{
			ColdStartT0: envFloat("TRUST_WEIGHT_COLD_START_T0", 10.0),
			Workers:     envInt("TRUST_GRAPH_WORKERS", 8),
			Prefetch:    envInt("TRUST_GRAPH_PREFETCH", 10),
		},
		Classifier: hybridClassifier,
	})

	log.Println("trust-graph-updater: starting")
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("trust-graph-updater: %v", err)
	}
	log.Println("trust-graph-updater: stopped")
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing required env %s", k)
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

func must(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}
