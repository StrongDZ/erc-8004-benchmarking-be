// feedback-others consumes erc8004.feedback.others events and runs the LLM fallback
// to resolve the category for rule-undecided feedback.  It persists the resolved category
// via UpdateFallback only; grading and wallet updates are handled by the score-refresh replay.
//
// Required env vars: MONGO_URI, MONGO_DATABASE_ANALYZED_AGENTS, RABBITMQ_URI
// Optional: MONGO_COLLECTION_FEEDBACK_HISTORY, MONGO_COLLECTION_AGENTS,
//
//	MONGO_COLLECTION_AGENT_SCORE_STATS,
//	LLM_BASE_URL, AI_SERVICE_MODEL, LLM_TIMEOUT_SECONDS,
//	FEEDBACK_OTHERS_WORKERS, FEEDBACK_OTHERS_PREFETCH
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/app/feedbackother"
	"erc-8004-benchmarking-be/internal/domain/classifier"
	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoClient, err := mongoinfra.NewClient(ctx, mustEnv("MONGO_URI"), mongoinfra.DefaultPoolOptions())
	must(err, "mongo connect")
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database(mustEnv("MONGO_DATABASE_ANALYZED_AGENTS"))
	feedbackRepo := feedbackrepo.NewRepository(db, envOr("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history"))
	agentRepo := agentrepo.NewRepository(
		db,
		envOr("MONGO_COLLECTION_AGENTS", "agents"),
		envOr("MONGO_COLLECTION_AGENT_SCORE_STATS", "agent_score_stats"),
	)

	conn, err := amqp.Dial(mustEnv("RABBITMQ_URI"))
	must(err, "rabbitmq connect")
	defer conn.Close()

	var hybridClassifier *classifier.HybridClassifier
	if llmURL := envOr("LLM_BASE_URL", ""); llmURL != "" {
		// AI_SERVICE_MODEL selects the ai-service classify backend: empty → Ollama
		// LLM (V7), "3tier" → per-tag SVM + agent-domain cosine, "knn"/"linear"/…
		aiModel := envOr("AI_SERVICE_MODEL", "")
		ai := classifier.NewAIClient(classifier.AIClientConfig{
			BaseURL:        llmURL,
			Model:          aiModel,
			PromptVersion:  "v7",
			TimeoutSeconds: envInt("LLM_TIMEOUT_SECONDS", 120),
		})
		hybridClassifier = classifier.NewHybridClassifier(ai)
		log.Printf("feedback-others: AI fallback enabled at %s (model=%q)", llmURL, aiModel)
	} else {
		log.Println("feedback-others: LLM_BASE_URL not set; others feedback will be nack'd until LLM is available")
	}

	app := feedbackother.NewApp(feedbackother.Deps{
		Conn:         conn,
		FeedbackRepo: feedbackRepo,
		AgentRepo:    agentRepo,
		Cfg: feedbackother.AppConfig{
			Workers:  envInt("FEEDBACK_OTHERS_WORKERS", 8),
			Prefetch: envInt("FEEDBACK_OTHERS_PREFETCH", 10),
		},
		Classifier: hybridClassifier,
	})

	log.Println("feedback-others: starting")
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("feedback-others: %v", err)
	}
	log.Println("feedback-others: stopped")
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
