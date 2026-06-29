// feedback-others consumes erc8004.feedback.others events, runs the LLM fallback
// to resolve the category for rule-undecided feedback, then grades with the shared routine.
//
// Required env vars: MONGO_URI, MONGO_DATABASE_ANALYZED_AGENTS, RABBITMQ_URI
// Optional: MONGO_COLLECTION_WALLETS, MONGO_COLLECTION_FEEDBACK_HISTORY,
//           MONGO_COLLECTION_AGENTS, MONGO_COLLECTION_AGENT_SCORE_STATS,
//           LLM_BASE_URL, AI_SERVICE_MODEL, LLM_TIMEOUT_SECONDS,
//           TRUST_WEIGHT_COLD_START_T0, TRUST_WI_BASE,
//           FEEDBACK_OTHERS_WORKERS, FEEDBACK_OTHERS_PREFETCH
//           (fall back to TRUST_GRAPH_WORKERS / TRUST_GRAPH_PREFETCH for backward compat)
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
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
	mqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
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
	agentRepo := agentrepo.NewRepository(
		db,
		envOr("MONGO_COLLECTION_AGENTS", "agents"),
		envOr("MONGO_COLLECTION_AGENT_SCORE_STATS", "agent_score_stats"),
	)

	if err := walletRepo.EnsureIndexes(ctx); err != nil {
		log.Printf("feedback-others: ensure wallet indexes: %v", err)
	}

	conn, err := amqp.Dial(mustEnv("RABBITMQ_URI"))
	must(err, "rabbitmq connect")
	defer conn.Close()

	publisher, err := mqinfra.NewMultiPublisher(conn)
	must(err, "rabbitmq multi-publisher")
	defer publisher.Close()

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
		log.Println("feedback-others: LLM_BASE_URL not set; others feedback will be held pending")
	}

	propCfg := scoring.DefaultQualityWeightConfig()
	propCfg.WiBase = envFloat("TRUST_WI_BASE", propCfg.WiBase)

	app := feedbackother.NewApp(feedbackother.Deps{
		Conn:         conn,
		FeedbackRepo: feedbackRepo,
		WalletRepo:   walletRepo,
		AgentRepo:    agentRepo,
		PropCfg:      propCfg,
		Cfg: feedbackother.AppConfig{
			ColdStartT0: envFloat("TRUST_WEIGHT_COLD_START_T0", 10.0),
			Workers:     envIntAlias("FEEDBACK_OTHERS_WORKERS", "TRUST_GRAPH_WORKERS", 8),
			Prefetch:    envIntAlias("FEEDBACK_OTHERS_PREFETCH", "TRUST_GRAPH_PREFETCH", 10),
		},
		Classifier: hybridClassifier,
		Publisher:  publisher,
	})

	log.Println("feedback-others: starting")
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("feedback-others: %v", err)
	}
	log.Println("feedback-others: stopped")
}

// envIntAlias reads newKey first; if absent, tries oldKey and logs a one-line
// deprecation warning so existing .env files keep working during the transition.
func envIntAlias(newKey, oldKey string, def int) int {
	if v := os.Getenv(newKey); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	if v := os.Getenv(oldKey); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			log.Printf("feedback-others: %s is deprecated, use %s instead", oldKey, newKey)
			return i
		}
	}
	return def
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
