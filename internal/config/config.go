package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"erc-8004-benchmarking-be/internal/utils"
)

type Config struct {
	// MongoDB
	MongoURI         string
	MongoDatabase    string
	AnalyzedDatabase string
	ContractsColl    string
	CrawlersColl     string
	EventsColl       string // decoded events collection
	OffchainColl     string
	ConfigColl       string
	AgentsColl       string
	IdentityHistColl string
	ScoreHistColl    string
	FeedbackHistColl    string
	TagStatsColl        string // tag_value_stats — per-tag tier vote counters
	TagCorrectionsColl  string // changed_tag_scales — rescale correction queue
	RescaleDeltasColl   string // rescale_deltas — pre-computed per-agent deltas
	ScoreStatsColl      string // agent_score_stats — periodic materialized view

	// Score-refresh worker
	ScoreRefreshCron string // cron expression, default "*/30 * * * *"

	// Tag-scale normalization
	TagStatsMinSamples int // feedbacks required before scale is locked (default 50)

	// Crawler / indexer
	BatchSize           uint64
	PollInterval        time.Duration // sleep between successful full crawl cycles (near head / live)
	ErrorPollInterval   time.Duration // sleep after a cycle that returned an error (backoff)
	RPCTimeout          time.Duration
	CrawlMaxConcurrency int // max concurrent (chain, contract) jobs; default 12
	RPCMaxRetries       int // per-RPC retry rounds after first failure
	RetryBackoff        time.Duration
	RetryStrategy       string // linear | exponential | fibonacci | random

	// RabbitMQ
	RabbitMQURI      string
	RabbitMQQueue    string // raw logs queue
	RabbitMQURIQueue string // uri fetch queue
	ConsumerPrefetch    int // number of unacked messages the raw-log consumer can hold at once
	URIConsumerPrefetch int // prefetch per AMQP channel: event_uri consumer; service_uri pool has one reader channel per chain with this prefetch

	// ServiceURIConsumerWorkers is the shared HTTP worker pool size for all uri.{chain}.service_uri queues (trustrank).
	ServiceURIConsumerWorkers int

	URIBootstrapBatchSize  int
	URIResolverIntervalSec int

	// TrustRank worker (Flow 2): pause between full passes; first pass runs at worker startup.
	TrustRankInterval             time.Duration
	TrustRankEventBatchSize       int64 // Mongo page size per chain (block/log order).
	TrustRankContractTypeSubBatch int   // max events per handler call within one contractType slice.

	// TrustRank formula params (§3.1–§3.4)
	TrustRankAlpha float64 // §3.2 minimum difficulty weight (default 1.0)
	TrustRankBeta  float64 // §3.2 difficulty amplifier (default 1.5)
	TrustRankK     float64 // §3.2 micro-unit amplifier (default 100.0)
	TrustRankTBase float64 // §3.3 half-life base in days (default 15.0)
	PenaltyGamma   float64 // §3.4 penalty base (default 5.0)
	PenaltyTheta   float64 // §3.4 penalty exponent (default 2.0)

	// Decay cron
	DecayIntervalHours int // how often the decay cron fires (default 4)

	// IPFS gateway for URI resolution
	IPFSGateway string
	// Arweave gateway for URI resolution
	ArweaveGateway string

	// HTTPS client for URI resolver (Stream 1): long timeout tolerates cold starts on hosts like Render.
	HTTPSFetchTimeout time.Duration
	HTTPSUserAgent    string

	// LLM classifier (Stage 3B fallback)
	LLMEnabled                  bool
	LLMMode                     string        // "ollama" | "llamacpp"
	LLMBaseURL                  string        // e.g. "http://localhost:11434"
	LLMModel                    string        // e.g. "qwen2.5:3b-instruct-q4_K_M"
	LLMTimeoutSeconds           int           // per-request timeout (default 3)
	LLMConfidenceThresholdLow   float64       // low-confidence floor (default 0.50)
	LLMConfidenceThresholdAccept float64      // accept threshold (default 0.70)
	LLMFallbackOnError          bool          // return "others" on LLM error (default true)
	LLMKeepAlive                time.Duration // Ollama keep-alive (default 30m)

	// REST API (cmd/workers/api)
	APIPort              int
	APIReadTimeout       time.Duration
	APIWriteTimeout      time.Duration
	APIShutdownTimeout   time.Duration
	APIAllowedOrigins    []string // CORS origins; "*" = allow all
	APICacheDefaultTTL   time.Duration
	AdminAPIKey          string // required header X-API-Key for /admin/*

	// Redis (realtime Pub/Sub fan-out between consumer and API WS hub)
	RedisURL            string // e.g. redis://localhost:6379/0
	RedisEventsChannel  string // Pub/Sub channel for decoded realtime events

	// MongoDB connection pool tuning
	MongoMaxPoolSize    uint64        // default 200
	MongoMinPoolSize    uint64        // default 10
	MongoMaxConnIdleMs  time.Duration // default 10m

	// Rate limiting (per IP, token bucket)
	RateLimitRPS   float64 // requests per second per IP (default 20)
	RateLimitBurst int     // burst depth (default 40)
}

// loadDotEnv loads the first .env found walking upward from the process working directory.
// This covers `go test ./tests/...` where cwd is the package dir, not the repo root.
func loadDotEnv() {
	wd, err := os.Getwd()
	if err != nil {
		return
	}
	dir := wd
	for i := 0; i < 12; i++ {
		path := filepath.Join(dir, ".env")
		if err := godotenv.Load(path); err == nil {
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return
		}
		dir = parent
	}
}

func Load() (Config, error) {
	loadDotEnv()

	cfg := Config{
		MongoURI:         utils.Getenv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:    utils.Getenv("MONGO_DATABASE", "erc8004"),
		AnalyzedDatabase: utils.Getenv("MONGO_DATABASE_ANALYZED_AGENTS", "analyzed_agents"),
		ContractsColl:    utils.Getenv("MONGO_COLLECTION_CONTRACTS", "contracts"),
		CrawlersColl:     utils.Getenv("MONGO_COLLECTION_CRAWLERS", "crawlers"),
		EventsColl:       utils.Getenv("MONGO_COLLECTION_EVENTS", "events"),
		OffchainColl:     utils.Getenv("MONGO_COLLECTION_OFFCHAIN_DATA", "offchain_data"),
		ConfigColl:       utils.Getenv("MONGO_COLLECTION_CONFIG", "config"),
		AgentsColl:       utils.Getenv("MONGO_COLLECTION_AGENTS", "agents"),
		IdentityHistColl: utils.Getenv("MONGO_COLLECTION_IDENTITY_HISTORY", "identity_history"),
		ScoreHistColl:    utils.Getenv("MONGO_COLLECTION_SCORE_HISTORY", "score_history"),
		FeedbackHistColl:   utils.Getenv("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history"),
		TagStatsColl:       utils.Getenv("MONGO_COLLECTION_TAG_VALUE_STATS", "tag_value_stats"),
		TagCorrectionsColl: utils.Getenv("MONGO_COLLECTION_TAG_CORRECTIONS", "changed_tag_scales"),
		RescaleDeltasColl:  utils.Getenv("MONGO_COLLECTION_RESCALE_DELTAS", "rescale_deltas"),
		ScoreStatsColl:     utils.Getenv("MONGO_COLLECTION_SCORE_STATS", "agent_score_stats"),

		ScoreRefreshCron: utils.Getenv("SCORE_REFRESH_CRON", "*/30 * * * *"),

		TagStatsMinSamples: utils.GetenvInt("TAG_STATS_MIN_SAMPLES", 1),

		BatchSize:           uint64(utils.GetenvInt("CRAWLER_BATCH_SIZE", 1000)),
		PollInterval:        time.Duration(utils.GetenvInt("CRAWLER_POLL_SECONDS", 5)) * time.Second,
		ErrorPollInterval:   time.Duration(utils.GetenvInt("CRAWLER_ERROR_POLL_SECONDS", 30)) * time.Second,
		RPCTimeout:          time.Duration(utils.GetenvInt("CRAWLER_RPC_TIMEOUT_SECONDS", 15)) * time.Second,
		CrawlMaxConcurrency: utils.GetenvInt("CRAWLER_MAX_CONCURRENCY", 12),
		RPCMaxRetries:       utils.GetenvInt("CRAWLER_RPC_MAX_RETRIES", 3),
		RetryBackoff:        time.Duration(utils.GetenvInt("CRAWLER_RETRY_BACKOFF_MS", 100)) * time.Millisecond,
		RetryStrategy:       utils.Getenv("CRAWLER_RETRY_STRATEGY", "linear"),

		RabbitMQURI:      utils.Getenv("RABBITMQ_URI", "amqp://guest:guest@localhost:5672/"),
		RabbitMQQueue:    utils.Getenv("RABBITMQ_QUEUE", "erc8004.raw_logs"),
		RabbitMQURIQueue: utils.Getenv("RABBITMQ_URI_QUEUE", "erc8004.agent_uri"),
		ConsumerPrefetch:    utils.GetenvInt("CONSUMER_PREFETCH", 10),
		URIConsumerPrefetch: utils.GetenvInt("URI_CONSUMER_PREFETCH", 5),
		ServiceURIConsumerWorkers: utils.GetenvInt("SERVICE_URI_CONSUMER_WORKERS", 8),

		URIBootstrapBatchSize:  utils.GetenvInt("URI_BOOTSTRAP_BATCH_SIZE", 1000),
		URIResolverIntervalSec: utils.GetenvInt("URI_RESOLVER_INTERVAL_SECONDS", 30),

		TrustRankInterval:             time.Duration(utils.GetenvInt("TRUSTRANK_INTERVAL_SECONDS", 300)) * time.Second,
		TrustRankEventBatchSize:       int64(utils.GetenvInt("TRUSTRANK_EVENT_BATCH_SIZE", 500)),
		TrustRankContractTypeSubBatch: utils.GetenvInt("TRUSTRANK_CONTRACT_TYPE_SUB_BATCH", 50),

		TrustRankAlpha: utils.GetenvFloat("TRUSTRANK_ALPHA", 1.0),
		TrustRankBeta:  utils.GetenvFloat("TRUSTRANK_BETA", 1.5),
		TrustRankK:     utils.GetenvFloat("TRUSTRANK_K", 100.0),
		TrustRankTBase: utils.GetenvFloat("TRUSTRANK_TBASE_DAYS", 15.0),
		PenaltyGamma:   utils.GetenvFloat("PENALTY_GAMMA", 5.0),
		PenaltyTheta:   utils.GetenvFloat("PENALTY_THETA", 2.0),

		DecayIntervalHours: utils.GetenvInt("DECAY_INTERVAL_HOURS", 4),

		IPFSGateway:    utils.Getenv("IPFS_GATEWAY", "https://ipfs.io/ipfs/"),
		ArweaveGateway: utils.Getenv("ARWEAVE_GATEWAY", "https://arweave.net/"),

		HTTPSFetchTimeout: time.Duration(utils.GetenvInt("HTTPS_FETCH_TIMEOUT_SECONDS", 90)) * time.Second,
		HTTPSUserAgent:    utils.Getenv("HTTPS_USER_AGENT", "erc8004-uri-bootstrap/1.0"),

		LLMEnabled:                   utils.GetenvBool("LLM_ENABLED", false),
		LLMMode:                      utils.Getenv("LLM_MODE", "ollama"),
		LLMBaseURL:                   utils.Getenv("LLM_BASE_URL", "http://localhost:11434"),
		LLMModel:                     utils.Getenv("LLM_MODEL", "qwen2.5:3b-instruct-q4_K_M"),
		LLMTimeoutSeconds:            utils.GetenvInt("LLM_TIMEOUT_SECONDS", 120),
		LLMConfidenceThresholdLow:    utils.GetenvFloat("LLM_CONFIDENCE_THRESHOLD_LOW", 0.50),
		LLMConfidenceThresholdAccept: utils.GetenvFloat("LLM_CONFIDENCE_THRESHOLD_ACCEPT", 0.70),
		LLMFallbackOnError:           utils.GetenvBool("LLM_FALLBACK_ON_ERROR", true),
		LLMKeepAlive:                 time.Duration(utils.GetenvInt("LLM_KEEP_ALIVE_MINUTES", 30)) * time.Minute,

		APIPort:            utils.GetenvInt("API_PORT", 8080),
		APIReadTimeout:     time.Duration(utils.GetenvInt("API_READ_TIMEOUT_SECONDS", 15)) * time.Second,
		APIWriteTimeout:    time.Duration(utils.GetenvInt("API_WRITE_TIMEOUT_SECONDS", 30)) * time.Second,
		APIShutdownTimeout: time.Duration(utils.GetenvInt("API_SHUTDOWN_TIMEOUT_SECONDS", 15)) * time.Second,
		APIAllowedOrigins:  splitCSV(utils.Getenv("API_ALLOWED_ORIGINS", "*")),
		APICacheDefaultTTL: time.Duration(utils.GetenvInt("API_CACHE_DEFAULT_TTL_SECONDS", 60)) * time.Second,
		AdminAPIKey:        utils.Getenv("ADMIN_API_KEY", ""),

		RedisURL:           utils.Getenv("REDIS_URL", "redis://localhost:6379/0"),
		RedisEventsChannel: utils.Getenv("REDIS_EVENTS_CHANNEL", "erc8004:events:realtime"),

		MongoMaxPoolSize:   uint64(utils.GetenvInt("MONGO_MAX_POOL_SIZE", 200)),
		MongoMinPoolSize:   uint64(utils.GetenvInt("MONGO_MIN_POOL_SIZE", 10)),
		MongoMaxConnIdleMs: time.Duration(utils.GetenvInt("MONGO_MAX_CONN_IDLE_MINUTES", 10)) * time.Minute,

		RateLimitRPS:   utils.GetenvFloat("RATE_LIMIT_RPS", 20),
		RateLimitBurst: utils.GetenvInt("RATE_LIMIT_BURST", 40),
	}

	if strings.TrimSpace(cfg.MongoURI) == "" {
		return Config{}, fmt.Errorf("MONGO_URI must not be empty")
	}
	if strings.TrimSpace(cfg.MongoDatabase) == "" {
		return Config{}, fmt.Errorf("MONGO_DATABASE must not be empty")
	}
	if strings.TrimSpace(cfg.AnalyzedDatabase) == "" {
		return Config{}, fmt.Errorf("MONGO_DATABASE_ANALYZED_AGENTS must not be empty")
	}
	if cfg.BatchSize == 0 {
		return Config{}, fmt.Errorf("CRAWLER_BATCH_SIZE must be > 0")
	}
	if cfg.PollInterval <= 0 {
		return Config{}, fmt.Errorf("CRAWLER_POLL_SECONDS must be > 0")
	}
	if cfg.ErrorPollInterval <= 0 {
		return Config{}, fmt.Errorf("CRAWLER_ERROR_POLL_SECONDS must be > 0")
	}
	if cfg.RPCTimeout <= 0 {
		return Config{}, fmt.Errorf("CRAWLER_RPC_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.CrawlMaxConcurrency < 1 {
		return Config{}, fmt.Errorf("CRAWLER_MAX_CONCURRENCY must be >= 1")
	}
	if cfg.RPCMaxRetries < 0 {
		return Config{}, fmt.Errorf("CRAWLER_RPC_MAX_RETRIES must be >= 0")
	}
	if cfg.RetryBackoff <= 0 {
		return Config{}, fmt.Errorf("CRAWLER_RETRY_BACKOFF_MS must be > 0")
	}
	if strings.TrimSpace(cfg.RabbitMQURI) == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URI must not be empty")
	}
	if cfg.ServiceURIConsumerWorkers < 1 {
		cfg.ServiceURIConsumerWorkers = 8
	}
	if cfg.ServiceURIConsumerWorkers > 256 {
		cfg.ServiceURIConsumerWorkers = 256
	}
	if strings.TrimSpace(cfg.RabbitMQURIQueue) == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URI_QUEUE must not be empty")
	}
	if cfg.URIBootstrapBatchSize <= 0 {
		return Config{}, fmt.Errorf("URI_BOOTSTRAP_BATCH_SIZE must be > 0")
	}
	if cfg.TrustRankInterval <= 0 {
		return Config{}, fmt.Errorf("TRUSTRANK_INTERVAL_SECONDS must be > 0")
	}
	if cfg.TrustRankEventBatchSize < 1 {
		return Config{}, fmt.Errorf("TRUSTRANK_EVENT_BATCH_SIZE must be >= 1")
	}
	if cfg.TrustRankContractTypeSubBatch < 1 {
		return Config{}, fmt.Errorf("TRUSTRANK_CONTRACT_TYPE_SUB_BATCH must be >= 1")
	}
	if cfg.DecayIntervalHours < 1 {
		return Config{}, fmt.Errorf("DECAY_INTERVAL_HOURS must be >= 1")
	}
	if cfg.HTTPSFetchTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTPS_FETCH_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIPort < 1 || cfg.APIPort > 65535 {
		return Config{}, fmt.Errorf("API_PORT must be in [1, 65535]")
	}
	if cfg.APIReadTimeout <= 0 {
		return Config{}, fmt.Errorf("API_READ_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIWriteTimeout <= 0 {
		return Config{}, fmt.Errorf("API_WRITE_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("API_SHUTDOWN_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.TrustRankAlpha <= 0 {
		return Config{}, fmt.Errorf("TRUSTRANK_ALPHA must be > 0")
	}
	if cfg.TrustRankBeta < 0 {
		return Config{}, fmt.Errorf("TRUSTRANK_BETA must be >= 0")
	}
	if cfg.TrustRankK <= 0 {
		return Config{}, fmt.Errorf("TRUSTRANK_K must be > 0")
	}
	if cfg.TrustRankTBase <= 0 {
		return Config{}, fmt.Errorf("TRUSTRANK_TBASE_DAYS must be > 0")
	}
	if cfg.RateLimitRPS <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_RPS must be > 0")
	}
	if cfg.RateLimitBurst < 1 {
		return Config{}, fmt.Errorf("RATE_LIMIT_BURST must be >= 1")
	}

	return cfg, nil
}

// splitCSV splits a comma-separated string into a trimmed slice, skipping empty tokens.
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
