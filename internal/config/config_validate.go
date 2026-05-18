package config

// config_validate.go — Validation logic for the loaded Config.
// Called by Load() after all defaults are applied.

import (
	"fmt"
	"log"
	"math"
	"strings"
)

// validate checks that required fields are present and values are within acceptable ranges.
// It returns the first fatal error encountered; non-fatal issues are logged as warnings.
func validate(cfg Config) error {
	if strings.TrimSpace(cfg.MongoURI) == "" {
		return fmt.Errorf("MONGO_URI must not be empty")
	}
	if strings.TrimSpace(cfg.MongoDatabase) == "" {
		return fmt.Errorf("MONGO_DATABASE must not be empty")
	}
	if strings.TrimSpace(cfg.AnalyzedDatabase) == "" {
		return fmt.Errorf("MONGO_DATABASE_ANALYZED_AGENTS must not be empty")
	}
	if cfg.BatchSize == 0 {
		return fmt.Errorf("CRAWLER_BATCH_SIZE must be > 0")
	}
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("CRAWLER_POLL_SECONDS must be > 0")
	}
	if cfg.ErrorPollInterval <= 0 {
		return fmt.Errorf("CRAWLER_ERROR_POLL_SECONDS must be > 0")
	}
	if cfg.RPCTimeout <= 0 {
		return fmt.Errorf("CRAWLER_RPC_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.CrawlMaxConcurrency < 1 {
		return fmt.Errorf("CRAWLER_MAX_CONCURRENCY must be >= 1")
	}
	if cfg.RPCMaxRetries < 0 {
		return fmt.Errorf("CRAWLER_RPC_MAX_RETRIES must be >= 0")
	}
	if cfg.RetryBackoff <= 0 {
		return fmt.Errorf("CRAWLER_RETRY_BACKOFF_MS must be > 0")
	}
	if strings.TrimSpace(cfg.RabbitMQURI) == "" {
		return fmt.Errorf("RABBITMQ_URI must not be empty")
	}
	if strings.TrimSpace(cfg.RabbitMQURIQueue) == "" {
		return fmt.Errorf("RABBITMQ_URI_QUEUE must not be empty")
	}
	if cfg.URIBootstrapBatchSize <= 0 {
		return fmt.Errorf("URI_BOOTSTRAP_BATCH_SIZE must be > 0")
	}
	if cfg.TrustRankInterval <= 0 {
		return fmt.Errorf("TRUSTRANK_INTERVAL_SECONDS must be > 0")
	}
	if cfg.TrustRankEventBatchSize < 1 {
		return fmt.Errorf("TRUSTRANK_EVENT_BATCH_SIZE must be >= 1")
	}
	if cfg.TrustRankContractTypeSubBatch < 1 {
		return fmt.Errorf("TRUSTRANK_CONTRACT_TYPE_SUB_BATCH must be >= 1")
	}
	if cfg.DecayIntervalHours < 1 {
		return fmt.Errorf("DECAY_INTERVAL_HOURS must be >= 1")
	}
	if cfg.HTTPSFetchTimeout <= 0 {
		return fmt.Errorf("HTTPS_FETCH_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIPort < 1 || cfg.APIPort > 65535 {
		return fmt.Errorf("API_PORT must be in [1, 65535]")
	}
	if cfg.APIReadTimeout <= 0 {
		return fmt.Errorf("API_READ_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIWriteTimeout <= 0 {
		return fmt.Errorf("API_WRITE_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.APIShutdownTimeout <= 0 {
		return fmt.Errorf("API_SHUTDOWN_TIMEOUT_SECONDS must be > 0")
	}
	if cfg.TrustRankAlpha <= 0 {
		return fmt.Errorf("TRUSTRANK_ALPHA must be > 0")
	}
	if cfg.TrustRankBeta < 0 {
		return fmt.Errorf("TRUSTRANK_BETA must be >= 0")
	}
	if cfg.TrustRankK <= 0 {
		return fmt.Errorf("TRUSTRANK_K must be > 0")
	}
	if cfg.TrustRankTBase <= 0 {
		return fmt.Errorf("TRUSTRANK_TBASE_DAYS must be > 0")
	}
	if cfg.RateLimitRPS <= 0 {
		return fmt.Errorf("RATE_LIMIT_RPS must be > 0")
	}
	if cfg.RateLimitBurst < 1 {
		return fmt.Errorf("RATE_LIMIT_BURST must be >= 1")
	}

	// Validate composite score blend weights sum ≈ 1.0.
	compositeSum := cfg.ScoreWeightReputation + cfg.ScoreWeightServices + cfg.ScoreWeightPublisher + cfg.ScoreWeightCompliance
	if math.Abs(compositeSum-1.0) > 0.001 {
		log.Printf("WARNING: composite score weights sum to %.4f (expected 1.0); check SCORE_WEIGHT_* env vars", compositeSum)
	}
	// Validate compliance tier weights sum ≈ 100.0.
	tierSum := cfg.ComplianceTier1Weight + cfg.ComplianceTier2Weight
	if math.Abs(tierSum-100.0) > 0.1 {
		log.Printf("WARNING: compliance tier weights sum to %.2f (expected 100.0); check COMPLIANCE_TIER*_WEIGHT env vars", tierSum)
	}

	return nil
}
