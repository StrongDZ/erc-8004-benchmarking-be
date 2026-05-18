package classifier

// llm_client.go — LLM client core: types, constructor, Classify dispatch, HealthCheck, output parsing.
// Backend transport lives in llm_backends.go.
// Prompt templates and message builders live in prompts.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// LLMResult is the structured output from the LLM classifier.
type LLMResult struct {
	Category      Category `json:"category"`
	Confidence    float64  `json:"confidence"`
	Reason        string   `json:"reason"`
	Source        string   `json:"-"` // "llm" | "fallback"
	LowConfidence bool     `json:"-"` // true when 0.50 <= confidence < 0.70
	ModelVer      string   `json:"-"`
}

// LLMMode selects the backend API protocol.
type LLMMode string

const (
	ModeOllama   LLMMode = "ollama"
	ModeLlamaCpp LLMMode = "llamacpp"
)

// LLMClient calls a self-hosted LLM via HTTP.
// Safe for concurrent use — no shared mutable state.
type LLMClient struct {
	baseURL       string
	model         string
	mode          LLMMode
	timeout       time.Duration
	promptVersion PromptVersion
	httpClient    *http.Client
}

// LLMClientConfig holds all tunable parameters for the LLM client.
type LLMClientConfig struct {
	BaseURL        string
	Model          string
	Mode           LLMMode
	TimeoutSeconds int           // defaults to 120 if 0 (cold model load + CPU infer can exceed a few seconds)
	PromptVersion  PromptVersion // empty / unknown → PromptVersionCompact3B
}

// NewLLMClient constructs an LLMClient from config.
func NewLLMClient(cfg LLMClientConfig) *LLMClient {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	return &LLMClient{
		baseURL:       strings.TrimRight(cfg.BaseURL, "/"),
		model:         cfg.Model,
		mode:          cfg.Mode,
		timeout:       timeout,
		promptVersion: resolvePromptVersion(cfg.PromptVersion),
		httpClient: &http.Client{
			Timeout: timeout + 2*time.Second, // slightly higher than LLM timeout
		},
	}
}

// PromptVersion returns the resolved prompt version this client uses.
func (c *LLMClient) PromptVersion() PromptVersion {
	return c.promptVersion
}

// Classify sends a classification request to the LLM.
// On any error or invalid response it returns category="others" with source="fallback".
//
// endpoint and scale are optional enrichment fields surfaced into the user
// message. Pass "" to omit either — older callers can keep doing so without
// behaviour change.
func (c *LLMClient) Classify(
	ctx context.Context,
	tag1, tag2 string,
	valueNorm float64,
	offchainContent string,
	agentDescription string,
	agentServices string,
	agentTags []string,
	endpoint string,
	scale string,
) LLMResult {
	llmCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var raw string
	var err error

	switch c.mode {
	case ModeOllama:
		raw, err = c.callOllama(llmCtx, tag1, tag2, valueNorm, offchainContent, agentDescription, agentServices, agentTags, endpoint, scale)
	case ModeLlamaCpp:
		raw, err = c.callLlamaCpp(llmCtx, tag1, tag2, valueNorm, offchainContent, agentDescription, agentServices, agentTags, endpoint, scale)
	default:
		err = fmt.Errorf("unknown LLM mode: %s", c.mode)
	}

	if err != nil {
		return LLMResult{
			Category:   CategoryOthers,
			Confidence: 0.0,
			Source:     "fallback",
			Reason:     fmt.Sprintf("llm_error: %v", err),
		}
	}

	return parseLLMOutput(raw)
}

// HealthCheck verifies the LLM endpoint is reachable.
// Returns nil when healthy.
func (c *LLMClient) HealthCheck(ctx context.Context) error {
	var url string
	switch c.mode {
	case ModeOllama:
		url = c.baseURL + "/api/tags"
	case ModeLlamaCpp:
		url = c.baseURL + "/health"
	default:
		return fmt.Errorf("unknown LLM mode: %s", c.mode)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("llm health check: build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("llm health check: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("llm health check: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// ─── Output parsing ───────────────────────────────────────────────────────────

// parseLLMOutput extracts and validates the JSON payload from raw LLM text.
func parseLLMOutput(raw string) LLMResult {
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start == -1 || end == -1 || end <= start {
		return LLMResult{
			Category:   CategoryOthers,
			Confidence: 0.0,
			Source:     "fallback",
			Reason:     "no json in llm output",
		}
	}
	raw = raw[start : end+1]

	var res LLMResult
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return LLMResult{
			Category:   CategoryOthers,
			Confidence: 0.0,
			Source:     "fallback",
			Reason:     "json parse error",
		}
	}

	// Validate category is one of the 6 known values.
	validCategories := map[Category]bool{
		CategoryService: true,
		CategoryConfig:  true,
		CategoryApp:     true,
		CategoryOthers:  true,
		CategorySpam:    true,
		CategoryNoise:   true,
	}
	if !validCategories[res.Category] {
		res.Category = CategoryOthers
		res.Confidence = 0.0
	}

	// Clamp confidence to [0, 1].
	if res.Confidence < 0 {
		res.Confidence = 0
	}
	if res.Confidence > 1 {
		res.Confidence = 1
	}

	// Confidence below 0.50 → unsafe to trust, downgrade to "others".
	if res.Confidence < 0.50 {
		res.Category = CategoryOthers
	}

	res.LowConfidence = res.Confidence >= 0.50 && res.Confidence < 0.70
	res.Source = "llm"
	res.ModelVer = "qwen2.5-3b-v1"
	return res
}

