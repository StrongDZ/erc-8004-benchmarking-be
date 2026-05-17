package classifier

// llm_client.go — Self-hosted LLM client for ERC-8004 feedback classification.
// Supports Ollama (/api/chat) and llama.cpp (/v1/chat/completions).
// Falls back to category="others" on any error — never blocks the pipeline.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// ─── Backend callers ──────────────────────────────────────────────────

func (c *LLMClient) callOllama(
	ctx context.Context,
	tag1, tag2 string,
	valueNorm float64,
	offchainContent string,
	agentDescription string,
	agentServices string,
	agentTags []string,
	endpoint string,
	scale string,
) (string, error) {
	payload := map[string]any{
		"model": c.model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPromptFor(c.promptVersion)},
			{"role": "user", "content": userMessageFor(c.promptVersion,
				tag1, tag2, valueNorm,
				offchainContent, agentDescription, agentServices, agentTags,
				endpoint, scale)},
		},
		"stream": false,
		// Structured output: forces category to a valid enum at the grammar level.
		// Supported in Ollama v0.5+; silently ignored by older versions.
		"format": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"category": map[string]any{
					"type": "string",
					"enum": []string{"spam", "noise", "service_feedback", "config_feedback", "app_specific", "others"},
				},
				"confidence": map[string]any{"type": "number"},
				"reason":     map[string]any{"type": "string"},
			},
			"required": []string{"category", "confidence"},
		},
		"options": map[string]any{
			"temperature":    0.0,
			"seed":           42,
			"num_predict":    128,
			"repeat_penalty": 1.3,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("ollama marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		c.baseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		msg := strings.TrimSpace(string(b))
		if resp.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("ollama: HTTP 404 — model %q not found at %s (run `ollama pull %s` or set LLM_MODEL to a name from `ollama list`); body=%s",
				c.model, c.baseURL, c.model, msg)
		}
		return "", fmt.Errorf("ollama: unexpected status %d body=%s", resp.StatusCode, msg)
	}

	var result struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("ollama decode: %w", err)
	}
	return result.Message.Content, nil
}

func (c *LLMClient) callLlamaCpp(
	ctx context.Context,
	tag1, tag2 string,
	valueNorm float64,
	offchainContent string,
	agentDescription string,
	agentServices string,
	agentTags []string,
	endpoint string,
	scale string,
) (string, error) {
	payload := map[string]any{
		"model": c.model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPromptFor(c.promptVersion)},
			{"role": "user", "content": userMessageFor(c.promptVersion,
				tag1, tag2, valueNorm,
				offchainContent, agentDescription, agentServices, agentTags,
				endpoint, scale)},
		},
		"temperature": 0.0,
		"max_tokens":  128,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("llamacpp marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("llamacpp request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("llamacpp: unexpected status %d", resp.StatusCode)
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("llamacpp decode: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("llamacpp: empty choices")
	}
	return result.Choices[0].Message.Content, nil
}

// ─── Output parsing ───────────────────────────────────────────────────

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

// ─── Prompt builder ───────────────────────────────────────────────────

const llmSystemPrompt = `You are a feedback signal classifier for the ERC-8004 protocol — a blockchain standard where AI agents receive structured feedback from clients after completing tasks.

Classify each feedback into exactly one of six categories based on the tags, value, and agent context provided.

Categories:

spam — deliberate manipulation: external links (t.me, telegram, https://), rank-gaming language, promotional text injected to abuse the scoring system.

noise — zero meaningful signal: ONLY when tag1 AND tag2 are both empty, whitespace, or pure placeholder ("test", "asd", "123"). Never use noise if either tag contains a real word.

service_feedback — human quality judgment: subjective adjectives, sentiment, satisfaction rating. The client is saying "this was good/bad".

config_feedback — automated technical measurement: latency, uptime, win-rate, signal-accuracy, reachability probe, health-check. Generated by monitoring systems, not humans.

app_specific — domain operation log: records that a specific action happened within this agent's specialized domain. Use agent_description and agent_services to confirm the operation fits what this agent does.

others — genuinely unclassifiable: ambiguous, random text, emoji-only, or when any classification would require guessing. Prefer others over uncertain service_feedback.

Priority order (stop at first match):
1. Adversarial/manipulative → spam
2. Both tags truly empty/meaningless → noise
3. Operation matching agent's domain → app_specific
4. Automated technical metric → config_feedback
5. Human quality judgment → service_feedback
6. Uncertain → others

Reply with ONLY valid JSON, nothing else:
{"category": "spam|noise|service_feedback|config_feedback|app_specific|others", "confidence": 0.00, "reason": "one sentence"}`

func sanitizePromptField(s string) string {
	// Keep prompt stable: strip newlines and excessive whitespace.
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

// BuildUserMessage builds the v1 user turn. Layout is one labelled field per
// line. Kept stable for the original (qwen2.5-7B+) prompt and for diagnostic
// tooling. Optional fields are omitted when empty:
//
//   tag1: …
//   tag2: …
//   value: 0.95
//   scale: pct100               (omitted if empty)
//   endpoint: api.example.com   (omitted if empty)
//   offchain: …                 (always shown; "(none)" placeholder)
//   agent_description: …
//   agent_services: …
//   agent_tags: …               (omitted if empty)
//
// endpoint and scale are surfaced when the bench / runtime caller supplies
// them. Older callers pass "" and the prompt stays identical to the previous
// schema for backward compatibility.
func BuildUserMessage(
	tag1, tag2 string,
	valueNorm float64,
	offchainContent, agentDescription, agentServices string,
	agentTags []string,
	endpoint, scale string,
) string {
	tag1 = sanitizePromptField(tag1)
	tag2 = sanitizePromptField(tag2)
	offchainContent = sanitizePromptField(offchainContent)
	agentDescription = sanitizePromptField(agentDescription)
	agentServices = sanitizePromptField(agentServices)
	endpoint = sanitizePromptField(endpoint)
	scale = sanitizePromptField(scale)

	if offchainContent == "" {
		offchainContent = "(none)"
	}
	if agentDescription == "" {
		agentDescription = "(not available)"
	}
	if agentServices == "" {
		agentServices = "(not available)"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "tag1: %s\ntag2: %s\nvalue: %.4f", tag1, tag2, valueNorm)
	if scale != "" {
		fmt.Fprintf(&b, "\nscale: %s", scale)
	}
	if endpoint != "" {
		fmt.Fprintf(&b, "\nendpoint: %s", EndpointHost(endpoint))
	}
	fmt.Fprintf(&b, "\noffchain: %s\nagent_description: %s\nagent_services: %s",
		offchainContent, agentDescription, agentServices)
	if len(agentTags) > 0 {
		fmt.Fprintf(&b, "\nagent_tags: %s", strings.Join(agentTags, ", "))
	}
	return b.String()
}

// EndpointHost strips scheme + path from an endpoint URL so the prompt sees
// only the host. For a free-text endpoint that's not a URL it returns the
// trimmed string unchanged. The host alone is the useful signal — full URLs
// burn tokens without adding classification value.
func EndpointHost(ep string) string {
	ep = strings.TrimSpace(ep)
	if ep == "" {
		return ""
	}
	// Strip scheme.
	if i := strings.Index(ep, "://"); i >= 0 {
		ep = ep[i+3:]
	}
	// Strip path / query / fragment after first separator.
	for _, sep := range []string{"/", "?", "#"} {
		if i := strings.Index(ep, sep); i >= 0 {
			ep = ep[:i]
		}
	}
	return ep
}

// Compact-3B field length caps. Tuned so a typical input lands well under
// ~240 tokens and a worst-case input still fits comfortably in a 3B model's
// short prompt budget.
//
// AgentDesc was bumped from 80 to 200 chars in Track B1 — the 80-char cap was
// dropping critical disambiguation context (e.g. agent 17 microtasking on
// Celo vs an unrelated DeFi vault), causing the 3B classifier to over-predict
// service_feedback / others on app_specific domain-op tags.
const (
	compactAgentDescMaxRunes = 200
	compactDomainsMaxItems   = 5
	compactServicesMaxRunes  = 60
	compactNoteMaxRunes      = 160
)

// BuildUserMessageCompact3B builds a natural-language compact user turn for
// the compact-3B prompt. Layout:
//
//	agent: <description summary>; domains: <leaves>[; tags: <onchain tags>][; endpoint: host]
//	t1=<humanized>; t2=<humanized>; score=<v>[; scale=<tier>]
//	note: <schema-aware offchain summary>
//
// Track B1 enrichment vs the original compact-3B build:
//   - agent_description budget raised 80 → 200 chars (compactAgentDescMaxRunes)
//   - endpoint host surfaced inline on the agent header when provided
//   - tag-scale tier (binary|star5|star10|pct100|unbounded) appended to the
//     scoreline so the LLM can interpret the value type (e.g. 1 means
//     "passed" under binary, "1 star" under star5)
//   - offchain note now uses SummarizeJSONLikeSchema, preserving JSON KEY
//     NAMES (validationType=…; slashingConditions=[3 items]) instead of
//     flattening to value-only text — schema names are strong category
//     signals (validation* → config, perDomain → config, comment → service).
//
// Mechanical identifiers in tag1/tag2 are humanized (snake/kebab/camel split,
// lowercased). agentTags are appended inline to the agent header when
// non-empty (capped at compactDomainsMaxItems).
func BuildUserMessageCompact3B(
	tag1, tag2 string,
	valueNorm float64,
	offchainContent, agentDescription, agentServices string,
	agentTags []string,
	endpoint, scale string,
) string {
	t1 := HumanizeIdentifier(tag1)
	if t1 == "" {
		t1 = "<EMPTY>"
	}
	t2 := HumanizeIdentifier(tag2)
	if t2 == "" {
		t2 = "<EMPTY>"
	}

	desc := SummarizeText(agentDescription, compactAgentDescMaxRunes)
	if desc == "" {
		desc = "unknown"
	}

	var ctx string
	if hints := ExtractDomainHints(agentServices, compactDomainsMaxItems); hints != "" {
		ctx = "; domains: " + hints
	} else if svc := SummarizeText(agentServices, compactServicesMaxRunes); svc != "" {
		ctx = "; services: " + svc
	}
	if len(agentTags) > 0 {
		tags := agentTags
		if len(tags) > compactDomainsMaxItems {
			tags = tags[:compactDomainsMaxItems]
		}
		ctx += "; tags: " + strings.Join(tags, ", ")
	}
	if host := EndpointHost(endpoint); host != "" {
		ctx += "; endpoint: " + host
	}

	note := SummarizeJSONLikeSchema(offchainContent, compactNoteMaxRunes)
	if note == "" {
		note = "(none)"
	}

	scoreLine := fmt.Sprintf("t1=%s; t2=%s; score=%.2f", t1, t2, valueNorm)
	if s := strings.TrimSpace(scale); s != "" {
		scoreLine += "; scale=" + s
	}

	return fmt.Sprintf("agent: %s%s\n%s\nnote: %s", desc, ctx, scoreLine, note)
}

// BuildPrompt is kept for diagnostics/tests. It composes the v1 system prompt
// with the v1 user message; the runtime path constructs separate system+user
// messages and respects the configured PromptVersion.
func BuildPrompt(tag1, tag2 string, valueNorm float64, offchainContent, agentDescription, agentServices string, agentTags []string, endpoint, scale string) string {
	return "[SYSTEM]\n" + llmSystemPrompt + "\n\n[USER]\n" +
		BuildUserMessage(tag1, tag2, valueNorm, offchainContent, agentDescription, agentServices, agentTags, endpoint, scale)
}

// BuildPromptCompact3B is the diagnostic counterpart to BuildPrompt for the
// compact-3B variant.
func BuildPromptCompact3B(tag1, tag2 string, valueNorm float64, offchainContent, agentDescription, agentServices string, agentTags []string, endpoint, scale string) string {
	return "[SYSTEM]\n" + llmSystemPromptCompact3B + "\n\n[USER]\n" +
		BuildUserMessageCompact3B(tag1, tag2, valueNorm, offchainContent, agentDescription, agentServices, agentTags, endpoint, scale)
}
