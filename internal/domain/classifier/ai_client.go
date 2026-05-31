package classifier

// ai_client.go — Thin HTTP client to the Python AI service (erc-8004-ai-service).
// Replaces the previous in-process Ollama/llama.cpp client. Prompt building,
// model dispatch and output parsing all live in the Python service now.

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

// LLMResult is the structured output returned by the AI service.
type LLMResult struct {
	Category      Category `json:"category"`
	Confidence    float64  `json:"confidence"`
	Reason        string   `json:"reason"`
	Source        string   `json:"source"` // "llm" | "fallback"
	LowConfidence bool     `json:"-"`      // 0 < confidence < 0.80 — kept category, flagged for review
	ModelVer      string   `json:"model_ver"`
}

// AgentServicePayload mirrors the Python ClassifyRequest.agent_services entry.
// Kept here (not imported from repository/agent) so the classifier package stays
// independent of repository wiring. Callers map their own service struct → this.
type AgentServicePayload struct {
	Name     string   `json:"name"`
	Endpoint string   `json:"endpoint"`
	Version  string   `json:"version,omitempty"`
	Skills   []string `json:"skills,omitempty"`
	Domains  []string `json:"domains,omitempty"`
}

// AIClientConfig configures the HTTP client.
type AIClientConfig struct {
	BaseURL        string // e.g. http://localhost:8000
	Model          string // optional override; empty → service default
	PromptVersion  string // "v4_xml" by default in the service
	TimeoutSeconds int    // defaults to 120
}

// AIClient talks to the Python AI service over HTTP. Safe for concurrent use.
type AIClient struct {
	baseURL       string
	model         string
	promptVersion string
	timeout       time.Duration
	httpClient    *http.Client
}

// NewAIClient builds a client from config.
func NewAIClient(cfg AIClientConfig) *AIClient {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	return &AIClient{
		baseURL:       strings.TrimRight(cfg.BaseURL, "/"),
		model:         cfg.Model,
		promptVersion: cfg.PromptVersion,
		timeout:       timeout,
		httpClient:    &http.Client{Timeout: timeout + 2*time.Second},
	}
}

// classifyRequest mirrors app/schemas.py:ClassifyRequest on the Python side.
// agent_services is structured (not a string) — the Python context builder
// filters generic services (web/OASF/A2A/email) and matches the feedback
// endpoint against agent service endpoints, so it needs name + endpoint per service.
type classifyRequest struct {
	Tag1             string                `json:"tag1"`
	Tag2             string                `json:"tag2"`
	ValueNorm        float64               `json:"value_norm"`
	Scale            string                `json:"scale,omitempty"`
	OffchainContent  string                `json:"offchain_content,omitempty"`
	Endpoint         string                `json:"endpoint,omitempty"`
	AgentDescription string                `json:"agent_description,omitempty"`
	AgentServices    []AgentServicePayload `json:"agent_services,omitempty"`
	AgentOASFDomains []string              `json:"agent_oasf_domains,omitempty"`
	AgentOASFSkills  []string              `json:"agent_oasf_skills,omitempty"`
	AgentTags        []string              `json:"agent_tags,omitempty"`
	PromptVersion    string                `json:"prompt_version,omitempty"`
	Model            string                `json:"model,omitempty"`
}

// Classify sends one feedback record to the AI service.
// On any transport/decode error the result downgrades to CategoryOthers / source=fallback,
// matching the previous LLMClient behaviour so hybrid.go can stay unchanged.
func (c *AIClient) Classify(
	ctx context.Context,
	tag1, tag2 string,
	valueNorm float64,
	offchainContent string,
	agentDescription string,
	agentServices []AgentServicePayload,
	agentOASFDomains []string,
	agentOASFSkills []string,
	agentTags []string,
	endpoint string,
	scale string,
) LLMResult {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	payload := classifyRequest{
		Tag1:             tag1,
		Tag2:             tag2,
		ValueNorm:        valueNorm,
		Scale:            scale,
		OffchainContent:  offchainContent,
		Endpoint:         endpoint,
		AgentDescription: agentDescription,
		AgentServices:    agentServices,
		AgentOASFDomains: agentOASFDomains,
		AgentOASFSkills:  agentOASFSkills,
		AgentTags:        agentTags,
		PromptVersion:    c.promptVersion,
		Model:            c.model,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fallbackResult(fmt.Errorf("marshal: %w", err))
	}

	req, err := http.NewRequestWithContext(reqCtx, "POST", c.baseURL+"/classify", bytes.NewReader(body))
	if err != nil {
		return fallbackResult(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fallbackResult(fmt.Errorf("ai service: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fallbackResult(fmt.Errorf("ai service: status %d body=%s", resp.StatusCode, strings.TrimSpace(string(b))))
	}

	var out LLMResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fallbackResult(fmt.Errorf("ai service: decode: %w", err))
	}

	if !validLLMCategory(out.Category) {
		return fallbackResult(fmt.Errorf("ai service: unknown category %q", out.Category))
	}
	if out.Confidence < 0 {
		out.Confidence = 0
	}
	if out.Confidence > 1 {
		out.Confidence = 1
	}
	// Spec: "chỉ chọn khi conf > 0.80" — anything below 0.80 keeps its category
	// (we do not force `others`) but is flagged so the operator can review it.
	out.LowConfidence = out.Confidence > 0 && out.Confidence < 0.80
	if out.Source == "" {
		out.Source = "llm"
	}
	return out
}

// HealthCheck verifies the AI service is reachable.
func (c *AIClient) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ai service health: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ai service health: status %d", resp.StatusCode)
	}
	return nil
}

func fallbackResult(err error) LLMResult {
	return LLMResult{
		Category:   CategoryOthers,
		Confidence: 0.0,
		Source:     "fallback",
		Reason:     fmt.Sprintf("ai_error: %v", err),
	}
}

func validLLMCategory(c Category) bool {
	switch c {
	case CategoryService, CategoryConfig, CategoryApp, CategoryJunk, CategoryOthers:
		return true
	}
	return false
}
