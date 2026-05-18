package classifier

// llm_backends.go — HTTP transport for Ollama and llama.cpp backends.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

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
