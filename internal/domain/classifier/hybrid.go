package classifier

// hybrid.go — Hybrid classifier: rule-based engine (Stage 1–3) → LLM fallback (Stage 3B).
// Pipeline:
//   Stage 1: Spam / Noise gate  (pure rule, O(1))
//   Stage 2: Rule classifier    (lookup maps + regex, ~90% coverage)
//   Stage 3: LLM classifier     (self-hosted, only for fallback combos)

import (
	"context"
	"fmt"
	"strings"
)

// HybridInput is the full set of signals available at classification time.
type HybridInput struct {
	Tag1             string
	Tag2             string
	ValueRaw         string // raw on-chain uint256 string
	ValueDecimals    int
	OffchainContent  string
	AgentDescription string
	AgentServices    string
	AgentTags        []string // denormalized onchain tags, e.g. ["defi","swap"]
}

// HybridResult extends the base Result with LLM-specific metadata.
type HybridResult struct {
	Result                        // embedded rule-based result fields
	LowConfidence bool            // true when 0.50 <= confidence < 0.70
	ModelVer      string          // set when Source == "llm"
	ValueNorm     float64         // normalised value used during classification
}

// knownConfigTag2 maps tag2 values that always signal config_feedback regardless
// of what the LLM decides. Used as a post-hoc override for LLM fallback results.
var knownConfigTag2 = map[string]bool{
	"oracle-screening": true, "liveness-check": true,
	"win-rate": true, "coverage-rate": true, "exit-rate": true,
	"automated-screening": true, "service_quality": true,
}

// knownAppTag2 maps tag2 values that always signal app_specific.
var knownAppTag2 = map[string]bool{
	"sentinelnet-v1": true,
}

// HybridClassifier combines the rule engine with an optional LLM fallback.
// LLMClient may be nil — in that case all fallback cases resolve to "others".
type HybridClassifier struct {
	llm                    *LLMClient
	confidenceThresholdLow float64 // low confidence floor (default 0.50)
}

// NewHybridClassifier constructs a HybridClassifier.
// llm may be nil to run rule-only mode (LLM disabled).
func NewHybridClassifier(llm *LLMClient) *HybridClassifier {
	return &HybridClassifier{
		llm:                    llm,
		confidenceThresholdLow: 0.50,
	}
}

// Classify runs the full pipeline on the given input.
// It is safe to call concurrently.
func (h *HybridClassifier) Classify(ctx context.Context, in HybridInput) (HybridResult, error) {
	// Normalize value once so both stages use the same number.
	valueNorm := NormalizeValue(in.ValueRaw, in.ValueDecimals)

	// Stage 1 + 2: rule-based classifier.
	ruleResult := Classify(in.Tag1, in.Tag2)

	// Rule matched → propagate immediately.
	if ruleResult.Source == "rule" {
		return HybridResult{
			Result:    ruleResult,
			ValueNorm: valueNorm,
		}, nil
	}

	// Stage 3B: LLM fallback.
	if h.llm == nil {
		// LLM disabled — stay with rule fallback result (category="others").
		return HybridResult{
			Result:    ruleResult,
			ValueNorm: valueNorm,
		}, nil
	}

	llmRes := h.llm.Classify(
		ctx,
		in.Tag1, in.Tag2,
		valueNorm,
		in.OffchainContent,
		in.AgentDescription,
		in.AgentServices,
		in.AgentTags,
	)

	// Post-hoc safety overrides: catch cases the LLM consistently miscategorises.
	// Applied only when LLM outputs service_feedback — the category with the
	// highest false-positive rate on 3B models.
	t1lo := strings.ToLower(strings.TrimSpace(in.Tag1))
	t2lo := strings.ToLower(strings.TrimSpace(in.Tag2))
	if llmRes.Category == CategoryService {
		switch {
		case isSpam(t1lo, t2lo):
			llmRes.Category = CategorySpam
			llmRes.Confidence = 0.99
			llmRes.Source = "override"
		case isNoise(t1lo, t2lo):
			llmRes.Category = CategoryNoise
			llmRes.Confidence = 0.99
			llmRes.Source = "override"
		case knownConfigTag2[t2lo]:
			llmRes.Category = CategoryConfig
			llmRes.Confidence = 0.95
			llmRes.Source = "override"
		case knownAppTag2[t2lo]:
			llmRes.Category = CategoryApp
			llmRes.Confidence = 0.95
			llmRes.Source = "override"
		}
	}

	result := HybridResult{
		Result: Result{
			Category:      llmRes.Category,
			Confidence:    llmRes.Confidence,
			Source:        llmRes.Source,
			NormalizedTag: in.Tag1,
		},
		LowConfidence: llmRes.LowConfidence,
		ModelVer:      llmRes.ModelVer,
		ValueNorm:     valueNorm,
	}

	return result, nil
}

// String returns a human-readable summary of the hybrid result (for logging).
func (r HybridResult) String() string {
	return fmt.Sprintf("category=%s confidence=%.2f source=%s low_conf=%v",
		r.Category, r.Confidence, r.Source, r.LowConfidence)
}
