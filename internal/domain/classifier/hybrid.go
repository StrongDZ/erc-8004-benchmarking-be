package classifier

// hybrid.go — Hybrid classifier: rule-based engine (Stage 1–3) → LLM fallback (Stage 3B).
// Pipeline:
//   Stage 1: Spam / Noise gate  (pure rule, O(1))
//   Stage 2: Rule classifier    (lookup maps + regex, ~90% coverage)
//   Stage 3: LLM classifier     (self-hosted, only for fallback combos)

import (
	"context"
	"fmt"
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
}

// HybridResult extends the base Result with LLM-specific metadata.
type HybridResult struct {
	Result                        // embedded rule-based result fields
	LowConfidence bool            // true when 0.50 <= confidence < 0.70
	ModelVer      string          // set when Source == "llm"
	ValueNorm     float64         // normalised value used during classification
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
	)

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
