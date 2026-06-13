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
	AgentServices    []AgentServicePayload // structured services so Python can filter generic + match endpoints
	AgentOASFDomains []string              // normalised OASF domain paths
	AgentOASFSkills  []string              // normalised OASF skill paths
	AgentTags        []string              // denormalised onchain tags, e.g. ["defi","swap"]
	Endpoint         string                // ERC-8004 v2.0 NewFeedback.endpoint — service URL
}

// HybridResult extends the base Result with LLM-specific metadata.
type HybridResult struct {
	Result                        // embedded rule-based result fields
	LowConfidence bool            // true when 0 < confidence < 0.80 — kept category, flagged for review
	ModelVer      string          // set when Source == "llm"
	ValueNorm     float64         // normalised value used during classification
}

// HybridClassifier combines the rule engine with an optional AI service fallback.
// ai may be nil — in that case all fallback cases resolve to "others".
type HybridClassifier struct {
	ai                     *AIClient
	confidenceThresholdLow float64 // low-confidence ceiling: results with conf < this are flagged (default 0.80)
}

// NewHybridClassifier constructs a HybridClassifier.
// ai may be nil to run rule-only mode (AI fallback disabled).
func NewHybridClassifier(ai *AIClient) *HybridClassifier {
	return &HybridClassifier{
		ai:                     ai,
		confidenceThresholdLow: 0.80,
	}
}

// Classify runs the full pipeline on the given input.
// It is safe to call concurrently.
func (h *HybridClassifier) Classify(ctx context.Context, in HybridInput) (HybridResult, error) {
	// Normalize value once so both stages use the same number.
	valueNorm := NormalizeValue(in.ValueRaw, in.ValueDecimals)

	// Compute tag-scale tier so the LLM prompt can interpret value meaning
	// (binary "passed"=1 vs star5 "1 star"=1). Empty when value is missing.
	scale := ""
	if real, ok := RawValueToReal(in.ValueRaw, in.ValueDecimals); ok {
		scale = AssignTier(real)
	}

	// Stage 1 + 2: rule-based classifier.
	ruleResult := Classify(in.Tag1, in.Tag2, scale)

	// Rule matched → propagate immediately.
	if ruleResult.Source == "rule" {
		return HybridResult{
			Result:    ruleResult,
			ValueNorm: valueNorm,
		}, nil
	}

	// Stage 3B: AI service fallback.
	if h.ai == nil {
		// AI disabled — stay with rule fallback result (category="others").
		return HybridResult{
			Result:    ruleResult,
			ValueNorm: valueNorm,
		}, nil
	}

	llmRes := h.ai.Classify(
		ctx,
		in.Tag1, in.Tag2,
		valueNorm,
		in.OffchainContent,
		in.AgentDescription,
		in.AgentServices,
		in.AgentOASFDomains,
		in.AgentOASFSkills,
		in.AgentTags,
		in.Endpoint,
		scale,
	)

	// Post-hoc safety overrides: catch cases the LLM consistently miscategorises.
	t1lo := strings.ToLower(strings.TrimSpace(in.Tag1))
	t2lo := strings.ToLower(strings.TrimSpace(in.Tag2))
	switch {
	case isSpam(t1lo, t2lo), isNoise(t1lo, t2lo):
		llmRes.Category = CategoryJunk
		llmRes.Feature = FeatureNone
		llmRes.Confidence = 0.99
		llmRes.Source = "override"
	case llmRes.Category == CategoryQuality && (isMetric(t1lo) || isMetric(t2lo)):
		// LLM said quality but a tag clearly names a measured metric → quantity.
		llmRes.Category = CategoryQuantity
		llmRes.Feature = featureOf(t1lo, t2lo)
		llmRes.Confidence = 0.95
		llmRes.Source = "override"
	}

	result := HybridResult{
		Result: Result{
			Category:      llmRes.Category,
			Feature:       llmRes.Feature,
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
