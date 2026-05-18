package trustgraph

// extract_quality.go — Extracts propagation.QualityInput from FeedbackParsed.
// All extractions are best-effort; missing/wrong-typed fields default to zero.

import (
	"erc-8004-benchmarking-be/internal/domain/propagation"
	"erc-8004-benchmarking-be/internal/repository/feedback"
)

func extractQualityInput(fb feedback.FeedbackRecord, classifierConfidence float64) propagation.QualityInput {
	p := fb.FeedbackParsed
	return propagation.QualityInput{
		ReasoningLen:         parsedStringLen(p, "reasoning"),
		AttachmentCount:      parsedArrayLen(p, "attachments"),
		HasRatingBreakdown:   parsedHasKey(p, "rating_breakdown"),
		HasProofOfPayment:    parsedHasKey(p, "proofOfPayment"),
		ClassifierConfidence: classifierConfidence,
	}
}

func parsedStringLen(p map[string]any, key string) int {
	if p == nil {
		return 0
	}
	v, ok := p[key]
	if !ok {
		return 0
	}
	s, ok := v.(string)
	if !ok {
		return 0
	}
	return len(s)
}

func parsedArrayLen(p map[string]any, key string) int {
	if p == nil {
		return 0
	}
	v, ok := p[key]
	if !ok {
		return 0
	}
	arr, ok := v.([]any)
	if !ok {
		return 0
	}
	return len(arr)
}

func parsedHasKey(p map[string]any, key string) bool {
	if p == nil {
		return false
	}
	v, ok := p[key]
	return ok && v != nil
}
