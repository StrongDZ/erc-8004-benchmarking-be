package scoring

// feedback_grade.go — Pure grading routine shared by ingest and async worker.
// Ports the verdict gate from trustgraph/validator.go and the quality-input
// extraction from trustgraph/extract_quality.go into a single side-effect-free
// function. Never called with category="others" — that path is intentionally omitted.

import (
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// GradeResult holds the full grading outcome for one feedback record.
type GradeResult struct {
	Verdict      string  // "valid" | "junk" | "missing_fields" | "self"
	Reason       string
	Confidence   float64
	Gated        bool    // true when Verdict != "valid"
	QualityScore float64 // Q ∈ [0,1]; 0 when gated
	Wi           float64 // feedback weight; 0 when gated
}

// GradeFeedback grades a feedback whose category is already resolved (never "others").
// Pure: no I/O. Mirrors the old trustgraph Validate + weight, minus the ErrPendingLLM path.
func GradeFeedback(fb feedbackrepo.FeedbackRecord, cfg QualityWeightConfig) GradeResult {
	if fb.IsSelfFeedback {
		return GradeResult{
			Verdict:    "self",
			Reason:     "clientAddress matches owner or agentWallet",
			Confidence: 1.0,
			Gated:      true,
		}
	}
	if m := missingRequiredField(fb); m != "" {
		return GradeResult{
			Verdict:    "missing_fields",
			Reason:     "required field absent: " + m,
			Confidence: 1.0,
			Gated:      true,
		}
	}
	if feedbackrepo.EffectiveCategory(fb) == "junk" {
		return GradeResult{
			Verdict:    "junk",
			Reason:     "classifier: junk",
			Confidence: 0.99,
			Gated:      true,
		}
	}
	q := ComputeFeedbackQuality(cfg, feedbackQualityInput(fb))
	return GradeResult{
		Verdict:      "valid",
		Confidence:   0.99,
		QualityScore: q,
		Wi:           ComputeFeedbackQualityWeight(cfg, q),
	}
}

// missingRequiredField returns the name of the first absent required field, or "".
func missingRequiredField(fb feedbackrepo.FeedbackRecord) string {
	switch {
	case fb.Value == "":
		return "value"
	case fb.AgentID == "":
		return "agentId"
	case fb.ClientAddress == "":
		return "clientAddress"
	case fb.Timestamp == 0:
		return "timestamp"
	}
	return ""
}

// feedbackQualityInput extracts scoring signals from FeedbackParsed.
// Rule-decided feedback uses confidence 0.99; LLM fallback uses its own confidence.
func feedbackQualityInput(fb feedbackrepo.FeedbackRecord) FeedbackQualityInput {
	confidence := 0.99
	if fb.Classification.Fallback != nil {
		confidence = fb.Classification.Fallback.Confidence
	}
	p := fb.FeedbackParsed
	return FeedbackQualityInput{
		ReasoningLen:         parsedStringLen(p, "reasoning"),
		AttachmentCount:      parsedArrayLen(p, "attachments"),
		HasRatingBreakdown:   parsedHasKey(p, "rating_breakdown"),
		HasProofOfPayment:    parsedHasKey(p, "proofOfPayment"),
		ClassifierConfidence: confidence,
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
