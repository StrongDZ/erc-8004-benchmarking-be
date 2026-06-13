package feedback

import "strings"

// EffectiveCategory returns the runtime category for a feedback record.
// Prefers top-level category, then LLM fallback, then rule classification.
func EffectiveCategory(fb FeedbackRecord) string {
	if c := strings.TrimSpace(fb.Category); c != "" {
		return c
	}
	if fb.Classification.Fallback != nil {
		if c := strings.TrimSpace(fb.Classification.Fallback.Category); c != "" {
			return c
		}
	}
	return fb.Classification.Rule.Category
}
