package snapshot

// quality_signals.go — Extract quality signals from a feedback's feedbackParsed map.
// Used by Cụm C SA to populate the quality fields of FeedbackSnapshot.
//
// Data-shape note: the ERC-8004 spec models rich feedback (reasoning, attachments,
// ratingBreakdown, proofOfPayment), but the indexed chain-8453 data uses a simpler
// shape (comment, score, tag1, tag2, …). To keep Cụm C meaningful on real data we
// map the real fields onto the spec signals:
//   - reasoningLen      ← len(reasoning) if present, else len(comment)
//   - hasRatingBreakdown ← a non-empty ratingBreakdown map OR a present score field
//   - attachmentCount / hasProofOfPayment ← spec fields (absent in chain-8453 data,
//     kept for forward-compatibility with spec-compliant sources).

import (
	"strings"
	"unicode/utf8"
)

// ExtractQualitySignals returns (reasoningLen, attachmentCount, hasRatingBreakdown, hasProofOfPayment).
func ExtractQualitySignals(parsed map[string]any) (int, int, bool, bool) {
	if len(parsed) == 0 {
		return 0, 0, false, false
	}

	// reasoningLen: prefer the spec "reasoning", fall back to real-world "comment".
	reasoning := firstNonEmptyString(parsed, "reasoning", "comment")
	reasoningLen := utf8.RuneCountInString(reasoning)

	attachCount := countValidAttachments(parsed["attachments"])

	hasBreakdown := hasNonEmpty(parsed["ratingBreakdown"]) ||
		hasNonEmpty(parsed["rating_breakdown"]) ||
		hasNonEmpty(parsed["score"])

	hasPayment := hasNonEmpty(parsed["proofOfPayment"]) ||
		hasNonEmpty(parsed["proof_of_payment"])

	return reasoningLen, attachCount, hasBreakdown, hasPayment
}

// firstNonEmptyString returns the trimmed string value of the first key that holds
// a non-empty string.
func firstNonEmptyString(parsed map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := parsed[k].(string); ok {
			if t := strings.TrimSpace(s); t != "" {
				return t
			}
		}
	}
	return ""
}

// countValidAttachments counts items in an attachments array that carry the
// spec-required fields (name, uri, mimeType, size).
func countValidAttachments(v any) int {
	arr, ok := v.([]any)
	if !ok {
		return 0
	}
	n := 0
	for _, item := range arr {
		if isValidAttachmentMap(item) {
			n++
		}
	}
	return n
}

func isValidAttachmentMap(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	for _, k := range []string{"name", "uri", "mimeType", "size"} {
		val, exists := m[k]
		if !exists || !hasNonEmpty(val) {
			return false
		}
	}
	return true
}

// hasNonEmpty reports whether v carries a meaningful (present, non-empty) value.
func hasNonEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(t) != ""
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}
