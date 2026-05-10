package scoring

// adjustments.go — Documented wi adjustment layer applied on top of §3.2.
//
// These adjustments are intentional extensions to the spec formula and must be
// explicitly listed here so they can be audited, A/B tested, or disabled independently.
// Each function returns the adjusted wi and a human-readable reason string.

import (
	"encoding/json"
	"strings"
)

// WiAdjustment records a single modification applied to the base wi.
type WiAdjustment struct {
	Reason     string
	Multiplier float64 // applied first: wi *= Multiplier (1.0 = no-op)
	Addend     float64 // applied second: wi += Addend (0.0 = no-op)
}

// ApplyAdjustments folds a slice of adjustments into wi in declaration order.
func ApplyAdjustments(wi float64, adjustments []WiAdjustment) float64 {
	for _, a := range adjustments {
		if a.Multiplier != 0 {
			wi *= a.Multiplier
		}
		wi += a.Addend
	}
	return wi
}

// LowConfidenceAdjustment halves wi when classifier confidence is below the threshold.
// Rationale: low-confidence classification means the category signal is unreliable;
// weighting the feedback less reduces noise without discarding it entirely.
func LowConfidenceAdjustment(confidence float64) (WiAdjustment, bool) {
	if confidence >= 0.60 {
		return WiAdjustment{}, false
	}
	return WiAdjustment{Reason: "low_classifier_confidence", Multiplier: 0.5}, true
}

// ServiceFeedbackBonus adds up to +0.4 wi for fully-structured service feedbacks.
// Rationale: feedbacks that include a verifiable feedbackURI, rich content, and
// proof of payment carry higher epistemic value than bare tag submissions.
//   - +0.2 for non-empty feedback content (feedbackURI present and resolved)
//   - +0.2 for proof-of-payment or attachment evidence
func ServiceFeedbackBonus(feedbackURI, content string) WiAdjustment {
	bonus := 0.0
	if strings.TrimSpace(feedbackURI) == "" {
		return WiAdjustment{Reason: "service_feedback_bonus", Addend: 0}
	}
	if strings.TrimSpace(content) != "" {
		bonus += 0.2
	}
	if hasProofOrAttachment(content) {
		bonus += 0.2
	}
	return WiAdjustment{Reason: "service_feedback_bonus", Addend: bonus}
}

// hasProofOrAttachment returns true when the feedback JSON contains a non-empty
// proofOfPayment (camelCase, current spec) or proof_of_payment (snake_case, legacy),
// or a non-empty attachments array whose elements pass Attachment Pattern v1.1 validation.
func hasProofOrAttachment(content string) bool {
	if strings.TrimSpace(content) == "" {
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return false
	}

	for k, v := range payload {
		lk := strings.ToLower(strings.TrimSpace(k))
		switch lk {
		case "proofofpayment", "proof_of_payment":
			if hasNonEmptyValue(v) {
				return true
			}
		case "attachments":
			if hasValidAttachments(v) {
				return true
			}
		}
	}
	return false
}

// hasValidAttachments returns true when v is a non-empty array where at least one
// element satisfies Attachment Pattern Standard v1.1 (name, uri, mimeType, size required).
func hasValidAttachments(v any) bool {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return false
	}
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if isValidAttachment(m) {
			return true
		}
	}
	return false
}

// isValidAttachment checks that all four required Attachment Pattern v1.1 fields are present
// and non-empty: name, uri, mimeType, size.
func isValidAttachment(m map[string]any) bool {
	for _, field := range []string{"name", "uri", "mimeType", "size"} {
		v, ok := m[field]
		if !ok {
			return false
		}
		if !hasNonEmptyValue(v) {
			return false
		}
	}
	return true
}

func hasNonEmptyValue(v any) bool {
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
