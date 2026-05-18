package trustgraph

// validator.go — Validate runs the gate over a feedback record.
//
// Gate order (first match wins):
//  1. isSelfFeedback          → VerdictSelf
//  2. missing required field  → VerdictMissingFields
//  3. rule == "junk"          → VerdictJunk
//  4. rule == "others", no fallback → ErrPendingLLM (requeue)
//  5. rule == "others", fallback.category == "junk" → VerdictJunk
//  6. otherwise               → VerdictValid

import (
	"errors"
	"fmt"

	"erc-8004-benchmarking-be/internal/repository/feedback"
)

const (
	VerdictValid         = "valid"
	VerdictJunk          = "junk"
	VerdictMissingFields = "missing_fields"
	VerdictSelf          = "self"
)

// ErrPendingLLM is returned when rule="others" and no LLM fallback exists yet.
// The caller should nack+requeue the message.
var ErrPendingLLM = errors.New("llm verdict pending")

// ValidationResult holds the gate outcome.
type ValidationResult struct {
	Code       string
	Reason     string
	Confidence float64
}

// IsGated reports whether the feedback must be excluded from the graph.
func (v ValidationResult) IsGated() bool { return v.Code != VerdictValid }

// Validate runs the gate. Returns ErrPendingLLM when LLM verdict is not yet available.
func Validate(fb feedback.FeedbackRecord) (ValidationResult, error) {
	if fb.IsSelfFeedback {
		return ValidationResult{Code: VerdictSelf, Confidence: 1.0,
			Reason: "clientAddress matches owner or agentWallet"}, nil
	}
	if missing := missingRequiredField(fb); missing != "" {
		return ValidationResult{
			Code:       VerdictMissingFields,
			Reason:     fmt.Sprintf("required field absent: %s", missing),
			Confidence: 1.0,
		}, nil
	}
	switch fb.Classification.Rule.Category {
	case "junk":
		return ValidationResult{Code: VerdictJunk, Confidence: 0.99,
			Reason: "rule classifier: junk"}, nil
	case "others":
		if fb.Classification.Fallback == nil {
			return ValidationResult{}, ErrPendingLLM
		}
		if fb.Classification.Fallback.Category == "junk" {
			return ValidationResult{
				Code:       VerdictJunk,
				Confidence: fb.Classification.Fallback.Confidence,
				Reason:     "llm fallback: junk",
			}, nil
		}
		return ValidationResult{
			Code:       VerdictValid,
			Confidence: fb.Classification.Fallback.Confidence,
		}, nil
	}
	return ValidationResult{Code: VerdictValid, Confidence: 0.99}, nil
}

func missingRequiredField(fb feedback.FeedbackRecord) string {
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
