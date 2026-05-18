package trustgraph

import (
	"testing"

	"erc-8004-benchmarking-be/internal/domain/propagation"
	"erc-8004-benchmarking-be/internal/repository/feedback"
)

func TestExtractQualityInput_EmptyParsed_AllZero(t *testing.T) {
	fb := feedback.FeedbackRecord{}
	got := extractQualityInput(fb, 0.99)
	want := propagation.QualityInput{ClassifierConfidence: 0.99}
	if got != want {
		t.Errorf("got=%+v want=%+v", got, want)
	}
}

func TestExtractQualityInput_ReasoningLen(t *testing.T) {
	text := "this agent was fast and accurate"
	fb := feedback.FeedbackRecord{
		FeedbackParsed: map[string]any{"reasoning": text},
	}
	got := extractQualityInput(fb, 0.5)
	if got.ReasoningLen != len(text) {
		t.Errorf("ReasoningLen=%d want=%d", got.ReasoningLen, len(text))
	}
}

func TestExtractQualityInput_AttachmentCount(t *testing.T) {
	fb := feedback.FeedbackRecord{
		FeedbackParsed: map[string]any{
			"attachments": []any{"a.pdf", "b.png"},
		},
	}
	got := extractQualityInput(fb, 0.9)
	if got.AttachmentCount != 2 {
		t.Errorf("AttachmentCount=%d want=2", got.AttachmentCount)
	}
}

func TestExtractQualityInput_HasRatingBreakdown(t *testing.T) {
	fb := feedback.FeedbackRecord{
		FeedbackParsed: map[string]any{
			"rating_breakdown": map[string]any{"accuracy": 90},
		},
	}
	got := extractQualityInput(fb, 0.9)
	if !got.HasRatingBreakdown {
		t.Error("expected HasRatingBreakdown=true")
	}
}

func TestExtractQualityInput_HasProofOfPayment(t *testing.T) {
	fb := feedback.FeedbackRecord{
		FeedbackParsed: map[string]any{
			"proofOfPayment": map[string]any{"txHash": "0xabc"},
		},
	}
	got := extractQualityInput(fb, 0.9)
	if !got.HasProofOfPayment {
		t.Error("expected HasProofOfPayment=true")
	}
}

func TestExtractQualityInput_ConfidencePassedThrough(t *testing.T) {
	fb := feedback.FeedbackRecord{}
	got := extractQualityInput(fb, 0.77)
	if got.ClassifierConfidence != 0.77 {
		t.Errorf("ClassifierConfidence=%v want=0.77", got.ClassifierConfidence)
	}
}
