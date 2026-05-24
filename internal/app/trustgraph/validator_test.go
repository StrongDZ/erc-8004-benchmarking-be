package trustgraph

import (
	"errors"
	"testing"

	"erc-8004-benchmarking-be/internal/repository/feedback"
)

func makeFeedback(overrides func(*feedback.FeedbackRecord)) feedback.FeedbackRecord {
	fb := feedback.FeedbackRecord{
		AgentID:       "123",
		ClientAddress: "0xabc",
		Value:         "1",
		Timestamp:     1716000000,
		Classification: feedback.FeedbackClassification{
			Rule: feedback.RuleClassification{Category: "service_feedback"},
		},
	}
	if overrides != nil {
		overrides(&fb)
	}
	return fb
}

func TestValidate_ValidServiceFeedback_ReturnsValid(t *testing.T) {
	fb := makeFeedback(nil)
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictValid {
		t.Errorf("code=%v want=%v", v.Code, VerdictValid)
	}
}

func TestValidate_SelfFeedback_ReturnsSelf(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) { f.IsSelfFeedback = true })
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictSelf {
		t.Errorf("code=%v want=%v", v.Code, VerdictSelf)
	}
}

func TestValidate_MissingValue_ReturnsMissingFields(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) { f.Value = "" })
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictMissingFields {
		t.Errorf("code=%v want=%v", v.Code, VerdictMissingFields)
	}
	if v.Reason == "" {
		t.Error("Reason should name the missing field")
	}
}

func TestValidate_MissingAgentID_ReturnsMissingFields(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) { f.AgentID = "" })
	v, _ := Validate(fb)
	if v.Code != VerdictMissingFields {
		t.Errorf("code=%v want=%v", v.Code, VerdictMissingFields)
	}
}

func TestValidate_MissingClientAddress_ReturnsMissingFields(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) { f.ClientAddress = "" })
	v, _ := Validate(fb)
	if v.Code != VerdictMissingFields {
		t.Errorf("code=%v want=%v", v.Code, VerdictMissingFields)
	}
}

func TestValidate_MissingTimestamp_ReturnsMissingFields(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) { f.Timestamp = 0 })
	v, _ := Validate(fb)
	if v.Code != VerdictMissingFields {
		t.Errorf("code=%v want=%v", v.Code, VerdictMissingFields)
	}
}

func TestValidate_JunkCategory_ReturnsJunk(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) {
		f.Classification.Rule.Category = "junk"
	})
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictJunk {
		t.Errorf("code=%v want=%v", v.Code, VerdictJunk)
	}
}

func TestValidate_OthersNoFallback_ReturnsErrPending(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) {
		f.Classification.Rule.Category = "others"
		f.Classification.Fallback = nil
	})
	_, err := Validate(fb)
	if !errors.Is(err, ErrPendingLLM) {
		t.Errorf("err=%v want ErrPendingLLM", err)
	}
}

func TestValidate_OthersFallbackJunk_ReturnsJunk(t *testing.T) {
	conf := 0.88
	fb := makeFeedback(func(f *feedback.FeedbackRecord) {
		f.Classification.Rule.Category = "others"
		f.Classification.Fallback = &feedback.FallbackClassification{
			Category:   "junk",
			Confidence: conf,
		}
	})
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictJunk {
		t.Errorf("code=%v want=%v", v.Code, VerdictJunk)
	}
	if v.Confidence != conf {
		t.Errorf("confidence=%v want=%v", v.Confidence, conf)
	}
}

func TestValidate_OthersFallbackValid_ReturnsValid(t *testing.T) {
	fb := makeFeedback(func(f *feedback.FeedbackRecord) {
		f.Classification.Rule.Category = "others"
		f.Classification.Fallback = &feedback.FallbackClassification{
			Category:   "service_feedback",
			Confidence: 0.82,
		}
	})
	v, err := Validate(fb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Code != VerdictValid {
		t.Errorf("code=%v want=%v", v.Code, VerdictValid)
	}
}
