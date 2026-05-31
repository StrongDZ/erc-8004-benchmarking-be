package snapshot

// filter.go — Apply FilterConfig to a single FeedbackRecord.

import (
	"erc-8004-benchmarking-be/internal/repository/feedback"
)

// Keep returns true when the feedback passes all configured filters.
// Used by the builder during snapshot construction.
func Keep(cfg FilterConfig, fb feedback.FeedbackRecord) bool {
	cat := fb.Classification.Rule.Category

	if cfg.IncludeSpam {
		// When spam included, accept any category as long as other filters pass.
	} else {
		if cfg.Category != "" && cat != cfg.Category {
			return false
		}
	}

	if !cfg.IncludeRevoked && fb.IsRevoked {
		return false
	}
	if !cfg.IncludeSelf && fb.IsSelfFeedback {
		return false
	}
	return true
}

// DefaultFilter returns the SA-default filter (service_feedback only, non-revoked, non-self).
func DefaultFilter() FilterConfig {
	return FilterConfig{
		Category:       "service_feedback",
		IncludeRevoked: false,
		IncludeSelf:    false,
		MinFeedbacks:   1,
		IncludeSpam:    false,
	}
}
