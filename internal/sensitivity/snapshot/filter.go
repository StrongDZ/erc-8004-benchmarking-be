package snapshot

// filter.go — Apply FilterConfig to a single FeedbackRecord.

import (
	"erc-8004-benchmarking-be/internal/repository/feedback"
)

// Keep returns true when the feedback passes all configured filters.
// Used by the builder during snapshot construction.
//
// Filters on the EFFECTIVE top-level category (what actually scored — the LLM
// fallback may overwrite the rule verdict), falling back to the rule category
// for rows persisted before the top-level field existed.
func Keep(cfg FilterConfig, fb feedback.FeedbackRecord) bool {
	cat := fb.Category
	if cat == "" {
		cat = fb.Classification.Rule.Category
	}

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

// DefaultFilter returns the SA-default filter (quality only, non-revoked, non-self).
func DefaultFilter() FilterConfig {
	return FilterConfig{
		Category:       "quality",
		IncludeRevoked: false,
		IncludeSelf:    false,
		MinFeedbacks:   1,
		IncludeSpam:    false,
	}
}
