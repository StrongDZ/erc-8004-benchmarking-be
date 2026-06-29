package feedback

import "strings"

// IsGraded reports whether feedback grading (by trustrank inline or feedback-others worker) has finished for a row.
// Graded verdicts exclude empty, "legacy", and "pending" — those may still be (re)processed.
func IsGraded(verdict string) bool {
	v := strings.TrimSpace(verdict)
	if v == "" || v == "legacy" || v == "pending" {
		return false
	}
	return true
}
