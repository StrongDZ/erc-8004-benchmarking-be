package feedback

import "strings"

// IsGraded reports whether trust-graph-updater has finished grading a feedback row.
// Graded verdicts exclude empty, "legacy", and "pending" — those may still be (re)processed.
func IsGraded(verdict string) bool {
	v := strings.TrimSpace(verdict)
	if v == "" || v == "legacy" || v == "pending" {
		return false
	}
	return true
}
