package classifier

// humanize.go — Pure helpers that turn machine-style identifiers and
// JSON-like blobs into compact natural-language phrases. These are used
// exclusively by the compact-3B prompt builder; the v1 prompt builder
// continues to feed raw fields verbatim.

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	humanizeSnakeRe   = regexp.MustCompile(`_+`)
	humanizeKebabRe   = regexp.MustCompile(`-+`)
	humanizeCamelRe   = regexp.MustCompile(`([a-z0-9])([A-Z])`)
	humanizeRunOfWS   = regexp.MustCompile(`\s+`)
	humanizeOASFPathRe = regexp.MustCompile(`[a-z][a-z0-9_]*(?:/[a-z0-9_][a-z0-9_]*)+`)
)

// HumanizeIdentifier converts a mechanical identifier into a lowercase,
// space-separated phrase. Idempotent on already-spaced strings; preserves
// non-ASCII runes verbatim.
//
//	"fx-trade"               → "fx trade"
//	"winRate"                → "win rate"
//	"m1-mainnet-reputation"  → "m1 mainnet reputation"
//	"swap_token"             → "swap token"
//	"Ichi USD₮-WBTC vault"   → "ichi usd₮ wbtc vault"
//	"CADm"                   → "cadm"
//	"🔥"                     → "🔥"
//	""                       → ""
func HumanizeIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.ContainsAny(s, " \t") {
		s = humanizeRunOfWS.ReplaceAllString(s, " ")
		s = humanizeKebabRe.ReplaceAllString(s, " ")
		return strings.ToLower(strings.TrimSpace(s))
	}
	s = humanizeCamelRe.ReplaceAllString(s, "${1} ${2}")
	s = humanizeSnakeRe.ReplaceAllString(s, " ")
	s = humanizeKebabRe.ReplaceAllString(s, " ")
	s = humanizeRunOfWS.ReplaceAllString(s, " ")
	return strings.ToLower(strings.TrimSpace(s))
}

// truncateRunes returns the first n runes of s, appending an ellipsis when
// truncation occurred. Returns s unchanged when shorter than n.
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// SummarizeText collapses whitespace and truncates s to maxRunes. Newlines and
// carriage returns become spaces; runs of whitespace collapse to one space.
func SummarizeText(s string, maxRunes int) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = humanizeRunOfWS.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	return truncateRunes(s, maxRunes)
}

// SummarizeJSONLike attempts to parse s as JSON. On success it surfaces the
// first non-empty preferred field (comment, feedback, description, name,
// status, message); else it serializes a few short string keys; else it falls
// back to SummarizeText.
func SummarizeJSONLike(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if !(strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")) {
		return SummarizeText(s, maxRunes)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(s), &obj); err == nil {
		for _, k := range []string{"comment", "feedback", "description", "name", "status", "message"} {
			if raw, ok := obj[k]; ok {
				if str, ok := raw.(string); ok && strings.TrimSpace(str) != "" {
					return SummarizeText(str, maxRunes)
				}
			}
		}
		var parts []string
		for k, v := range obj {
			sv, ok := v.(string)
			if !ok || strings.TrimSpace(sv) == "" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%s", k, sv))
			if len(parts) >= 3 {
				break
			}
		}
		if len(parts) > 0 {
			return SummarizeText(strings.Join(parts, "; "), maxRunes)
		}
	}
	return SummarizeText(s, maxRunes)
}

// ExtractDomainHints surfaces OASF-style domain leaves from a free-text
// agentServices payload. It first looks for slash-paths
// (e.g. "technology/blockchain/defi"), de-duplicates leaves, and joins with
// commas — capped to maxItems entries. When no slash-path is found, it returns
// "" so callers can fall back to a raw services snippet.
func ExtractDomainHints(agentServices string, maxItems int) string {
	if agentServices == "" || maxItems <= 0 {
		return ""
	}
	matches := humanizeOASFPathRe.FindAllString(strings.ToLower(agentServices), -1)
	if len(matches) == 0 {
		return ""
	}
	seen := map[string]bool{}
	out := make([]string, 0, maxItems)
	for _, m := range matches {
		leaf := m
		if i := strings.LastIndex(m, "/"); i >= 0 {
			leaf = m[i+1:]
		}
		if seen[leaf] {
			continue
		}
		seen[leaf] = true
		out = append(out, leaf)
		if len(out) >= maxItems {
			break
		}
	}
	return strings.Join(out, ", ")
}
