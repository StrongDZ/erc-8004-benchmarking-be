package evm

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ParseTagsFromDecoded normalizes on-chain "tags" metadata into a deduped, lowercased slice.
// Accepts a JSON array string, []string, or []any (e.g. from BSON/JSON).
func ParseTagsFromDecoded(decoded any) []string {
	if decoded == nil {
		return nil
	}
	switch v := decoded.(type) {
	case string:
		return parseTagsFromJSONString(v)
	case []string:
		return normalizeTagStrings(v)
	case []any:
		strs := make([]string, 0, len(v))
		for _, x := range v {
			switch t := x.(type) {
			case string:
				strs = append(strs, t)
			case nil:
				continue
			default:
				strs = append(strs, fmt.Sprint(t))
			}
		}
		return normalizeTagStrings(strs)
	default:
		return nil
	}
}

func parseTagsFromJSONString(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	return normalizeTagStrings(arr)
}

func normalizeTagStrings(arr []string) []string {
	if len(arr) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(arr))
	out := make([]string, 0, len(arr))
	for _, t := range arr {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
