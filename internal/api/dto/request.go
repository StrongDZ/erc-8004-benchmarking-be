package dto

// request.go — Query-string parsing helpers for handlers.
//
// Every handler uses these helpers so we enforce the §1.3 conventions in one place:
//   - page 1-indexed (default 1)
//   - limit default 50, max 100 (overrideable via maxLimit arg)
//   - chainId required where applicable
//   - from/to parsed as RFC3339 UTC timestamps

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseInt parses an integer query parameter, returning `def` if absent or invalid.
func ParseInt(r *http.Request, name string, def int) int {
	v := strings.TrimSpace(r.URL.Query().Get(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ParseInt64 parses an int64 query parameter, returning `def` if absent or invalid.
func ParseInt64(r *http.Request, name string, def int64) int64 {
	v := strings.TrimSpace(r.URL.Query().Get(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// ParseFloat parses a float query parameter, returning `def` if absent or invalid.
func ParseFloat(r *http.Request, name string, def float64) float64 {
	v := strings.TrimSpace(r.URL.Query().Get(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

// ParseBoolPtr returns a *bool when the param is present ("true"/"false"/"1"/"0"),
// nil otherwise (so callers can distinguish "unset" from "false").
func ParseBoolPtr(r *http.Request, name string) *bool {
	v := strings.ToLower(strings.TrimSpace(r.URL.Query().Get(name)))
	if v == "" {
		return nil
	}
	switch v {
	case "true", "1", "yes":
		t := true
		return &t
	case "false", "0", "no":
		f := false
		return &f
	}
	return nil
}

// ParseInt64Slice parses a repeatable or comma-separated int64 query param into a deduped slice.
// Invalid tokens are skipped silently.
func ParseInt64Slice(r *http.Request, name string) []int64 {
	values := r.URL.Query()[name]
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]bool, len(values))
	out := make([]int64, 0, len(values))
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			n, err := strconv.ParseInt(part, 10, 64)
			if err != nil || seen[n] {
				continue
			}
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// ParseStringSlice parses a repeatable or comma-separated query param into a deduped slice.
func ParseStringSlice(r *http.Request, name string) []string {
	values := r.URL.Query()[name]
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			out = append(out, part)
		}
	}
	return out
}

// ParsePagination returns (page, limit, skip) enforcing 1-indexed pages and caps.
// defaultLimit and maxLimit are passed in so different endpoints can tighten the cap
// (e.g. /leaderboard/search uses 10/20).
func ParsePagination(r *http.Request, defaultLimit, maxLimit int) (page, limit int, skip int64) {
	page = ParseInt(r, "page", 1)
	if page < 1 {
		page = 1
	}
	limit = ParseInt(r, "limit", defaultLimit)
	if limit < 1 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	skip = int64(page-1) * int64(limit)
	return page, limit, skip
}

// ParseTimeRange parses `from` and `to` as RFC3339 (or empty). Returns (*time.Time, *time.Time, error).
func ParseTimeRange(r *http.Request) (*time.Time, *time.Time, error) {
	var from, to *time.Time
	if v := strings.TrimSpace(r.URL.Query().Get("from")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, nil, errors.New("invalid 'from' (want RFC3339)")
		}
		tt := t.UTC()
		from = &tt
	}
	if v := strings.TrimSpace(r.URL.Query().Get("to")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, nil, errors.New("invalid 'to' (want RFC3339)")
		}
		tt := t.UTC()
		to = &tt
	}
	return from, to, nil
}

// RequireInt64Path parses a required int64 path param. Returns an error on missing/invalid.
func RequireInt64Path(r *http.Request, name string) (int64, error) {
	v := strings.TrimSpace(r.PathValue(name))
	if v == "" {
		return 0, errors.New("missing path parameter: " + name)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, errors.New("invalid path parameter: " + name)
	}
	return n, nil
}

// RequireStringPath returns a non-empty path parameter or an error.
func RequireStringPath(r *http.Request, name string) (string, error) {
	v := strings.TrimSpace(r.PathValue(name))
	if v == "" {
		return "", errors.New("missing path parameter: " + name)
	}
	return v, nil
}
