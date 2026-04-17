package utils

import (
	"os"
	"strconv"
	"strings"
)

// ToInt64 converts common numeric representations into int64.
// Returns (value, true) when conversion is possible, otherwise (0, false).
// String values are parsed as base-10 integers (e.g. environment variables).
func ToInt64(v any) (int64, bool) {
	// maxInt64 is the largest signed 64-bit integer value.
	const maxInt64 = int64(^uint64(0) >> 1)

	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		// Clamp to int64 range conservatively.
		if n > uint64(maxInt64) {
			return 0, false
		}
		return int64(n), true
	case uint32:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case string:
		s := strings.TrimSpace(n)
		if s == "" {
			return 0, false
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

// ToFloat64 converts common numeric representations into float64.
// String values use strconv.ParseFloat (e.g. environment variables).
func ToFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case uint64:
		return float64(n), true
	case string:
		s := strings.TrimSpace(n)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// Getenv returns os.Getenv(key) trimmed; if empty, returns fallback.
func Getenv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

// GetenvInt parses a decimal integer from the environment; invalid or missing uses fallback.
func GetenvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if n, ok := ToInt64(v); ok {
		return int(n)
	}
	return fallback
}

// GetenvBool parses a boolean from the environment ("true"/"1"/"yes" → true).
// Invalid or missing values use fallback.
func GetenvBool(key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return fallback
	}
}

// GetenvFloat parses a float from the environment; invalid or missing uses fallback.
func GetenvFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if f, ok := ToFloat64(v); ok {
		return f
	}
	return fallback
}

// GetStringArg returns a non-empty string from decoded event args (ABI decoder output).
func GetStringArg(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

// GetUint64Arg reads a uint64 from decoded event args.
// Supports JSON numbers, uint/int variants, and decimal strings (indexed uint256 from the EVM decoder).
func GetUint64Arg(args map[string]any, key string) uint64 {
	v, ok := args[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return uint64(n)
	case int64:
		return uint64(n)
	case uint64:
		return n
	case int:
		return uint64(n)
	case string:
		s := strings.TrimSpace(n)
		if s == "" {
			return 0
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0
		}
		return u
	default:
		return 0
	}
}

// GetUint8Arg reads a uint8 from decoded event args (e.g. valueDecimals). Values > 255 yield 0.
func GetUint8Arg(args map[string]any, key string) uint8 {
	v := GetUint64Arg(args, key)
	if v > 255 {
		return 0
	}
	return uint8(v)
}
