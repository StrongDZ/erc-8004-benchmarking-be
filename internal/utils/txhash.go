package utils

// txhash.go — Hex transaction hash normalization for Mongo queries ($in variants).

import "strings"

// TxHashMatchVariants returns lowercase hex forms of a tx hash for equality matching:
// with and without "0x" prefix.
func TxHashMatchVariants(txHash string) []string {
	h := strings.ToLower(strings.TrimSpace(txHash))
	if h == "" {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	add(h)
	if strings.HasPrefix(h, "0x") {
		add(strings.TrimPrefix(h, "0x"))
	} else {
		add("0x" + h)
	}
	return out
}
