package evm

// event_abi.go — Builds topic0 hashes for FilterLogs from JSON ABI event fragments.

import (
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"erc-8004-benchmarking-be/internal/repository/config"
)

// EventTopic0Hashes builds topic0 values for FilterLogs from JSON ABI event fragments.
// For each non-anonymous event: keccak256("Name(type1,type2,...)") per Solidity ABI spec.
// Anonymous events are skipped (no signature topic0 to match).
func EventTopic0Hashes(eventABI []config.ABIEventFragment) []common.Hash {
	if len(eventABI) == 0 {
		return nil
	}

	seen := make(map[common.Hash]struct{})
	out := make([]common.Hash, 0)

	for _, ev := range eventABI {
		if ev.Anonymous {
			continue
		}
		sig := canonicalEventSignature(ev)
		if sig == "" {
			continue
		}
		h := crypto.Keccak256Hash([]byte(sig))
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	return out
}

// canonicalEventSignature returns the Solidity event signature string used for topic0 hashing.
func canonicalEventSignature(ev config.ABIEventFragment) string {
	name := strings.TrimSpace(ev.Name)
	if name == "" {
		return ""
	}
	types := make([]string, 0, len(ev.Inputs))
	for _, in := range ev.Inputs {
		t := abiInputCanonicalType(in)
		if t == "" {
			return ""
		}
		types = append(types, t)
	}
	return name + "(" + strings.Join(types, ",") + ")"
}

func abiInputCanonicalType(in config.ABIEventInput) string {
	if t := strings.TrimSpace(in.Type); t != "" {
		return t
	}
	return strings.TrimSpace(in.InternalType)
}
