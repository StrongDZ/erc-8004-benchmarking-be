package wallet

// repo_logic.go — Pure-logic helpers. Mongo-free; unit-testable.

import (
	"fmt"
	"strings"
)

// WalletDocumentID returns the wallets _id: {chainId}:{address-lowercased}.
func WalletDocumentID(chainID int64, address string) string {
	return fmt.Sprintf("%d:%s", chainID, normalizeAddress(address))
}

// normalizeAddress trims and lowercases an Ethereum address.
func normalizeAddress(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// clipTrustScore clamps a trust score into the canonical [0, 100] range.
func clipTrustScore(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

// computeColdStartT0 returns the initial trust score for a new wallet.
// If the wallet owns agents (ownedAgentScores non-empty), returns the average
// of those composite scores. Otherwise returns defaultT0 (typically 10).
func computeColdStartT0(ownedAgentScores []float64, defaultT0 float64) float64 {
	if len(ownedAgentScores) == 0 {
		return defaultT0
	}
	var sum float64
	for _, s := range ownedAgentScores {
		sum += s
	}
	return sum / float64(len(ownedAgentScores))
}
