package wallet

// repo_logic.go — Pure-logic helpers. Mongo-free; unit-testable.

import (
	"fmt"

	"erc-8004-benchmarking-be/internal/utils"
)

// WalletDocumentID returns the wallets _id: {chainId}:{address-lowercased}.
func WalletDocumentID(chainID int64, address string) string {
	return fmt.Sprintf("%d:%s", chainID, utils.NormalizeAddress(address))
}

// normalizeAddress is a thin alias kept for in-package readability.
// Prefer utils.NormalizeAddress at write boundaries.
func normalizeAddress(addr string) string {
	return utils.NormalizeAddress(addr)
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

// buildUpsertColdUpdate constructs the Mongo update document for a cold-start upsert.
// $setOnInsert only fires on INSERT (new doc); $set fires on every call to bump updatedAt.
func buildUpsertColdUpdate(chainID int64, address string, t0 float64, nowUnix int64) map[string]any {
	addr := utils.NormalizeAddress(address)
	return map[string]any{
		"$setOnInsert": map[string]any{
			"_id":                  WalletDocumentID(chainID, address),
			"address":              addr,
			"chainId":              chainID,
			"kind":                 string(WalletKindUser),
			"trustScore":         clipTrustScore(t0),
			"feedbackTotalCount": int64(0),
			"feedbackValidCount":   int64(0),
			"feedbackJunkCount":    int64(0),
			"junkRatio":            0.0,
			"createdAt":            nowUnix,
		},
		"$set": map[string]any{
			"updatedAt": nowUnix,
		},
	}
}

// computeCounterIncrements returns the $inc subdocument for ApplyTrustDelta.
func computeCounterIncrements(isValid bool) map[string]any {
	inc := map[string]any{
		"feedbackTotalCount": int64(1),
	}
	if isValid {
		inc["feedbackValidCount"] = int64(1)
	} else {
		inc["feedbackJunkCount"] = int64(1)
	}
	return inc
}
