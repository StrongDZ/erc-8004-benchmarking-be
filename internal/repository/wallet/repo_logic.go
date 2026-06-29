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
			"trustScore":           clipTrustScore(t0),
			"feedbackTotalCount":   int64(0),
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

// defaultReviewerColdStartT0 is the initial trust score assigned to a reviewer-only
// wallet created during the score-refresh counter upsert. Matches the default cold-start
// T0 used by UpsertCold for owner wallets.
const defaultReviewerColdStartT0 = 10.0

// buildFeedbackCounterUpsertUpdate constructs the Mongo update document used by
// BulkSetFeedbackCounters when upserting a reviewer wallet. On INSERT it creates a
// well-formed cold-start "user" wallet (identical to what UpsertCold would produce);
// on both INSERT and UPDATE it sets the derived counters. No field appears in both
// $set and $setOnInsert (Mongo rejects overlapping keys).
func buildFeedbackCounterUpsertUpdate(c FeedbackCounterSet, nowUnix int64) map[string]any {
	addr := normalizeAddress(c.Address)
	return map[string]any{
		"$setOnInsert": map[string]any{
			"_id":                WalletDocumentID(c.ChainID, c.Address),
			"address":            addr,
			"chainId":            c.ChainID,
			"kind":               string(WalletKindUser),
			"trustScore":         clipTrustScore(defaultReviewerColdStartT0),
			"feedbackTotalCount": int64(0),
			"junkRatio":          0.0,
			"createdAt":          nowUnix,
		},
		"$set": map[string]any{
			"feedbackValidCount": c.Valid,
			"feedbackJunkCount":  c.Junk,
			"updatedAt":          nowUnix,
		},
	}
}

const bulkTrustScoreChunkSize = 5000

// dedupeNonEmptyIDs returns unique non-empty strings preserving first-seen order.
func dedupeNonEmptyIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// chunkStrings splits s into sub-slices of at most size elements.
func chunkStrings(s []string, size int) [][]string {
	if len(s) == 0 || size <= 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(s)+size-1)/size)
	for i := 0; i < len(s); i += size {
		end := i + size
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[i:end])
	}
	return chunks
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
