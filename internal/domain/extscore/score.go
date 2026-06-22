package extscore

// score.go — pure external wallet-trust formula. No I/O. Blends costly-to-fake
// on-chain features (age, counterparties) above cheap ones (balance, nonce, ENS).
// Partial feature sets are renormalized via scoring.ComputeCompositeRenorm so a
// wallet whose explorer enrichment hasn't landed yet still gets a fair score.

import (
	"math"

	"erc-8004-benchmarking-be/internal/domain/scoring"
)

// Feature weights; sum to 1.0 when all are present.
var weights = struct{ Age, Counterparties, Balance, Nonce, ENS float64 }{
	Age: 0.30, Counterparties: 0.30, Balance: 0.20, Nonce: 0.10, ENS: 0.10,
}

// Features holds raw external on-chain features for one wallet. The Present
// flags mark which were actually fetched. ENSApplicable is true only on chains
// where ENS reverse resolution is meaningful (chain 1); elsewhere its weight is
// dropped rather than scored 0.
type Features struct {
	BalanceUSD           float64
	Nonce                uint64
	AgeDays              float64
	UniqueCounterparties int
	HasENS               bool

	BalancePresent        bool
	NoncePresent          bool
	AgePresent            bool
	CounterpartiesPresent bool
	ENSApplicable         bool
}

func satLog10(v, maxLog float64) float64 {
	if v <= 0 {
		return 0
	}
	l := math.Log10(1 + v)
	if l > maxLog {
		return 1
	}
	return l / maxLog
}

func boolScore(b bool) float64 {
	if b {
		return 100
	}
	return 0
}

// Score returns Score_ext in [0,100] over the PRESENT features, redistributing
// the weight of absent features.
func Score(f Features) float64 {
	age := math.Min(f.AgeDays/365.0, 1.0) * 100
	comps := []scoring.WeightedComponent{
		{Score: age, Weight: weights.Age, Present: f.AgePresent},
		{Score: 100 * satLog10(float64(f.UniqueCounterparties), 2), Weight: weights.Counterparties, Present: f.CounterpartiesPresent},
		{Score: 100 * satLog10(f.BalanceUSD, 4), Weight: weights.Balance, Present: f.BalancePresent},
		{Score: 100 * satLog10(float64(f.Nonce), 3), Weight: weights.Nonce, Present: f.NoncePresent},
		{Score: boolScore(f.HasENS), Weight: weights.ENS, Present: f.ENSApplicable},
	}
	return scoring.ComputeCompositeRenorm(comps)
}

// Complete reports whether all RPC+explorer features were present (ENS excluded —
// it is chain-specific and optional).
func Complete(f Features) bool {
	return f.AgePresent && f.CounterpartiesPresent && f.BalancePresent && f.NoncePresent
}

// explorerUnsupportedChains — all chains are now supported.
var explorerUnsupportedChains = map[int64]struct{}{}

// ExplorerApplicable is false on chains where explorer age/counterparty data is
// not fetched (Score_ext uses cheap RPC features only).
func ExplorerApplicable(chainID int64) bool {
	return true
}

// ExplorerUnsupportedChainIDs returns chain IDs excluded from explorer enrichment
// (for Mongo $nin filters).
func ExplorerUnsupportedChainIDs() []int64 {
	return nil
}

// CompleteForChain reports whether external enrichment is finished for chainID.
// On explorer-unsupported chains, balance+nonce suffice.
func CompleteForChain(chainID int64, f Features) bool {
	if !ExplorerApplicable(chainID) {
		return f.BalancePresent && f.NoncePresent
	}
	return Complete(f)
}

