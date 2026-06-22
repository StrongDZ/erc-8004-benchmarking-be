package scoring

// wallet_trust.go — Unified WalletTrust score for any Ethereum address.
// Replaces EigenTrust propagation as publisher score and feedback-weight multiplier.

// WalletTrustWMin is the floor for WalletTrustMult: new wallets start at 40% weight.
const WalletTrustWMin = 0.40

// WalletTrustWeights holds the blend weights for the three WalletTrust inputs.
type WalletTrustWeights struct {
	WR float64 // reviewer track record (ReviewerReliability)
	WE float64 // external on-chain score
	WO float64 // quality of owned agents (AgentDirectReputation weighted by mass)
}

// DefaultWalletTrustWeights returns the baseline blend weights.
func DefaultWalletTrustWeights() WalletTrustWeights {
	return WalletTrustWeights{WR: 0.35, WE: 0.25, WO: 0.40}
}

// ComputeWalletTrust returns WalletTrust ∈ [0,100] renormalized over present components.
//
//	R = ReviewerReliability(validCount, junkCount)
//	E = wallets.external.score  (ePresent = wallets.external.present)
//	O = Σ(AgentDirectReputation_i·weightMass_i)/Σ(weightMass_i) for owned agents with wm>0
func ComputeWalletTrust(R, E, O float64, ePresent, oPresent bool, w WalletTrustWeights) float64 {
	return ComputeCompositeRenorm([]WeightedComponent{
		{Score: R, Weight: w.WR, Present: true},
		{Score: E, Weight: w.WE, Present: ePresent},
		{Score: O, Weight: w.WO, Present: oPresent},
	})
}

// WalletTrustMult converts a WalletTrust score to a feedback-weight multiplier ∈ [WMin, 1].
//
//	mult = WMin + (1−WMin) × (walletTrust / 100)
func WalletTrustMult(walletTrust float64) float64 {
	return WalletTrustWMin + (1-WalletTrustWMin)*(walletTrust/100.0)
}
