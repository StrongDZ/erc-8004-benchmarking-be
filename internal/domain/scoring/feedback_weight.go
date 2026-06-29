package scoring

// EffectiveWi applies the current WalletTrust multiplier to a base weight.
func EffectiveWi(baseWi, walletTrust float64) float64 {
	return baseWi * WalletTrustMult(walletTrust)
}
