package scoring

import feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"

// ResolveBaseWi returns the pre-WalletTrust feedback weight stored on the record.
// Legacy rows baked WalletTrust at ingest: unbake via Wi / WalletTrustMult(ingest).
func ResolveBaseWi(fb feedbackrepo.FeedbackRecord) float64 {
	if fb.WalletTrustAtIngest > 0 {
		mult := WalletTrustMult(fb.WalletTrustAtIngest)
		if mult > 0 {
			return fb.Wi / mult
		}
	}
	return fb.Wi
}

// EffectiveWi applies the current WalletTrust multiplier to a base weight.
func EffectiveWi(baseWi, walletTrust float64) float64 {
	return baseWi * WalletTrustMult(walletTrust)
}
