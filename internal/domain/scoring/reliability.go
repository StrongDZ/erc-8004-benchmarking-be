package scoring

// reliability.go — direct-reputation priors used in WalletTrust computation. Pure; no I/O.

// ReviewerReliability returns a wallet's reviewer-reliability prior on [0,100]
// from its valid/junk feedback counters, as a Laplace-smoothed valid ratio.
// A wallet with no feedback yet returns 50 (neutral).
func ReviewerReliability(validCount, junkCount int64) float64 {
	return 100.0 * float64(validCount+1) / float64(validCount+junkCount+2)
}

// AgentDirectReputation returns an agent's composite score on [0,100] with the
// publisher component EXCLUDED. Used as the O-component input to WalletTrust to
// prevent owner↔agent circularity. reputationPresent mirrors qualityPresent.
func AgentDirectReputation(reputation, adoption, services, compliance float64, reputationPresent bool, w CompositeWeights) float64 {
	return ComputeCompositeRenorm([]WeightedComponent{
		{Score: reputation, Weight: w.Reputation, Present: reputationPresent},
		{Score: adoption, Weight: w.Adoption, Present: true},
		{Score: services, Weight: w.Services, Present: true},
		{Score: compliance, Weight: w.Compliance, Present: true},
		// publisher intentionally omitted
	})
}
