package propagation

// ComputeRewardDelta returns Δ+ > 0 applied to sender trust on valid feedback.
// Δ+ = Eta * wi
func ComputeRewardDelta(cfg PropagationConfig, wi float64) float64 {
	return cfg.Eta * wi
}

// ComputePenaltyDelta returns Δ- < 0 applied to sender trust on gated feedback.
// Δ- = -Kappa * classifierConfidence
func ComputePenaltyDelta(cfg PropagationConfig, classifierConfidence float64) float64 {
	return -cfg.Kappa * clipFloat(classifierConfidence, 0, 1)
}
