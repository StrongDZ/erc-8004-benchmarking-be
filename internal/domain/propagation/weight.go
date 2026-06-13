package propagation

// ComputeWeight returns the feedback's quality weight wi ∈ [WiBase, 1].
// Formula: wi = WiBase + (1 - WiBase)·Q, Q = qualityScore ∈ [0,1].
//
// Sender trust is intentionally NOT a factor: it propagates via the trust graph
// (the flowing value t[i] on each edge); including it here too would double-count
// sender trust inside agent reputation (Σ wᵢ·dᵢ·vᵢ).
func ComputeWeight(cfg PropagationConfig, qualityScore float64) float64 {
	q := clipFloat(qualityScore, 0, 1)
	return cfg.WiBase + (1-cfg.WiBase)*q
}
