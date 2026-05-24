package propagation

// ComputeWeight returns wi ∈ [cfg.WiMin, cfg.WiMax].
// senderTrustScore is on [0,100]; qualityScore is on [0,1].
// Formula: wi = clip(0.2 + 0.4·S + 0.9·Q, WiMin, WiMax)
func ComputeWeight(cfg PropagationConfig, senderTrustScore float64, qualityScore float64) float64 {
	S := clipFloat(senderTrustScore/100.0, 0, 1)
	Q := clipFloat(qualityScore, 0, 1)
	return clipFloat(0.2+0.4*S+0.9*Q, cfg.WiMin, cfg.WiMax)
}
