package pipeline

// cluster_c.go — Cụm C: Quality Score + Edge Weight + Reward/Penalty Delta.
// Parameters: 5 Q-weights (sum=1), ReasoningLenFull, AttachmentCountFull,
//             WiMin, WiMax, Eta, Kappa.

import (
	"erc-8004-benchmarking-be/internal/domain/propagation"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterCParamSpecs returns the canonical Cluster-C parameter list with sweep ranges.
func ClusterCParamSpecs() []runner.ParamSpec {
	def := propagation.DefaultPropagationConfig()
	return []runner.ParamSpec{
		{Name: "QWeightReasoning", Default: def.QWeightReasoning, Low: 0.05, High: 0.50},
		{Name: "QWeightAttachment", Default: def.QWeightAttachment, Low: 0.05, High: 0.50},
		{Name: "QWeightBreakdown", Default: def.QWeightBreakdown, Low: 0.05, High: 0.50},
		{Name: "QWeightPayment", Default: def.QWeightPayment, Low: 0.05, High: 0.50},
		{Name: "QWeightConfidence", Default: def.QWeightConfidence, Low: 0.05, High: 0.50},
		{Name: "ReasoningLenFull", Default: float64(def.ReasoningLenFull), Low: 100, High: 400},
		{Name: "AttachmentCountFull", Default: float64(def.AttachmentCountFull), Low: 1, High: 6},
		{Name: "WiMin", Default: def.WiMin, Low: 0.1, High: 0.5},
		{Name: "WiMax", Default: def.WiMax, Low: 1.0, High: 3.0},
		{Name: "Eta", Default: def.Eta, Low: 0.25, High: 1.0},
		{Name: "Kappa", Default: def.Kappa, Low: 1.0, High: 4.0},
	}
}

// DefaultClusterCConfig returns the baseline config (Default values for every spec).
func DefaultClusterCConfig() map[string]float64 {
	cfg := make(map[string]float64)
	for _, p := range ClusterCParamSpecs() {
		cfg[p.Name] = p.Default
	}
	return cfg
}

// ClusterCRecompute returns per-agent post-propagation trust scores.
//
// For each agent we start from a neutral baseline trust of 1.0 and walk its
// feedbacks: for each one we compute the quality score Q, the edge weight wᵢ,
// then a reward (service_feedback) or penalty (anything else) delta.
//
// Sender trust is fixed at 100 (full trust) for SA simplicity — this isolates the
// quality/edge-weight parameters from the cross-cluster reputation dynamics. A
// consequence is that the sender-trust term of wᵢ is saturated (S=1.0), so WiMin
// (range below 0.6) never binds; that is reported as a finding, not a bug.
func ClusterCRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	byAgent := make(map[string][]snapshot.FeedbackSnapshot, len(data.Agents))
	for _, fb := range data.Feedbacks {
		byAgent[fb.AgentID] = append(byAgent[fb.AgentID], fb)
	}

	return func(cfg map[string]float64) map[string]float64 {
		pc := propagation.PropagationConfig{
			WiMin:               cfg["WiMin"],
			WiMax:               cfg["WiMax"],
			QWeightReasoning:    cfg["QWeightReasoning"],
			QWeightAttachment:   cfg["QWeightAttachment"],
			QWeightBreakdown:    cfg["QWeightBreakdown"],
			QWeightPayment:      cfg["QWeightPayment"],
			QWeightConfidence:   cfg["QWeightConfidence"],
			ReasoningLenFull:    int(cfg["ReasoningLenFull"]),
			AttachmentCountFull: int(cfg["AttachmentCountFull"]),
			Eta:                 cfg["Eta"],
			Kappa:               cfg["Kappa"],
		}
		// Normalise Q-weights so they sum to 1 (the propagation Q formula assumes this).
		qSum := pc.QWeightReasoning + pc.QWeightAttachment + pc.QWeightBreakdown +
			pc.QWeightPayment + pc.QWeightConfidence
		if qSum > 0 {
			pc.QWeightReasoning /= qSum
			pc.QWeightAttachment /= qSum
			pc.QWeightBreakdown /= qSum
			pc.QWeightPayment /= qSum
			pc.QWeightConfidence /= qSum
		}

		out := make(map[string]float64, len(data.Agents))
		for _, a := range data.Agents {
			trust := 1.0 // neutral baseline
			for _, fb := range byAgent[a.ID] {
				q := propagation.ComputeQualityScore(pc, propagation.QualityInput{
					ReasoningLen:         fb.ReasoningLen,
					AttachmentCount:      fb.AttachmentCount,
					HasRatingBreakdown:   fb.HasRatingBreakdown,
					HasProofOfPayment:    fb.HasProofOfPayment,
					ClassifierConfidence: fb.ClassifierConfidence,
				})
				wi := propagation.ComputeWeight(pc, 100.0, q)
				if fb.Category == "service_feedback" {
					trust += propagation.ComputeRewardDelta(pc, wi)
				} else {
					trust += propagation.ComputePenaltyDelta(pc, fb.ClassifierConfidence)
				}
			}
			out[a.ID] = trust
		}
		return out
	}
}
