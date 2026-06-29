package pipeline

// cluster_c.go — Cụm C: Feedback Quality Score → Edge Weight.
//
// The propagation model no longer applies a per-feedback reward/penalty trust walk
// (the old WiMin/WiMax/Eta/Kappa delta model is gone). Quality signals now feed a
// single edge weight that scales each client→agent edge in the trust graph:
//
//	Q  = QWeightR·R + QWeightA·A + QWeightB·B + QWeightP·P + QWeightC·Conf   ∈ [0,1]
//	wᵢ = WiBase + (1 - WiBase)·Q                                              ∈ [WiBase,1]
//
// Cụm C therefore measures how the quality-signal weights (and the ReasoningLen /
// AttachmentCount saturation points and WiBase floor) move each agent's mean
// incoming edge weight — the quantity that Cụm A's reputation mass accumulates.
//
// Parameters: 5 Q-weights (sum=1), ReasoningLenFull, AttachmentCountFull, WiBase.

import (
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterCParamSpecs returns the canonical Cluster-C parameter list with sweep ranges.
func ClusterCParamSpecs() []runner.ParamSpec {
	def := scoring.DefaultQualityWeightConfig()
	return []runner.ParamSpec{
		{Name: "QWeightReasoning", Default: def.QWeightReasoning, Low: 0.05, High: 0.50},
		{Name: "QWeightAttachment", Default: def.QWeightAttachment, Low: 0.05, High: 0.50},
		{Name: "QWeightBreakdown", Default: def.QWeightBreakdown, Low: 0.05, High: 0.50},
		{Name: "QWeightPayment", Default: def.QWeightPayment, Low: 0.05, High: 0.50},
		{Name: "QWeightConfidence", Default: def.QWeightConfidence, Low: 0.05, High: 0.50},
		{Name: "ReasoningLenFull", Default: float64(def.ReasoningLenFull), Low: 100, High: 400},
		{Name: "AttachmentCountFull", Default: float64(def.AttachmentCountFull), Low: 1, High: 6},
		{Name: "WiBase", Default: def.WiBase, Low: 0.1, High: 0.8},
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

// ClusterCRecompute returns per-agent mean incoming edge weight wᵢ. For each agent we
// compute Q and wᵢ for every feedback, then average — the quality-weighted edge
// strength that flows into the trust graph.
func ClusterCRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	byAgent := make(map[string][]snapshot.FeedbackSnapshot, len(data.Agents))
	for _, fb := range data.Feedbacks {
		byAgent[fb.AgentID] = append(byAgent[fb.AgentID], fb)
	}

	return func(cfg map[string]float64) map[string]float64 {
		pc := scoring.QualityWeightConfig{
			WiBase:              cfg["WiBase"],
			QWeightReasoning:    cfg["QWeightReasoning"],
			QWeightAttachment:   cfg["QWeightAttachment"],
			QWeightBreakdown:    cfg["QWeightBreakdown"],
			QWeightPayment:      cfg["QWeightPayment"],
			QWeightConfidence:   cfg["QWeightConfidence"],
			ReasoningLenFull:    int(cfg["ReasoningLenFull"]),
			AttachmentCountFull: int(cfg["AttachmentCountFull"]),
		}
		// Normalise Q-weights so they sum to 1 (the Q formula assumes this).
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
			fbs := byAgent[a.ID]
			if len(fbs) == 0 {
				out[a.ID] = 0
				continue
			}
			var sum float64
			for _, fb := range fbs {
				q := scoring.ComputeFeedbackQuality(pc, scoring.FeedbackQualityInput{
					ReasoningLen:         fb.ReasoningLen,
					AttachmentCount:      fb.AttachmentCount,
					HasRatingBreakdown:   fb.HasRatingBreakdown,
					HasProofOfPayment:    fb.HasProofOfPayment,
					ClassifierConfidence: fb.ClassifierConfidence,
				})
				sum += scoring.ComputeFeedbackQualityWeight(pc, q)
			}
			out[a.ID] = sum / float64(len(fbs))
		}
		return out
	}
}
