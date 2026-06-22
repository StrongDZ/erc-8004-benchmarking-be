package pipeline

// cluster_d.go — Cụm D: WalletTrust Publisher sensitivity.
//
// Replaces the old Cụm D (EigenTrust propagation). Computes WalletTrust(owner) directly
// as the publisher score for each agent, sweeping the three blend weights WR/WE/WO.
//
//	WalletTrust(owner) = renorm(WR·R, WE·E, WO·O)
//	  R = ReviewerReliability(validFbCount, 0)            [from feedback snapshot]
//	  E = ExternalScore                                    [from baseline_scores]
//	  O = Σ(AgentDirectReputation_i·wm_i)/Σ(wm_i)        [owned agents with wm>0]
//
// Output: per-agent composite score using WalletTrust as publisher (20%).

import (
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// ClusterDParamSpecs returns the canonical Cluster-D parameter list with sweep ranges.
func ClusterDParamSpecs() []runner.ParamSpec {
	w := scoring.DefaultWalletTrustWeights()
	return []runner.ParamSpec{
		{Name: "WR", Default: w.WR, Low: 0.10, High: 0.80},
		{Name: "WE", Default: w.WE, Low: 0.00, High: 0.60},
		{Name: "WO", Default: w.WO, Low: 0.00, High: 0.70},
	}
}

// DefaultClusterDConfig returns the baseline config (Default values for every spec).
func DefaultClusterDConfig() map[string]float64 {
	cfg := make(map[string]float64)
	for _, p := range ClusterDParamSpecs() {
		cfg[p.Name] = p.Default
	}
	return cfg
}

type ownerEntry struct {
	directRep  float64
	weightMass float64
}

// clusterDTemplate holds precomputed per-owner data that is independent of the
// swept WR/WE/WO weights, so each recompute only re-derives WalletTrust blends.
type clusterDTemplate struct {
	// per agent
	baseline map[string]snapshot.BaselineScore // agentID → scores
	// per owner
	validCountByOwner      map[string]int64
	externalByOwner        map[string]float64
	externalPresentByOwner map[string]bool
	ownerToAgents          map[string][]ownerEntry // owner → [(directRep, weightMass)]
	agents                 []snapshot.AgentSnapshot
}

// buildClusterDTemplate precomputes the weight-independent parts from the snapshot.
func buildClusterDTemplate(data snapshot.SnapshotData) clusterDTemplate {
	cw := scoring.DefaultCompositeWeights()

	baselineByID := make(map[string]snapshot.BaselineScore, len(data.Baseline))
	for _, b := range data.Baseline {
		baselineByID[b.AgentID] = b
	}

	// R: count feedbacks each client wallet submitted (quality-only snapshot; junk=0).
	validCount := make(map[string]int64)
	for _, fb := range data.Feedbacks {
		if fb.ClientAddress != "" {
			validCount[fb.ClientAddress]++
		}
	}

	// E and O: iterate over agents.
	external := make(map[string]float64)
	externalPresent := make(map[string]bool)
	ownerToAgents := make(map[string][]ownerEntry)
	for _, a := range data.Agents {
		if a.Owner == "" {
			continue
		}
		b := baselineByID[a.ID]
		if b.ExternalScore > external[a.Owner] {
			external[a.Owner] = b.ExternalScore
		}
		if b.ExternalScore > 0 {
			externalPresent[a.Owner] = true
		}
		if b.WeightMass <= 0 {
			continue
		}
		dr := scoring.AgentDirectReputation(b.ReputationScore, b.AdoptionScore, b.ServicesScore, b.ComplianceScore, true, cw)
		ownerToAgents[a.Owner] = append(ownerToAgents[a.Owner], ownerEntry{directRep: dr, weightMass: b.WeightMass})
	}

	return clusterDTemplate{
		baseline:               baselineByID,
		validCountByOwner:      validCount,
		externalByOwner:        external,
		externalPresentByOwner: externalPresent,
		ownerToAgents:          ownerToAgents,
		agents:                 data.Agents,
	}
}

// walletTrustFor computes WalletTrust for an owner address given the sweep weights.
func (t *clusterDTemplate) walletTrustFor(owner string, w scoring.WalletTrustWeights) float64 {
	R := scoring.ReviewerReliability(t.validCountByOwner[owner], 0)
	E := t.externalByOwner[owner]
	ePresent := t.externalPresentByOwner[owner]

	var O float64
	oPresent := false
	if entries := t.ownerToAgents[owner]; len(entries) > 0 {
		var sumN, sumD float64
		for _, e := range entries {
			sumN += e.directRep * e.weightMass
			sumD += e.weightMass
		}
		if sumD > 0 {
			O = sumN / sumD
			oPresent = true
		}
	}
	return scoring.ComputeWalletTrust(R, E, O, ePresent, oPresent, w)
}

// ClusterDRecompute returns per-agent composite scores after substituting
// WalletTrust(owner) as publisher for each swept WR/WE/WO configuration.
func ClusterDRecompute(data snapshot.SnapshotData) runner.RecomputeFn {
	tpl := buildClusterDTemplate(data)
	cw := scoring.DefaultCompositeWeights()

	return func(cfg map[string]float64) map[string]float64 {
		w := scoring.WalletTrustWeights{WR: cfg["WR"], WE: cfg["WE"], WO: cfg["WO"]}

		// Cache WalletTrust per owner for this config.
		wtCache := make(map[string]float64)
		walletTrust := func(owner string) float64 {
			if v, ok := wtCache[owner]; ok {
				return v
			}
			v := tpl.walletTrustFor(owner, w)
			wtCache[owner] = v
			return v
		}

		out := make(map[string]float64, len(tpl.agents))
		for _, a := range tpl.agents {
			b := tpl.baseline[a.ID]
			pubScore := walletTrust(a.Owner)
			qualityPresent := b.WeightMass > 0
			composite := scoring.ComputeCompositeFromStats(
				b.ReputationScore, b.AdoptionScore, b.ServicesScore, pubScore, b.ComplianceScore,
				qualityPresent, true, cw,
			)
			out[a.ID] = composite
		}
		return out
	}
}
