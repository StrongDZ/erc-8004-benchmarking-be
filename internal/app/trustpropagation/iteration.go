package trustpropagation

// iteration.go — TrustRankPass: weighted TrustRank power iteration.
// Formula: t^{k+1} = (1-α)·d + α·M·t^k
// Cycle handling: damping factor (1-α) leaks mass to seed each step.

import "math"

// PropagationIterConfig holds hyperparameters for one pass.
type PropagationIterConfig struct {
	Alpha         float64
	Epsilon       float64
	MaxIter       int
	SeedThreshold float64
}

// DefaultIterConfig returns design-doc defaults.
func DefaultIterConfig() PropagationIterConfig {
	return PropagationIterConfig{Alpha: 0.85, Epsilon: 1e-4, MaxIter: 30, SeedThreshold: 80}
}

// TrustRankPass runs one full propagation pass.
// Returns map[nodeID]score ∈ [0,100] and the actual iteration count.
func TrustRankPass(gd GraphData, cfg PropagationIterConfig) (map[string]float64, int) {
	n := len(gd.Nodes)
	if n == 0 {
		return map[string]float64{}, 0
	}

	idx := make(map[string]int, n)
	for i, nd := range gd.Nodes {
		idx[nd.ID] = i
	}

	edges := make([]csrEdge, 0, len(gd.Edges))
	for _, e := range gd.Edges {
		fi, okF := idx[e.From]
		ti, okT := idx[e.To]
		if !okF || !okT || e.Weight <= 0 {
			continue
		}
		edges = append(edges, csrEdge{from: fi, to: ti, weight: e.Weight})
	}
	M := BuildCSR(n, edges)

	// Seed vector d: normalized compositeScore for qualifying agents.
	d := make([]float64, n)
	var seedSum float64
	for i, nd := range gd.Nodes {
		if nd.Kind == NodeKindAgent && nd.CompositeScore > cfg.SeedThreshold {
			d[i] = nd.CompositeScore
			seedSum += nd.CompositeScore
		}
	}
	if seedSum > 0 {
		for i := range d {
			d[i] /= seedSum
		}
	} else {
		for i := range d {
			d[i] = 1.0 / float64(n)
		}
	}

	// Initial t: normalized current trust.
	t := make([]float64, n)
	var tSum float64
	for i, nd := range gd.Nodes {
		t[i] = nd.TrustScore
		tSum += nd.TrustScore
	}
	if tSum > 0 {
		for i := range t {
			t[i] /= tSum
		}
	} else {
		for i := range t {
			t[i] = 1.0 / float64(n)
		}
	}

	alpha := cfg.Alpha
	oMinusAlpha := 1.0 - alpha

	iters := 0
	for iters = 0; iters < cfg.MaxIter; iters++ {
		mv := M.SpMV(t)

		// Dangling mass → distribute via d.
		var dangMass float64
		for j := 0; j < n; j++ {
			if M.IsDangling(j) {
				dangMass += t[j]
			}
		}

		next := make([]float64, n)
		for i := range next {
			next[i] = oMinusAlpha*d[i] + alpha*(mv[i]+dangMass*d[i])
		}

		maxDiff := 0.0
		for i := range next {
			if diff := math.Abs(next[i] - t[i]); diff > maxDiff {
				maxDiff = diff
			}
		}
		t = next
		if maxDiff < cfg.Epsilon {
			break
		}
	}
	iters++ // Count this as one iteration completed

	raw := make(map[string]float64, n)
	for i, nd := range gd.Nodes {
		raw[nd.ID] = t[i]
	}
	return minMaxScale(raw, 0, 100), iters
}

// minMaxScale maps values to [lo, hi]. Returns mid when all values are equal.
func minMaxScale(in map[string]float64, lo, hi float64) map[string]float64 {
	if len(in) == 0 {
		return in
	}
	minV, maxV := math.MaxFloat64, -math.MaxFloat64
	for _, v := range in {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	out := make(map[string]float64, len(in))
	span := maxV - minV
	if span < 1e-12 {
		mid := (lo + hi) / 2
		for k := range in {
			out[k] = mid
		}
		return out
	}
	for k, v := range in {
		out[k] = lo + (v-minV)/span*(hi-lo)
	}
	return out
}
