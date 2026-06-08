package trustpropagation

// iteration.go — EigenTrust-style scalar propagation.
//   t[j] = α·inflow[j] + (1-α)·p[j]
//   inflow[agent] = Σ_clients M[agent][client]·t[client]   (source-normalized)
//   inflow[owner] = mean over owned agents of t[agent]      (mean = anti-gaming)
//   p             = direct reputation normalized to sum 1   (teleport)
// Output: min-max scaled scores for WALLET nodes only.

import "math"

type IterConfig struct {
	Alpha   float64
	Epsilon float64 // L1 convergence threshold
	MaxIter int
}

func DefaultIterConfig() IterConfig { return IterConfig{Alpha: 0.85, Epsilon: 1e-6, MaxIter: 50} }

// EigenTrustPass returns wallet scores ∈ [0,100] (min-max over wallets) + iters.
func EigenTrustPass(gd GraphData, cfg IterConfig) (map[string]float64, int) {
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

	ownedAgents := make(map[int][]int)
	for i, nd := range gd.Nodes {
		if nd.Kind == NodeKindAgent && nd.OwnerID != "" {
			if oi, ok := idx[nd.OwnerID]; ok {
				ownedAgents[oi] = append(ownedAgents[oi], i)
			}
		}
	}

	p := make([]float64, n)
	var pSum float64
	for i, nd := range gd.Nodes {
		p[i] = nd.DirectRep
		pSum += nd.DirectRep
	}
	if pSum > 0 {
		for i := range p {
			p[i] /= pSum
		}
	} else {
		for i := range p {
			p[i] = 1.0 / float64(n)
		}
	}

	t := make([]float64, n)
	copy(t, p)
	alpha, oneMinus := cfg.Alpha, 1.0-cfg.Alpha

	iters := 0
	for iters = 0; iters < cfg.MaxIter; iters++ {
		mv := M.SpMV(t)
		next := make([]float64, n)
		for j := 0; j < n; j++ {
			inflow := mv[j]
			if owned := ownedAgents[j]; len(owned) > 0 {
				var sum float64
				for _, ai := range owned {
					sum += t[ai]
				}
				inflow += sum / float64(len(owned))
			}
			next[j] = alpha*inflow + oneMinus*p[j]
		}
		var l1 float64
		for j := range next {
			l1 += math.Abs(next[j] - t[j])
		}
		t = next
		if l1 < cfg.Epsilon {
			break
		}
	}
	iters++

	raw := make(map[string]float64)
	for i, nd := range gd.Nodes {
		if nd.Kind == NodeKindWallet {
			raw[nd.ID] = t[i]
		}
	}
	return minMaxScale(raw, 0, 100), iters
}

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
	if span := maxV - minV; span < 1e-12 {
		mid := (lo + hi) / 2
		for k := range in {
			out[k] = mid
		}
	} else {
		for k, v := range in {
			out[k] = lo + (v-minV)/span*(hi-lo)
		}
	}
	return out
}
