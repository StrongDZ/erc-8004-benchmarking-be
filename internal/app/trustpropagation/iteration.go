package trustpropagation

// iteration.go — EigenTrust-style scalar propagation.
//   t[j] = α·inflow[j] + (1-α)·p[j]
//   inflow[agent] = Σ_clients M[agent][client]·t[client]   (source-normalized)
//   inflow[owner] = mean over owned agents of t[agent]      (mean = anti-gaming)
//   p             = direct reputation normalized to sum 1   (teleport)
// Output: min-max scaled scores for WALLET nodes only.

import (
	"math"
	"sort"
)

type IterConfig struct {
	Alpha   float64
	Epsilon float64 // L1 convergence threshold
	MaxIter int
}

func DefaultIterConfig() IterConfig { return IterConfig{Alpha: 0.85, Epsilon: 1e-6, MaxIter: 50} }

// EigenTrustPass returns rated wallet scores ∈ [0,100] (p99-normalized) and unrated wallet IDs + iters.
func EigenTrustPass(gd GraphData, cfg IterConfig) (WalletScores, int) {
	n := len(gd.Nodes)
	if n == 0 {
		return WalletScores{Rated: map[string]float64{}, Unrated: nil}, 0
	}
	idx := make(map[string]int, n)
	for i, nd := range gd.Nodes {
		idx[nd.ID] = i
	}

	weight := make([]float64, n)
	for i, nd := range gd.Nodes {
		weight[i] = nd.Weight
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

	ratedWallet := make(map[int]bool, len(ownedAgents))
	for oi, owned := range ownedAgents {
		var wsum float64
		for _, ai := range owned {
			wsum += weight[ai]
		}
		if wsum > 0 {
			ratedWallet[oi] = true
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
				var wsum, num float64
				for _, ai := range owned {
					wsum += weight[ai]
					num += weight[ai] * t[ai]
				}
				if wsum > 0 {
					inflow += num / wsum
				}
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

	ratedRaw := make(map[string]float64)
	unrated := make([]string, 0)
	for i, nd := range gd.Nodes {
		if nd.Kind != NodeKindWallet {
			continue
		}
		if ratedWallet[i] {
			ratedRaw[nd.ID] = t[i]
		} else {
			unrated = append(unrated, nd.ID)
		}
	}
	return WalletScores{Rated: p99Scale(ratedRaw), Unrated: unrated}, iters
}

// p99Scale winsorizes at the 99th percentile: out = clamp(100·(v-min)/(p99-min), 0, 100).
// Capping at p99 (not max) prevents a single hub from anchoring the whole scale.
func p99Scale(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	if len(in) == 0 {
		return out
	}
	vals := make([]float64, 0, len(in))
	minV := math.MaxFloat64
	for _, v := range in {
		vals = append(vals, v)
		if v < minV {
			minV = v
		}
	}
	sort.Float64s(vals)
	idx := int(math.Ceil(0.99*float64(len(vals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	p99 := vals[idx]
	span := p99 - minV
	if span < 1e-12 {
		for k := range in {
			out[k] = 50
		}
		return out
	}
	for k, v := range in {
		s := (v - minV) / span * 100
		if s < 0 {
			s = 0
		} else if s > 100 {
			s = 100
		}
		out[k] = s
	}
	return out
}
