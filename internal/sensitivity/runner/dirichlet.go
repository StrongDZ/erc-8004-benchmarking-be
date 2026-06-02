package runner

// dirichlet.go — Dirichlet sampling on the (k-1)-simplex.
//
// For weight-constrained parameters (must sum to 1), uniform Dirichlet(α=1) draws
// points uniformly inside the simplex. Higher concentration α concentrates samples
// near the centroid (1/k, 1/k, ..., 1/k).

import (
	"math"
	"math/rand"
)

// DirichletSamples returns n samples from Dirichlet(α, α, ..., α) — k dimensions.
// Each sample is a slice of length k summing to 1. Deterministic for a given seed.
func DirichletSamples(k, n int, alpha float64, seed int64) [][]float64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]float64, n)
	for i := 0; i < n; i++ {
		gammas := make([]float64, k)
		var sum float64
		for j := 0; j < k; j++ {
			g := sampleGamma(rng, alpha)
			gammas[j] = g
			sum += g
		}
		row := make([]float64, k)
		for j := 0; j < k; j++ {
			if sum == 0 {
				row[j] = 1.0 / float64(k)
			} else {
				row[j] = gammas[j] / sum
			}
		}
		out[i] = row
	}
	return out
}

// sampleGamma draws from Gamma(shape, 1) using Marsaglia-Tsang's algorithm for
// shape ≥ 1, falling back to the G(shape)=G(shape+1)·U^(1/shape) boosting trick
// for shape < 1.
func sampleGamma(rng *rand.Rand, shape float64) float64 {
	if shape < 1 {
		g := sampleGamma(rng, shape+1)
		u := rng.Float64()
		return g * math.Pow(u, 1.0/shape)
	}
	d := shape - 1.0/3.0
	c := 1.0 / math.Sqrt(9.0*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}
