package metrics

// distribution.go — Distribution-shift metrics: Describe (mean/std/skew),
// KS2Sample (two-sample Kolmogorov-Smirnov D statistic).

import (
	"math"
	"sort"
)

// Description aggregates standard descriptive stats for a sample.
type Description struct {
	N    int
	Mean float64
	Std  float64
	Skew float64
}

// Describe computes mean, sample standard deviation, and Fisher-Pearson skewness.
// Returns zero-valued Description if input is empty.
func Describe(vals []float64) Description {
	n := len(vals)
	if n == 0 {
		return Description{}
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(n)
	var sqSum, cubeSum float64
	for _, v := range vals {
		d := v - mean
		sqSum += d * d
		cubeSum += d * d * d
	}
	variance := sqSum / float64(n-1)
	if n < 2 {
		variance = 0
	}
	std := math.Sqrt(variance)
	skew := 0.0
	if std > 0 && n > 0 {
		skew = (cubeSum / float64(n)) / math.Pow(std, 3)
	}
	return Description{N: n, Mean: mean, Std: std, Skew: skew}
}

// KS2Sample returns the two-sample KS D statistic (max distance between
// empirical CDFs). Range [0, 1]. Sorted copies are made — input is not mutated.
func KS2Sample(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	sa := append([]float64(nil), a...)
	sb := append([]float64(nil), b...)
	sort.Float64s(sa)
	sort.Float64s(sb)

	na, nb := float64(len(sa)), float64(len(sb))
	i, j := 0, 0
	var d float64
	for i < len(sa) && j < len(sb) {
		x := sa[i]
		y := sb[j]
		if x <= y {
			i++
		}
		if y <= x {
			j++
		}
		fa := float64(i) / na
		fb := float64(j) / nb
		if diff := math.Abs(fa - fb); diff > d {
			d = diff
		}
	}
	return d
}
