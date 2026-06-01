package metrics

// rank.go — Rank stability metrics: Spearman ρ, Kendall τ, Top-K churn.

import (
	"sort"
)

// Spearman returns Spearman's rank correlation coefficient between two score maps.
// Both maps MUST cover the same keys; entries with missing peer are ignored.
// Result in [-1, 1]. Returns 0 when fewer than 2 common keys.
func Spearman(a, b map[string]float64) float64 {
	keys := commonKeys(a, b)
	if len(keys) < 2 {
		return 0
	}
	rankA := assignRanks(keys, a)
	rankB := assignRanks(keys, b)
	return pearson(rankA, rankB)
}

// Kendall returns Kendall's τ (tau-a) between two score maps.
// Result in [-1, 1]. Returns 0 when fewer than 2 common keys.
func Kendall(a, b map[string]float64) float64 {
	keys := commonKeys(a, b)
	n := len(keys)
	if n < 2 {
		return 0
	}
	var concordant, discordant int
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			da := a[keys[i]] - a[keys[j]]
			db := b[keys[i]] - b[keys[j]]
			sign := da * db
			switch {
			case sign > 0:
				concordant++
			case sign < 0:
				discordant++
			}
		}
	}
	total := n * (n - 1) / 2
	return float64(concordant-discordant) / float64(total)
}

// TopKChurn returns the fraction of items in the top-K of `a` that are NOT in
// the top-K of `b`. Range [0, 1]. K is clamped to min(K, len(a), len(b)).
func TopKChurn(a, b map[string]float64, k int) float64 {
	topA := topK(a, k)
	topB := topK(b, k)
	if len(topA) == 0 {
		return 0
	}
	bSet := make(map[string]struct{}, len(topB))
	for _, id := range topB {
		bSet[id] = struct{}{}
	}
	missing := 0
	for _, id := range topA {
		if _, ok := bSet[id]; !ok {
			missing++
		}
	}
	return float64(missing) / float64(len(topA))
}

func commonKeys(a, b map[string]float64) []string {
	out := make([]string, 0, len(a))
	for k := range a {
		if _, ok := b[k]; ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// assignRanks returns rank values (1-indexed, ties handled via average).
func assignRanks(keys []string, m map[string]float64) []float64 {
	type kv struct {
		key string
		val float64
	}
	pairs := make([]kv, len(keys))
	for i, k := range keys {
		pairs[i] = kv{k, m[k]}
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].val < pairs[j].val })
	ranks := make(map[string]float64, len(pairs))
	for i := 0; i < len(pairs); {
		j := i
		for j+1 < len(pairs) && pairs[j+1].val == pairs[i].val {
			j++
		}
		avg := float64(i+1+j+1) / 2.0
		for k := i; k <= j; k++ {
			ranks[pairs[k].key] = avg
		}
		i = j + 1
	}
	out := make([]float64, len(keys))
	for i, k := range keys {
		out[i] = ranks[k]
	}
	return out
}

func pearson(x, y []float64) float64 {
	n := float64(len(x))
	if n < 2 {
		return 0
	}
	var sx, sy, sxx, syy, sxy float64
	for i := range x {
		sx += x[i]
		sy += y[i]
		sxx += x[i] * x[i]
		syy += y[i] * y[i]
		sxy += x[i] * y[i]
	}
	num := n*sxy - sx*sy
	den := sqrt((n*sxx - sx*sx) * (n*syy - sy*sy))
	if den == 0 {
		return 0
	}
	return num / den
}

func sqrt(v float64) float64 {
	if v <= 0 {
		return 0
	}
	// Newton's iteration — avoids importing math from a hot path test.
	x := v
	for i := 0; i < 20; i++ {
		x = 0.5 * (x + v/x)
	}
	return x
}

func topK(m map[string]float64, k int) []string {
	type kv struct {
		key string
		val float64
	}
	pairs := make([]kv, 0, len(m))
	for kk, v := range m {
		pairs = append(pairs, kv{kk, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].val == pairs[j].val {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].val > pairs[j].val
	})
	if k > len(pairs) {
		k = len(pairs)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = pairs[i].key
	}
	return out
}
