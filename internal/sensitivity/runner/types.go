package runner

// types.go — Common types used by all four runner strategies (OAT, Tornado, Sobol, Grid).

// ParamSpec describes one tunable parameter and the range to sweep over.
type ParamSpec struct {
	Name    string  // canonical name for CSV columns and configs (e.g. "Alpha")
	Default float64 // baseline value
	Low     float64 // OAT minimum, Tornado low value
	High    float64 // OAT maximum, Tornado high value
}

// SweepValues returns the n equally-spaced values for OAT sweep over [Low, High].
func (p ParamSpec) SweepValues(n int) []float64 {
	if n < 2 {
		return []float64{p.Default}
	}
	step := (p.High - p.Low) / float64(n-1)
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = p.Low + float64(i)*step
	}
	return out
}

// RecomputeFn maps a full parameter configuration to per-agent scores.
// Closure should capture the snapshot data and apply the formula with the given config.
// Result key = agentID; result value = score.
type RecomputeFn func(cfg map[string]float64) map[string]float64

// RunResult captures the outcome of one parameter setting.
type RunResult struct {
	ParamName  string             // varied parameter (empty for multi-param results)
	ParamValue float64            // value of the varied parameter (NaN for multi-param)
	Config     map[string]float64 // full effective config
	Scores     map[string]float64 // per-agent scores
}
