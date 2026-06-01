package runner

// oat.go — One-At-a-Time sweep over parameter specs.

// OATSweep varies each parameter through n values in [Low, High], holding
// other parameters at their Default. Returns one RunResult per (param × value).
//
// Ordering: results grouped by param in the order they appear in `specs`.
func OATSweep(specs []ParamSpec, n int, recompute RecomputeFn) []RunResult {
	results := make([]RunResult, 0, len(specs)*n)
	for _, p := range specs {
		for _, v := range p.SweepValues(n) {
			cfg := defaultConfig(specs)
			cfg[p.Name] = v
			results = append(results, RunResult{
				ParamName:  p.Name,
				ParamValue: v,
				Config:     cfg,
				Scores:     recompute(cfg),
			})
		}
	}
	return results
}

func defaultConfig(specs []ParamSpec) map[string]float64 {
	cfg := make(map[string]float64, len(specs))
	for _, p := range specs {
		cfg[p.Name] = p.Default
	}
	return cfg
}
