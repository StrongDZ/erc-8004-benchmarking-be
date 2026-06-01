package runner

// grid.go — Full-factorial grid search on N parameters with k levels each.
// Caller is responsible for picking the top-k params before calling Grid.

// Grid enumerates all combinations of levels-per-param across specs and returns
// one RunResult per combination. Combinations are emitted in lexicographic order
// (last-spec varies fastest).
func Grid(specs []ParamSpec, levels int, recompute RecomputeFn) []RunResult {
	if len(specs) == 0 || levels < 2 {
		return nil
	}
	values := make([][]float64, len(specs))
	for i, p := range specs {
		values[i] = p.SweepValues(levels)
	}

	total := 1
	for range specs {
		total *= levels
	}
	results := make([]RunResult, 0, total)

	idx := make([]int, len(specs))
	for {
		cfg := make(map[string]float64, len(specs))
		for i, p := range specs {
			cfg[p.Name] = values[i][idx[i]]
		}
		results = append(results, RunResult{
			Config: cfg,
			Scores: recompute(cfg),
		})

		// Advance odometer-style.
		pos := len(specs) - 1
		for pos >= 0 {
			idx[pos]++
			if idx[pos] < levels {
				break
			}
			idx[pos] = 0
			pos--
		}
		if pos < 0 {
			return results
		}
	}
}
