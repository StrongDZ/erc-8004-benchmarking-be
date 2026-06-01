package runner

// tornado.go — Tornado-chart input. Varies each parameter to its Low and High
// (already specified in ParamSpec; caller is responsible for choosing ±20% etc.),
// computes |Δ scalar metric| at each extreme, sorts descending by total |Δ|.

import "sort"

// TornadoEntry is one row in the tornado output.
type TornadoEntry struct {
	Param     string
	DeltaLow  float64 // |scalar(low) - scalar(default)|
	DeltaHigh float64 // |scalar(high) - scalar(default)|
}

// ScalarReducer collapses per-agent scores into a single scalar (typically the
// global mean composite score). Custom reducer lets the caller swap in "mean",
// "median", "separation", etc.
type ScalarReducer func(scores map[string]float64) float64

// Tornado returns entries sorted descending by (DeltaLow + DeltaHigh).
func Tornado(specs []ParamSpec, recompute RecomputeFn, scalar ScalarReducer) []TornadoEntry {
	baseCfg := defaultConfig(specs)
	baseline := scalar(recompute(baseCfg))

	out := make([]TornadoEntry, 0, len(specs))
	for _, p := range specs {
		cfgLow := copyCfg(baseCfg)
		cfgLow[p.Name] = p.Low
		cfgHigh := copyCfg(baseCfg)
		cfgHigh[p.Name] = p.High
		dl := absDiff(scalar(recompute(cfgLow)), baseline)
		dh := absDiff(scalar(recompute(cfgHigh)), baseline)
		out = append(out, TornadoEntry{Param: p.Name, DeltaLow: dl, DeltaHigh: dh})
	}
	sort.Slice(out, func(i, j int) bool {
		ti := out[i].DeltaLow + out[i].DeltaHigh
		tj := out[j].DeltaLow + out[j].DeltaHigh
		if ti == tj {
			return out[i].Param < out[j].Param
		}
		return ti > tj
	})
	return out
}

func copyCfg(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func absDiff(a, b float64) float64 {
	d := a - b
	if d < 0 {
		return -d
	}
	return d
}
