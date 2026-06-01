package runner

// sobol.go — Saltelli-Sobol global sensitivity indices.
//
// Reference: Saltelli et al. 2010, "Variance based sensitivity analysis of model
// output: design and estimator for the total sensitivity index" — uses the
// matrices A, B, AB_i for k parameters with N base samples.

import "math/rand"

// SobolEntry holds the first-order S1 and total-order ST indices for one parameter.
type SobolEntry struct {
	Param      string
	FirstOrder float64 // S1 in [0, 1]: variance explained by this param alone
	TotalOrder float64 // ST in [0, 1]: variance explained including all interactions
}

// Sobol runs Saltelli sampling with N base samples → N*(k+2) function evaluations.
// Returns one SobolEntry per parameter in the input order.
func Sobol(specs []ParamSpec, n int, seed int64, recompute RecomputeFn, scalar ScalarReducer) []SobolEntry {
	k := len(specs)
	rng := rand.New(rand.NewSource(seed))

	// Sample matrices A and B (each N×k) in [Low,High] per column.
	A := sampleMatrix(rng, n, specs)
	B := sampleMatrix(rng, n, specs)

	yA := evalMatrix(A, specs, recompute, scalar)
	yB := evalMatrix(B, specs, recompute, scalar)

	// AB_i: column i of A replaced by column i of B → evaluate.
	yAB := make([][]float64, k)
	for i := 0; i < k; i++ {
		ABi := cloneMatrix(A)
		for row := 0; row < n; row++ {
			ABi[row][i] = B[row][i]
		}
		yAB[i] = evalMatrix(ABi, specs, recompute, scalar)
	}

	// Variance V(Y) from yA.
	V := variance(yA)
	if V == 0 {
		// All evaluations identical → indices undefined; return zeros.
		out := make([]SobolEntry, k)
		for i, p := range specs {
			out[i] = SobolEntry{Param: p.Name}
		}
		return out
	}

	out := make([]SobolEntry, k)
	for i, p := range specs {
		// Saltelli 2010 estimators:
		//   S1_i = (1/N) Σ yB * (yAB_i - yA) / V
		//   ST_i = (1/(2N)) Σ (yA - yAB_i)^2 / V
		var s1Num, stNum float64
		for j := 0; j < n; j++ {
			s1Num += yB[j] * (yAB[i][j] - yA[j])
			diff := yA[j] - yAB[i][j]
			stNum += diff * diff
		}
		out[i] = SobolEntry{
			Param:      p.Name,
			FirstOrder: clip01(s1Num / float64(n) / V),
			TotalOrder: clip01(stNum / (2 * float64(n)) / V),
		}
	}
	return out
}

func sampleMatrix(rng *rand.Rand, n int, specs []ParamSpec) [][]float64 {
	out := make([][]float64, n)
	for i := 0; i < n; i++ {
		row := make([]float64, len(specs))
		for j, p := range specs {
			row[j] = p.Low + rng.Float64()*(p.High-p.Low)
		}
		out[i] = row
	}
	return out
}

func cloneMatrix(m [][]float64) [][]float64 {
	out := make([][]float64, len(m))
	for i, row := range m {
		out[i] = append([]float64(nil), row...)
	}
	return out
}

func evalMatrix(m [][]float64, specs []ParamSpec, recompute RecomputeFn, scalar ScalarReducer) []float64 {
	out := make([]float64, len(m))
	for i, row := range m {
		cfg := make(map[string]float64, len(specs))
		for j, p := range specs {
			cfg[p.Name] = row[j]
		}
		out[i] = scalar(recompute(cfg))
	}
	return out
}

func variance(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	var sq float64
	for _, v := range vals {
		d := v - mean
		sq += d * d
	}
	return sq / float64(len(vals)-1)
}

func clip01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
