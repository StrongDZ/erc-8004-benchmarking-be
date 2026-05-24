package trustpropagation

// csr.go — Native CSR for column-stochastic matrix. No external deps.

type colEntry struct {
	row    int
	weight float64
}

type csrEdge struct {
	from   int
	to     int
	weight float64
}

// CSR is a column-stochastic sparse matrix (adjacency list per column).
type CSR struct {
	n       int
	colData [][]colEntry
}

// BuildCSR constructs a column-stochastic CSR from a directed edge list.
// Normalizes each column so outgoing weights sum to 1.
func BuildCSR(n int, edges []csrEdge) *CSR {
	rawWeights := make([]map[int]float64, n)
	colTotals := make([]float64, n)
	for _, e := range edges {
		if rawWeights[e.from] == nil {
			rawWeights[e.from] = make(map[int]float64)
		}
		rawWeights[e.from][e.to] += e.weight
		colTotals[e.from] += e.weight
	}

	colData := make([][]colEntry, n)
	for j, m := range rawWeights {
		if m == nil || colTotals[j] == 0 {
			continue
		}
		entries := make([]colEntry, 0, len(m))
		for row, w := range m {
			entries = append(entries, colEntry{row: row, weight: w / colTotals[j]})
		}
		colData[j] = entries
	}
	return &CSR{n: n, colData: colData}
}

// SpMV computes M·v. Returns a new vector.
func (m *CSR) SpMV(v []float64) []float64 {
	out := make([]float64, m.n)
	for j, entries := range m.colData {
		vj := v[j]
		if vj == 0 || len(entries) == 0 {
			continue
		}
		for _, e := range entries {
			out[e.row] += e.weight * vj
		}
	}
	return out
}

// IsDangling reports whether column j has no outgoing edges.
func (m *CSR) IsDangling(j int) bool {
	return len(m.colData[j]) == 0
}
