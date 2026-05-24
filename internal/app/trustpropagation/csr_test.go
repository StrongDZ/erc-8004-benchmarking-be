package trustpropagation

import (
	"math"
	"testing"
)

func buildTestCSR() *CSR {
	// 3-node cycle: 0→1, 1→2, 2→0. Column-stochastic.
	return BuildCSR(3, []csrEdge{
		{from: 0, to: 1, weight: 1.0},
		{from: 1, to: 2, weight: 1.0},
		{from: 2, to: 0, weight: 1.0},
	})
}

func TestBuildCSR_ColumnSumsToOne(t *testing.T) {
	m := buildTestCSR()
	for j := 0; j < m.n; j++ {
		var sum float64
		for _, e := range m.colData[j] {
			sum += e.weight
		}
		if math.Abs(sum-1.0) > 1e-9 {
			t.Errorf("col %d sum=%v want=1.0", j, sum)
		}
	}
}

func TestSpMV_SingleMassNode(t *testing.T) {
	m := buildTestCSR()
	v := []float64{1.0, 0.0, 0.0} // all mass at node 0
	out := m.SpMV(v)
	// node 0 → node 1 (weight=1.0), so out[1]=1.0
	if math.Abs(out[1]-1.0) > 1e-9 {
		t.Errorf("out[1]=%v want=1.0", out[1])
	}
	if math.Abs(out[0]) > 1e-9 || math.Abs(out[2]) > 1e-9 {
		t.Errorf("unexpected mass at 0 or 2: out=%v", out)
	}
}

func TestSpMV_PreservesMass(t *testing.T) {
	m := buildTestCSR()
	v := []float64{0.3, 0.4, 0.3}
	out := m.SpMV(v)
	var inSum, outSum float64
	for _, x := range v {
		inSum += x
	}
	for _, x := range out {
		outSum += x
	}
	if math.Abs(inSum-outSum) > 1e-9 {
		t.Errorf("mass not preserved: in=%v out=%v", inSum, outSum)
	}
}
