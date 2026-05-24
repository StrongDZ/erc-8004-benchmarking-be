package trustpropagation

import (
	"math"
	"testing"
)

func TestTrustRankPass_3NodeCycle_Converges(t *testing.T) {
	nodes := []GraphNode{
		{ID: "n0", Kind: NodeKindAgent, TrustScore: 33, CompositeScore: 90},
		{ID: "n1", Kind: NodeKindWallet, TrustScore: 33},
		{ID: "n2", Kind: NodeKindWallet, TrustScore: 34},
	}
	edges := []GraphEdge{
		{From: "n0", To: "n1", Weight: 1.0},
		{From: "n1", To: "n2", Weight: 1.0},
		{From: "n2", To: "n0", Weight: 1.0},
	}
	cfg := PropagationIterConfig{Alpha: 0.85, Epsilon: 1e-5, MaxIter: 100, SeedThreshold: 80}
	scores, iters := TrustRankPass(GraphData{Nodes: nodes, Edges: edges}, cfg)

	if iters > cfg.MaxIter {
		t.Errorf("did not converge within %d iterations, got %d", cfg.MaxIter, iters)
	}
	if len(scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(scores))
	}
	for id, s := range scores {
		if s < 0 || s > 100 {
			t.Errorf("score %s=%v out of [0,100]", id, s)
		}
	}
}

func TestTrustRankPass_DampingBreaksCycle(t *testing.T) {
	if residual := math.Pow(0.85, 30); residual > 0.01 {
		t.Errorf("0.85^30 = %v > 0.01 — damping insufficient", residual)
	}
}

func TestMinMaxScale_BasicRange(t *testing.T) {
	in := map[string]float64{"a": 0.0, "b": 0.5, "c": 1.0}
	out := minMaxScale(in, 0, 100)
	if math.Abs(out["a"]-0) > 1e-9 {
		t.Errorf("a=%v want=0", out["a"])
	}
	if math.Abs(out["b"]-50) > 1e-9 {
		t.Errorf("b=%v want=50", out["b"])
	}
	if math.Abs(out["c"]-100) > 1e-9 {
		t.Errorf("c=%v want=100", out["c"])
	}
}

func TestMinMaxScale_AllEqual_ReturnsMid(t *testing.T) {
	in := map[string]float64{"a": 0.5, "b": 0.5}
	out := minMaxScale(in, 0, 100)
	for id, s := range out {
		if s != 50 {
			t.Errorf("%s=%v want=50", id, s)
		}
	}
}
