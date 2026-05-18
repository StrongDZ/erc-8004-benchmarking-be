package wallet

import "testing"

func TestNormalizeAddress_LowercasesAndTrims(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"0xABCdef0001", "0xabcdef0001"},
		{"  0xABCDEF0001  ", "0xabcdef0001"},
		{"", ""},
	}
	for _, c := range cases {
		got := normalizeAddress(c.in)
		if got != c.want {
			t.Errorf("normalizeAddress(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestClipTrustScore_ClampsToRange(t *testing.T) {
	cases := []struct {
		in, want float64
	}{
		{-5.0, 0.0},
		{0.0, 0.0},
		{50.5, 50.5},
		{100.0, 100.0},
		{120.0, 100.0},
	}
	for _, c := range cases {
		got := clipTrustScore(c.in)
		if got != c.want {
			t.Errorf("clipTrustScore(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestComputeColdStartT0_NoOwnedAgents_ReturnsDefault(t *testing.T) {
	got := computeColdStartT0(nil, 10.0)
	if got != 10.0 {
		t.Errorf("got=%v want=10.0", got)
	}
}

func TestComputeColdStartT0_OwnedAgents_ReturnsAverage(t *testing.T) {
	scores := []float64{60.0, 80.0, 100.0}
	got := computeColdStartT0(scores, 10.0)
	if got != 80.0 {
		t.Errorf("got=%v want=80.0", got)
	}
}

func TestComputeColdStartT0_EmptyOwnedAgents_ReturnsDefault(t *testing.T) {
	got := computeColdStartT0([]float64{}, 15.0)
	if got != 15.0 {
		t.Errorf("got=%v want=15.0", got)
	}
}

func TestWalletDocumentID_ProducesExpectedFormat(t *testing.T) {
	got := WalletDocumentID(8453, "0xABC123")
	want := "8453:0xabc123"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}
