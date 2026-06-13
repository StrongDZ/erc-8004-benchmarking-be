package wallet

import "testing"

func TestBuildExternalUpdate(t *testing.T) {
	in := ExternalUpdate{ID: "1:0xabc", Doc: ExternalDoc{Score: 62.4, Present: true, Complete: false}}
	set := buildExternalSet(in)
	if set["external.score"] != 62.4 {
		t.Fatalf("want score path set, got %v", set["external.score"])
	}
	if set["external.present"] != true {
		t.Fatalf("want present path set, got %v", set["external.present"])
	}
}

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

func TestPickProfileWalletDoc_PrefersRatedHighestScore(t *testing.T) {
	ratedLow := WalletDocument{Address: "0xabc", TrustRated: true, TrustScore: 40, FeedbackTotalCount: 1}
	ratedHigh := WalletDocument{Address: "0xabc", TrustRated: true, TrustScore: 90, FeedbackTotalCount: 5}
	unrated := WalletDocument{Address: "0xabc", TrustRated: false, TrustScore: 99, FeedbackTotalCount: 9}

	got := pickProfileWalletDoc([]WalletDocument{unrated, ratedLow, ratedHigh})
	if got == nil || got.TrustScore != 90 || !got.TrustRated {
		t.Fatalf("expected rated 90, got %+v", got)
	}
	if got.FeedbackTotalCount != 5 {
		t.Fatalf("feedbackTotalCount=%d want 5", got.FeedbackTotalCount)
	}
}

func TestWalletDocumentID_ProducesExpectedFormat(t *testing.T) {
	got := WalletDocumentID(8453, "0xABC123")
	want := "8453:0xabc123"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}

func TestBuildUpsertColdUpdate_NewWalletIncludesCreatedAt(t *testing.T) {
	now := int64(1716000000)
	update := buildUpsertColdUpdate(8453, "0xABC", 10.0, now)

	setOnInsert, ok := update["$setOnInsert"].(map[string]any)
	if !ok {
		t.Fatalf("expected $setOnInsert map, got %T", update["$setOnInsert"])
	}
	if setOnInsert["_id"] != "8453:0xabc" {
		t.Errorf("_id=%v want 8453:0xabc", setOnInsert["_id"])
	}
	if setOnInsert["address"] != "0xabc" {
		t.Errorf("address=%v want 0xabc", setOnInsert["address"])
	}
	if setOnInsert["chainId"] != int64(8453) {
		t.Errorf("chainId=%v want 8453", setOnInsert["chainId"])
	}
	if setOnInsert["trustScore"] != 10.0 {
		t.Errorf("trustScore=%v want 10.0", setOnInsert["trustScore"])
	}
	if setOnInsert["kind"] != "user" {
		t.Errorf("kind=%v want user", setOnInsert["kind"])
	}
	if setOnInsert["createdAt"] != now {
		t.Errorf("createdAt=%v want %v", setOnInsert["createdAt"], now)
	}

	setUpdate, ok := update["$set"].(map[string]any)
	if !ok {
		t.Fatalf("expected $set map, got %T", update["$set"])
	}
	if setUpdate["updatedAt"] != now {
		t.Errorf("updatedAt=%v want %v", setUpdate["updatedAt"], now)
	}
}

func TestApplyTrustDeltaCounters_ValidIncrements(t *testing.T) {
	got := computeCounterIncrements(true)
	if got["feedbackTotalCount"] != int64(1) {
		t.Errorf("totalCount inc=%v want 1", got["feedbackTotalCount"])
	}
	if got["feedbackValidCount"] != int64(1) {
		t.Errorf("validCount inc=%v want 1", got["feedbackValidCount"])
	}
	if _, ok := got["feedbackJunkCount"]; ok {
		t.Errorf("junkCount should not be incremented for valid feedback")
	}
}

func TestApplyTrustDeltaCounters_JunkIncrements(t *testing.T) {
	got := computeCounterIncrements(false)
	if got["feedbackTotalCount"] != int64(1) {
		t.Errorf("totalCount inc=%v want 1", got["feedbackTotalCount"])
	}
	if got["feedbackJunkCount"] != int64(1) {
		t.Errorf("junkCount inc=%v want 1", got["feedbackJunkCount"])
	}
	if _, ok := got["feedbackValidCount"]; ok {
		t.Errorf("validCount should not be incremented for junk feedback")
	}
}
