package rpc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchBalanceNonce(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// addr[0]: balance id=0 (0x1bc16d674ec80000 = 2e18), nonce id=1 (0x5)
		_, _ = w.Write([]byte(`[
			{"jsonrpc":"2.0","id":0,"result":"0x1bc16d674ec80000"},
			{"jsonrpc":"2.0","id":1,"result":"0x5"}]`))
	}))
	defer srv.Close()

	c := NewClient(srv.Client(), []string{srv.URL})
	out, err := c.FetchBalanceNonce([]string{"0xabc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || !out[0].OK {
		t.Fatalf("want 1 ok result, got %+v", out)
	}
	if out[0].Nonce != 5 {
		t.Fatalf("want nonce 5, got %d", out[0].Nonce)
	}
	if out[0].BalanceWei.String() != "2000000000000000000" {
		t.Fatalf("want 2e18 wei, got %s", out[0].BalanceWei.String())
	}
}
