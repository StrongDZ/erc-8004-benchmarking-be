package rpc

// client.go — batched JSON-RPC client for the two cheap external features:
// native balance (eth_getBalance) and outbound tx count (eth_getTransactionCount).
// Rotates through the provided RPC URLs on transport/decode error.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
)

type HTTPDoer interface {
	Post(url, contentType string, body *bytes.Reader) (*http.Response, error)
}

type Client struct {
	httpc *http.Client
	rpcs  []string
}

func NewClient(httpc *http.Client, rpcs []string) *Client {
	return &Client{httpc: httpc, rpcs: rpcs}
}

type Result struct {
	BalanceWei *big.Int
	Nonce      uint64
	OK         bool
}

type rpcReq struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcResp struct {
	ID     int             `json:"id"`
	Result string          `json:"result"`
	Error  json.RawMessage `json:"error"`
}

// FetchBalanceNonce sends one batched request: per addr an eth_getBalance and an
// eth_getTransactionCount. Returns results index-aligned with addrs.
func (c *Client) FetchBalanceNonce(addrs []string) ([]Result, error) {
	reqs := make([]rpcReq, 0, len(addrs)*2)
	for i, a := range addrs {
		reqs = append(reqs,
			rpcReq{"2.0", i * 2, "eth_getBalance", []interface{}{a, "latest"}},
			rpcReq{"2.0", i*2 + 1, "eth_getTransactionCount", []interface{}{a, "latest"}},
		)
	}
	body, _ := json.Marshal(reqs)

	var lastErr error
	for _, url := range c.rpcs {
		resp, err := c.httpc.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		var out []rpcResp
		derr := json.NewDecoder(resp.Body).Decode(&out)
		_ = resp.Body.Close()
		if derr != nil || len(out) == 0 {
			lastErr = fmt.Errorf("decode: %v", derr)
			continue
		}
		byID := make(map[int]rpcResp, len(out))
		for _, r := range out {
			byID[r.ID] = r
		}
		results := make([]Result, len(addrs))
		for i := range addrs {
			bal, nc := byID[i*2], byID[i*2+1]
			if len(bal.Error) > 0 || len(nc.Error) > 0 || bal.Result == "" || nc.Result == "" {
				continue
			}
			b, ok1 := new(big.Int).SetString(trim0x(bal.Result), 16)
			n, ok2 := new(big.Int).SetString(trim0x(nc.Result), 16)
			if !ok1 || !ok2 {
				continue
			}
			results[i] = Result{BalanceWei: b, Nonce: n.Uint64(), OK: true}
		}
		return results, nil
	}
	return nil, lastErr
}

func trim0x(s string) string {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return s[2:]
	}
	return s
}
