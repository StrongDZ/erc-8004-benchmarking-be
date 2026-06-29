package extenrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// FetchCheapSignal fetches balance + nonce from RPC and updates the wallet's external doc.
func (a *App) FetchCheapSignal(ctx context.Context, w wallet.WalletDocument) (wallet.WalletDocument, error) {
	rpcs := a.rpcByChain[w.ChainID]
	if len(rpcs) == 0 {
		return w, fmt.Errorf("no rpcs configured for chain %d", w.ChainID)
	}
	rpcClient := NewRPCClient(a.httpc, rpcs)
	usdPrice, _ := a.price.NativeUSD(w.ChainID)
	now := time.Now().Unix()

	results, err := rpcClient.FetchBalanceNonce([]string{w.Address})
	if err != nil {
		return w, err
	}
	if len(results) == 0 || !results[0].OK {
		return w, fmt.Errorf("rpc fetch failed for %s", w.Address)
	}

	r := results[0]
	balanceUSD := weiToFloat(r.BalanceWei) * usdPrice
	f := extscore.Features{
		BalanceUSD:     balanceUSD,
		Nonce:          r.Nonce,
		BalancePresent: true,
		NoncePresent:   true,
		// ENS is not resolved in the cheap pass; the ENS/rich sweep sets this true on chain 1.
		ENSApplicable:  false,
	}

	doc := w.External
	doc.Score = extscore.Score(f)
	doc.Complete = extscore.CompleteForChain(w.ChainID, f)
	doc.Present = true
	doc.BalanceUSD = balanceUSD
	doc.Nonce = r.Nonce
	doc.CheapAt = now
	doc.CheapFetched = true

	update := wallet.ExternalUpdate{ID: w.ID, Doc: doc}
	if err := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err != nil {
		return w, fmt.Errorf("write through cheap signal: %w", err)
	}

	w.External = doc
	return w, nil
}

// RPCClient handles batched JSON-RPC queries for balance and transaction count.
type RPCClient struct {
	httpc *http.Client
	rpcs  []string
}

func NewRPCClient(httpc *http.Client, rpcs []string) *RPCClient {
	return &RPCClient{httpc: httpc, rpcs: rpcs}
}

type RPCResult struct {
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
func (c *RPCClient) FetchBalanceNonce(addrs []string) ([]RPCResult, error) {
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
		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("rpc rate limit: status code 429")
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
		results := make([]RPCResult, len(addrs))
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
			results[i] = RPCResult{BalanceWei: b, Nonce: n.Uint64(), OK: true}
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
