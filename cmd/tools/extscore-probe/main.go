// extscore-probe — FEASIBILITY SPIKE (not wired into any pipeline).
//
// Goal: verify whether the CHEAP external-trust features (native balance via
// eth_getBalance, outbound tx count via eth_getTransactionCount) can be fetched
// for the full wallet set (~151k) at acceptable throughput, using the same
// public RPCs already stored in erc8004.contracts. It samples N wallets per
// chain, fetches the two features with batched JSON-RPC + bounded concurrency,
// measures wall-clock throughput, extrapolates to the full per-chain counts,
// and prints a cheap-only Score_ext distribution for the sample.
//
// It does NOT compute age / unique-counterparties — those are not obtainable
// from a plain RPC node and need an explorer/indexer (reported, not done here).
//
// Run: go run ./cmd/tools/extscore-probe --per-chain=300 --workers=8 --batch=40
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	perChain := flag.Int("per-chain", 300, "sample size of wallets per chain")
	workers := flag.Int("workers", 8, "concurrent HTTP workers per chain")
	batch := flag.Int("batch", 40, "wallets per batched JSON-RPC request (2 calls each)")
	timeoutS := flag.Int("timeout", 15, "per-request HTTP timeout (seconds)")
	flag.Parse()

	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("extscore-probe: config: %v", err)
	}
	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("extscore-probe: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	rpcByChain := loadRPCs(ctx, client, cfg)
	httpc := &http.Client{Timeout: time.Duration(*timeoutS) * time.Second}

	chains := []int64{1, 56, 8453, 42220, 43114}
	grand := struct{ ok, fail, total int64 }{}
	for _, cid := range chains {
		rpcs := rpcByChain[cid]
		if len(rpcs) == 0 {
			fmt.Printf("chain=%d  SKIP (no rpc in contracts)\n", cid)
			continue
		}
		addrs := sampleWallets(ctx, client, cfg, cid, *perChain)
		if len(addrs) == 0 {
			fmt.Printf("chain=%d  SKIP (no wallets)\n", cid)
			continue
		}
		res := probeChain(httpc, rpcs, addrs, *workers, *batch)
		grand.ok += res.ok
		grand.fail += res.fail
		grand.total += int64(len(addrs))
		printChainReport(cid, res, len(addrs))
	}

	fmt.Printf("\n=== GRAND TOTAL ===\nsampled=%d ok=%d fail=%d okRate=%.1f%%\n",
		grand.total, grand.ok, grand.fail, pct(grand.ok, grand.total))
	fmt.Println("NOTE: age (first-tx) and uniqueCounterparties are NOT fetched —")
	fmt.Println("      a plain RPC cannot serve them; they need an explorer/indexer API.")
}

// ---- config / sampling ----------------------------------------------------

// loadRPCs reads per-chain RPC URL lists from erc8004.contracts.
func loadRPCs(ctx context.Context, client *mongo.Client, cfg config.Config) map[int64][]string {
	coll := client.Database(cfg.MongoDatabase).Collection(cfg.ContractsColl)
	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		log.Fatalf("extscore-probe: load contracts: %v", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	out := make(map[int64][]string)
	for cur.Next(ctx) {
		var d struct {
			ChainID int64    `bson:"chainId"`
			RPCs    []string `bson:"rpcs"`
		}
		if err := cur.Decode(&d); err != nil {
			continue
		}
		out[d.ChainID] = d.RPCs
	}
	return out
}

// sampleWallets draws up to n real wallet addresses for a chain, skipping
// system/burn addresses (long runs of zeros in the _id).
func sampleWallets(ctx context.Context, client *mongo.Client, cfg config.Config, chainID int64, n int) []string {
	coll := client.Database(cfg.AnalyzedDatabase).Collection(cfg.WalletColl)
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{"chainId": chainID, "_id": bson.M{"$not": primitive.Regex{Pattern: "0{30,}", Options: ""}}}}},
		bson.D{{Key: "$sample", Value: bson.M{"size": n}}},
		bson.D{{Key: "$project", Value: bson.M{"_id": 1}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		log.Printf("extscore-probe: sample chain=%d: %v", chainID, err)
		return nil
	}
	defer func() { _ = cur.Close(ctx) }()

	out := make([]string, 0, n)
	for cur.Next(ctx) {
		var d struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&d); err != nil {
			continue
		}
		if i := indexColon(d.ID); i >= 0 {
			out = append(out, d.ID[i+1:])
		}
	}
	return out
}

func indexColon(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

// ---- result types ---------------------------------------------------------

type chainResult struct {
	ok, fail int64
	elapsed  time.Duration
	scores   []float64
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

// ---- RPC probe ------------------------------------------------------------

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

func probeChain(httpc *http.Client, rpcs []string, addrs []string, workers, batch int) chainResult {
	type job struct{ addrs []string }
	jobs := make(chan job)
	var ok, fail int64
	var mu sync.Mutex
	scores := make([]float64, 0, len(addrs))

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				feats, err := fetchBatch(httpc, rpcs, j.addrs)
				if err != nil {
					atomic.AddInt64(&fail, int64(len(j.addrs)))
					continue
				}
				mu.Lock()
				for _, f := range feats {
					if f.okFlag {
						ok++
						scores = append(scores, cheapScore(f.balanceWei, f.nonce))
					} else {
						fail++
					}
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < len(addrs); i += batch {
		end := i + batch
		if end > len(addrs) {
			end = len(addrs)
		}
		jobs <- job{addrs: addrs[i:end]}
	}
	close(jobs)
	wg.Wait()
	return chainResult{ok: ok, fail: fail, elapsed: time.Since(start), scores: scores}
}

type feat struct {
	balanceWei *big.Int
	nonce      uint64
	okFlag     bool
}

// fetchBatch sends one batched JSON-RPC request: for each addr, an
// eth_getBalance and an eth_getTransactionCount. Rotates through rpcs on error.
func fetchBatch(httpc *http.Client, rpcs []string, addrs []string) ([]feat, error) {
	reqs := make([]rpcReq, 0, len(addrs)*2)
	for i, a := range addrs {
		reqs = append(reqs,
			rpcReq{"2.0", i * 2, "eth_getBalance", []interface{}{a, "latest"}},
			rpcReq{"2.0", i*2 + 1, "eth_getTransactionCount", []interface{}{a, "latest"}},
		)
	}
	body, _ := json.Marshal(reqs)

	var lastErr error
	for _, url := range rpcs {
		resp, err := httpc.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		var out []rpcResp
		dec := json.NewDecoder(resp.Body)
		derr := dec.Decode(&out)
		_ = resp.Body.Close()
		if derr != nil || len(out) == 0 {
			lastErr = fmt.Errorf("decode: %v", derr)
			continue
		}
		byID := make(map[int]rpcResp, len(out))
		for _, r := range out {
			byID[r.ID] = r
		}
		feats := make([]feat, len(addrs))
		for i := range addrs {
			bal := byID[i*2]
			nc := byID[i*2+1]
			if len(bal.Error) > 0 || len(nc.Error) > 0 || bal.Result == "" || nc.Result == "" {
				continue
			}
			b, ok1 := new(big.Int).SetString(trim0x(bal.Result), 16)
			n, ok2 := new(big.Int).SetString(trim0x(nc.Result), 16)
			if !ok1 || !ok2 {
				continue
			}
			feats[i] = feat{balanceWei: b, nonce: n.Uint64(), okFlag: true}
		}
		return feats, nil
	}
	return nil, lastErr
}

func trim0x(s string) string {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return s[2:]
	}
	return s
}

// ---- cheap-only score (prototype; native units, no USD price) -------------

// cheapScore blends saturating-log balance (native token) and outbound tx count.
// This is a feasibility placeholder, NOT the final weighting (no age,
// counterparties, ENS, or price normalization).
func cheapScore(balanceWei *big.Int, nonce uint64) float64 {
	ether := weiToFloat(balanceWei)
	fBal := satLog10(ether, 3.0) // saturates ~1000 native
	fTx := satLog10(float64(nonce), 3.0)
	return 100.0 * (0.5*fBal + 0.5*fTx)
}

func satLog10(v, maxLog float64) float64 {
	if v <= 0 {
		return 0
	}
	l := math.Log10(1 + v)
	if l > maxLog {
		return 1
	}
	return l / maxLog
}

func weiToFloat(wei *big.Int) float64 {
	if wei == nil {
		return 0
	}
	f := new(big.Float).SetInt(wei)
	f.Quo(f, big.NewFloat(1e18))
	v, _ := f.Float64()
	return v
}

// ---- reporting ------------------------------------------------------------

func printChainReport(cid int64, r chainResult, sampled int) {
	rate := 0.0
	if r.elapsed > 0 {
		rate = float64(r.ok) / r.elapsed.Seconds()
	}
	fmt.Printf("\n--- chain=%d ---\n", cid)
	fmt.Printf("sampled=%d ok=%d fail=%d okRate=%.1f%% elapsed=%s throughput=%.0f wallets/s\n",
		sampled, r.ok, r.fail, pct(r.ok, int64(sampled)), r.elapsed.Round(time.Millisecond), rate)
	if len(r.scores) > 0 {
		sort.Float64s(r.scores)
		fmt.Printf("Score_ext(cheap) min=%.1f p50=%.1f p90=%.1f max=%.1f\n",
			r.scores[0], quantile(r.scores, 0.5), quantile(r.scores, 0.9), r.scores[len(r.scores)-1])
	}
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}
