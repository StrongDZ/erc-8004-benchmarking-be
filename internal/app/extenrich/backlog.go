package extenrich

// backlog.go — periodic backlog sweep (cron-triggered + once at startup).
//
// Cheap sweep: for each chain in the fixed MVP set, find wallets missing
// external.present and run the cache-first batched RPC balance+nonce pass
// (cheapPassChain / cheapPassBatch — same logic as the cheap consumer path in
// consumer_wallet.go, but batched for throughput).
//
// Explorer sweep: across all chains, find wallets with cheap-but-incomplete
// enrichment (external.present=true, external.complete!=true). Cache hits
// are written directly; misses are enqueued onto explorerJobs for
// explorer_workers.go to process.

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// chains is the fixed MVP chain set (mirrors extscore-probe).
var chains = []int64{1, 56, 8453, 42220, 43114}

// cheapBatchSize is the number of wallets per batched JSON-RPC request.
const cheapBatchSize = 40

// runSweep performs one backlog sweep pass: cheap enrichment for wallets
// across the fixed MVP chain set, then explorer job enqueuing for wallets
// with cheap-but-incomplete enrichment. Errors are logged only — sweeps
// retry on the next cron tick.
func (a *App) runSweep(ctx context.Context) {
	if err := a.sweepCheap(ctx); err != nil {
		log.Printf("extenrich: cheap sweep: %v", err)
	}
	if err := a.sweepExplorerBacklog(ctx); err != nil {
		log.Printf("extenrich: explorer sweep: %v", err)
	}
	if err := a.sweepENSBacklog(ctx); err != nil {
		log.Printf("extenrich: ens sweep: %v", err)
	}
}

// sweepCheap finds wallets across the fixed MVP chain set still missing cheap
// enrichment (external.present != true) and runs the cache-first batched RPC
// balance+nonce pass for each chain.
func (a *App) sweepCheap(ctx context.Context) error {
	for _, chainID := range chains {
		rpcs := a.rpcByChain[chainID]
		if len(rpcs) == 0 {
			log.Printf("extenrich: cheap sweep chain=%d: no rpcs configured, skip", chainID)
			continue
		}
		wallets, err := a.wallets.Find(ctx,
			bson.M{"chainId": chainID, "external.cheapFetched": bson.M{"$ne": true}},
			options.Find().SetProjection(bson.M{"_id": 1, "address": 1, "chainId": 1, "external": 1}).SetBatchSize(10000),
		)
		if err != nil {
			return fmt.Errorf("chain=%d: scan wallets: %w", chainID, err)
		}
		if len(wallets) == 0 {
			continue
		}
		if err := a.cheapPassChain(ctx, chainID, rpcs, wallets); err != nil {
			return fmt.Errorf("chain=%d: %w", chainID, err)
		}
		log.Printf("extenrich: cheap sweep chain=%d: enriched %d wallet(s)", chainID, len(wallets))
	}
	return nil
}

// cheapPassChain runs the cache-first batched RPC balance+nonce pass for one
// chain's backlog of wallets, spread across a.workers goroutines.
func (a *App) cheapPassChain(ctx context.Context, chainID int64, rpcs []string, wallets []wallet.WalletDocument) error {
	ids := make([]string, len(wallets))
	for i, w := range wallets {
		ids[i] = w.ID
	}
	cached := a.lookupCache(ctx, ids)

	var hits []wallet.ExternalUpdate
	var misses []wallet.WalletDocument
	for _, w := range wallets {
		if upd, ok := cacheHitUpdate(w.ID, cached); ok {
			hits = append(hits, upd)
			continue
		}
		misses = append(misses, w)
	}
	if len(hits) > 0 {
		if err := a.wallets.BulkSetExternal(ctx, hits); err != nil {
			return fmt.Errorf("write cache hits: %w", err)
		}
		log.Printf("extenrich: cheap sweep chain=%d: %d wallet(s) from cache", chainID, len(hits))
	}
	if len(misses) == 0 {
		return nil
	}

	rpcClient := NewRPCClient(a.httpc, rpcs)
	usdPrice, _ := a.price.NativeUSD(chainID)
	now := time.Now().Unix()
	ensApplicable := chainID == 1

	jobs := make(chan []wallet.WalletDocument)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := 0; i < a.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				if err := a.cheapPassBatch(ctx, rpcClient, usdPrice, ensApplicable, now, batch); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < len(misses); i += cheapBatchSize {
		end := i + cheapBatchSize
		if end > len(misses) {
			end = len(misses)
		}
		jobs <- misses[i:end]
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// cheapPassBatch fetches balance+nonce for one batch of wallets via a single
// batched JSON-RPC request and writes through any successful results.
func (a *App) cheapPassBatch(ctx context.Context, rpcClient *RPCClient, usdPrice float64, ensApplicable bool, now int64, batch []wallet.WalletDocument) error {
	addrs := make([]string, len(batch))
	for i, w := range batch {
		addrs[i] = w.Address
	}
	results, err := rpcClient.FetchBalanceNonce(addrs)
	if err != nil {
		return err
	}

	updates := make([]wallet.ExternalUpdate, 0, len(batch))
	for i, w := range batch {
		r := results[i]
		if !r.OK {
			continue
		}
		balanceUSD := weiToFloat(r.BalanceWei) * usdPrice
		f := extscore.Features{
			BalanceUSD:     balanceUSD,
			Nonce:          r.Nonce,
			BalancePresent: true,
			NoncePresent:   true,
			ENSApplicable:  ensApplicable,
		}
		doc := w.External
		doc.Score = extscore.Score(f)
		doc.Complete = extscore.CompleteForChain(w.ChainID, f)
		doc.Present = true
		doc.BalanceUSD = balanceUSD
		doc.Nonce = r.Nonce
		doc.CheapAt = now
		doc.CheapFetched = true
		if !extscore.ExplorerApplicable(w.ChainID) {
			doc.ExplorerSkipped = true
		}
		updates = append(updates, wallet.ExternalUpdate{ID: w.ID, Doc: doc})
	}
	if len(updates) == 0 {
		return nil
	}
	return a.writeThrough(ctx, updates)
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

// sweepExplorerBacklog finds wallets with cheap-but-incomplete enrichment
// (external.present=true, external.complete!=true), applies any cache hits
// directly, and enqueues the remainder onto explorerJobs for
// explorer_workers.go. No-op if no explorer clients are configured
// (ETHERSCAN_API_KEYS unset).
func (a *App) sweepExplorerBacklog(ctx context.Context) error {
	if len(a.explorerClients) == 0 {
		return nil
	}
	wallets, err := a.wallets.Find(ctx,
		bson.M{
			"external.cheapFetched":    true,
			"external.richFetched":     bson.M{"$ne": true},
			"external.explorerSkipped": bson.M{"$ne": true},
		},
		options.Find().SetProjection(bson.M{"_id": 1, "address": 1, "chainId": 1, "external": 1}).SetBatchSize(10000),
	)
	if err != nil {
		return fmt.Errorf("scan wallets: %w", err)
	}
	if len(wallets) == 0 {
		return nil
	}

	ids := make([]string, len(wallets))
	for i, w := range wallets {
		ids[i] = w.ID
	}
	cached := a.lookupCache(ctx, ids)

	var hits []wallet.ExternalUpdate
	var misses []wallet.WalletDocument
	for _, w := range wallets {
		if upd, ok := explorerCacheHit(w.ID, cached); ok {
			hits = append(hits, upd)
			continue
		}
		misses = append(misses, w)
	}
	if len(hits) > 0 {
		if err := a.wallets.BulkSetExternal(ctx, hits); err != nil {
			return fmt.Errorf("write cache hits: %w", err)
		}
		log.Printf("extenrich: explorer sweep: %d wallet(s) from cache", len(hits))
	}
	if len(misses) == 0 {
		return nil
	}

	for _, w := range misses {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a.explorerJobs <- w:
		}
	}
	log.Printf("extenrich: explorer sweep: enqueued %d wallet(s)", len(misses))
	return nil
}

// sweepENSBacklog finds wallets still missing an ENS lookup
// (external.present=true, external.ensAt unset), applies any cache hits
// directly, and enqueues the remainder onto ensJobs for ens_workers.go.
// No-op if no ENS client is configured (the ENS pass is a permanent no-op).
func (a *App) sweepENSBacklog(ctx context.Context) error {
	if a.ensClient == nil {
		return nil
	}
	wallets, err := a.wallets.Find(ctx,
		bson.M{"external.cheapFetched": true, "external.ensFetched": bson.M{"$ne": true}},
		options.Find().SetProjection(bson.M{"_id": 1, "address": 1, "chainId": 1, "external": 1}).SetBatchSize(10000),
	)
	if err != nil {
		return fmt.Errorf("scan wallets: %w", err)
	}
	if len(wallets) == 0 {
		return nil
	}

	ids := make([]string, len(wallets))
	for i, w := range wallets {
		ids[i] = w.ID
	}
	cached := a.lookupCache(ctx, ids)

	var hits []wallet.ExternalENSUpdate
	var misses []wallet.WalletDocument
	for _, w := range wallets {
		if upd, ok := ensCacheHit(w, cached); ok {
			hits = append(hits, upd)
			continue
		}
		misses = append(misses, w)
	}
	if len(hits) > 0 {
		if err := a.wallets.BulkSetExternalENS(ctx, hits); err != nil {
			return fmt.Errorf("write cache hits: %w", err)
		}
		log.Printf("extenrich: ens sweep: %d wallet(s) from cache", len(hits))
	}
	if len(misses) == 0 {
		return nil
	}

	for _, w := range misses {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a.ensJobs <- w:
		}
	}
	log.Printf("extenrich: ens sweep: enqueued %d wallet(s)", len(misses))
	return nil
}
