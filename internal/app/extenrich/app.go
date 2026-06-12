package extenrich

// app.go — orchestrates external on-chain wallet enrichment for the
// extscore teleport blend (external-wallet-trust plan, Task 6).
//
// RunCheap: batched RPC balance+nonce (no API key required) — writes a
// partial Score_ext (Age/Counterparties absent, Complete=false).
//
// RunExplorer: Etherscan V2 txlist age+counterparties, gated on
// ETHERSCAN_API_KEYS — fills the remaining features and sets Complete=true.
//
// Both passes are cache-first against erc8004.wallet_external_cache: a
// wallet already enriched (in this or a prior analyzed_agents lifetime) is
// never re-fetched via RPC/Etherscan (see cache.go).

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/extsource/explorer"
	"erc-8004-benchmarking-be/internal/extsource/price"
	"erc-8004-benchmarking-be/internal/extsource/rpc"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// chains is the fixed MVP chain set (mirrors extscore-probe).
var chains = []int64{1, 56, 8453, 42220, 43114}

// nativeUSD is a static snapshot of native-token USD prices per chain.
// internal/extsource/price.Provider is the seam for a future live source.
var nativeUSD = map[int64]float64{
	1:     3000, // ETH (Ethereum)
	56:    600,  // BNB (BNB Chain)
	8453:  3000, // ETH (Base)
	42220: 0.6,  // CELO (Celo)
	43114: 30,   // AVAX (Avalanche)
}

// cheapBatchSize is the number of wallets per batched JSON-RPC request.
const cheapBatchSize = 40

type App struct {
	wallets         *wallet.Repository
	cache           *wallet.Repository
	rpcByChain      map[int64][]string
	httpc           *http.Client
	price           price.Provider
	explorerClients []*explorer.Client
	workers         int
	rate            float64
}

// New constructs the enrich orchestrator. explorerClients is empty when
// ETHERSCAN_API_KEYS is unset — RunExplorer becomes a no-op. One worker
// goroutine is spawned per client during RunExplorer.
func New(ctx context.Context, cfg config.Config, client *mongo.Client, httpc *http.Client, explorerClients []*explorer.Client, workers int, rate float64) *App {
	if workers < 1 {
		workers = 1
	}
	return &App{
		wallets:         wallet.NewRepository(client.Database(cfg.AnalyzedDatabase), cfg.WalletColl),
		cache:           wallet.NewRepository(client.Database(cfg.MongoDatabase), cfg.WalletExternalCacheColl),
		rpcByChain:      loadRPCs(ctx, client, cfg),
		httpc:           httpc,
		price:           price.NewStatic(nativeUSD),
		explorerClients: explorerClients,
		workers:         workers,
		rate:            rate,
	}
}

// loadRPCs reads per-chain RPC URL lists from {MongoDatabase}.{ContractsColl}.
func loadRPCs(ctx context.Context, client *mongo.Client, cfg config.Config) map[int64][]string {
	coll := client.Database(cfg.MongoDatabase).Collection(cfg.ContractsColl)
	cur, err := coll.Find(ctx, bson.M{})
	if err != nil {
		log.Printf("extenrich: load rpcs: %v", err)
		return nil
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

// RunCheap fetches balance + nonce via RPC for every wallet missing external
// enrichment and writes a partial Score_ext (Age/Counterparties absent).
func (a *App) RunCheap(ctx context.Context) error {
	for _, chainID := range chains {
		rpcs := a.rpcByChain[chainID]
		if len(rpcs) == 0 {
			log.Printf("extenrich: cheap chain=%d: no rpcs configured, skip", chainID)
			continue
		}
		wallets, err := a.wallets.Find(ctx,
			bson.M{"chainId": chainID, "external.present": bson.M{"$ne": true}},
			options.Find().SetProjection(bson.M{"_id": 1, "address": 1, "chainId": 1}).SetBatchSize(10000),
		)
		if err != nil {
			return fmt.Errorf("extenrich: cheap chain=%d: scan wallets: %w", chainID, err)
		}
		if len(wallets) == 0 {
			continue
		}
		if err := a.cheapPassChain(ctx, chainID, rpcs, wallets); err != nil {
			return fmt.Errorf("extenrich: cheap chain=%d: %w", chainID, err)
		}
		log.Printf("extenrich: cheap chain=%d: enriched %d wallet(s)", chainID, len(wallets))
	}
	return nil
}

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
			return fmt.Errorf("extenrich: cheap chain=%d: write cache hits: %w", chainID, err)
		}
		log.Printf("extenrich: cheap chain=%d: %d wallet(s) from cache", chainID, len(hits))
	}
	if len(misses) == 0 {
		return nil
	}

	rpcClient := rpc.NewClient(a.httpc, rpcs)
	usdPrice, _ := a.price.NativeUSD(chainID)
	now := time.Now().Unix()
	ensApplicable := chainID == 1

	jobs := make(chan []wallet.WalletDocument)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for w := 0; w < a.workers; w++ {
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

func (a *App) cheapPassBatch(ctx context.Context, rpcClient *rpc.Client, usdPrice float64, ensApplicable bool, now int64, batch []wallet.WalletDocument) error {
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
		updates = append(updates, wallet.ExternalUpdate{
			ID: w.ID,
			Doc: wallet.ExternalDoc{
				Score:      extscore.Score(f),
				Complete:   false,
				Present:    true,
				BalanceUSD: balanceUSD,
				Nonce:      r.Nonce,
				CheapAt:    now,
			},
		})
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

// RunExplorer fetches age + unique-counterparty count via Etherscan for
// wallets that have cheap enrichment but are not yet Complete. No-op if no
// explorer clients were configured (ETHERSCAN_API_KEYS unset). Wallets cached
// from a prior run (Complete=true in erc8004.wallet_external_cache) are
// repopulated without an Etherscan call. One worker goroutine runs per
// configured API key, each independently rate-limited at a.rate req/s, so
// total throughput scales linearly with the number of keys. Wallets that
// finish this pass are marked Complete and drop out of future scans —
// re-running RunExplorer naturally resumes with the remaining wallets.
func (a *App) RunExplorer(ctx context.Context) error {
	if len(a.explorerClients) == 0 {
		log.Print("extenrich: explorer pass skipped (no ETHERSCAN_API_KEYS)")
		return nil
	}
	wallets, err := a.wallets.Find(ctx,
		bson.M{"external.present": true, "external.complete": bson.M{"$ne": true}},
		options.Find().SetProjection(bson.M{"_id": 1, "address": 1, "chainId": 1, "external": 1}).SetBatchSize(10000),
	)
	if err != nil {
		return fmt.Errorf("extenrich: explorer: scan wallets: %w", err)
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
			return fmt.Errorf("extenrich: explorer: write cache hits: %w", err)
		}
		log.Printf("extenrich: explorer: %d wallet(s) from cache", len(hits))
	}
	if len(misses) == 0 {
		return nil
	}

	interval := time.Second
	if a.rate > 0 {
		interval = time.Duration(float64(time.Second) / a.rate)
	}

	jobs := make(chan wallet.WalletDocument)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	enriched := 0

	for _, client := range a.explorerClients {
		wg.Add(1)
		go func(client *explorer.Client) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for w := range jobs {
				select {
				case <-ctx.Done():
					mu.Lock()
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					mu.Unlock()
					return
				case <-ticker.C:
				}

				feat, err := client.FetchFeatures(w.ChainID, w.Address, time.Now())
				if err != nil {
					log.Printf("extenrich: explorer %s: %v", w.ID, err)
					continue
				}
				f := extscore.Features{
					BalanceUSD:            w.External.BalanceUSD,
					Nonce:                 w.External.Nonce,
					AgeDays:               feat.AgeDays,
					UniqueCounterparties:  feat.UniqueCounterparties,
					HasENS:                w.External.HasENS,
					BalancePresent:        true,
					NoncePresent:          true,
					AgePresent:            true,
					CounterpartiesPresent: true,
					ENSApplicable:         w.ChainID == 1,
				}
				update := wallet.ExternalUpdate{
					ID: w.ID,
					Doc: wallet.ExternalDoc{
						Score:          extscore.Score(f),
						Complete:       extscore.Complete(f),
						Present:        true,
						BalanceUSD:     w.External.BalanceUSD,
						Nonce:          w.External.Nonce,
						AgeDays:        feat.AgeDays,
						Counterparties: feat.UniqueCounterparties,
						HasENS:         w.External.HasENS,
						CheapAt:        w.External.CheapAt,
						ExplorerAt:     time.Now().Unix(),
					},
				}
				if err := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("extenrich: explorer: write %s: %w", w.ID, err)
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				enriched++
				mu.Unlock()
			}
		}(client)
	}

	go func() {
		defer close(jobs)
		for _, w := range misses {
			select {
			case <-ctx.Done():
				return
			case jobs <- w:
			}
		}
	}()

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	log.Printf("extenrich: explorer: enriched %d/%d wallet(s) (%d from cache)", enriched, len(wallets), len(hits))
	return nil
}
