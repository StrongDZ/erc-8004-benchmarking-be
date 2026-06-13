package extenrich

// explorer_workers.go — persistent explorer worker pool. One goroutine per
// configured Etherscan API key drains explorerJobs (filled by
// consumer_wallet.go and backlog.go) at a.rate requests/second, fetching
// age + unique-counterparty count via Etherscan and writing through the
// resulting Score_ext to both analyzed_agents.wallets and
// erc8004.wallet_external_cache.

import (
	"context"
	"log"
	"sync"
	"time"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// runExplorerWorkers starts one goroutine per configured Etherscan API key
// and blocks until ctx is cancelled. No-op if no explorer clients are
// configured (ETHERSCAN_API_KEYS unset).
func (a *App) runExplorerWorkers(ctx context.Context) {
	if len(a.explorerClients) == 0 {
		return
	}
	interval := time.Second
	if a.rate > 0 {
		interval = time.Duration(float64(time.Second) / a.rate)
	}

	var wg sync.WaitGroup
	for _, client := range a.explorerClients {
		wg.Add(1)
		go func(client *ExplorerClient) {
			defer wg.Done()
			a.runExplorerWorker(ctx, client, interval)
		}(client)
	}
	wg.Wait()
}

// runExplorerWorker drains explorerJobs at one job per interval until ctx is
// cancelled.
func (a *App) runExplorerWorker(ctx context.Context, client *ExplorerClient, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case w := <-a.explorerJobs:
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			a.enrichOneExplorer(ctx, client, w)
		}
	}
}

// enrichOneExplorer fetches age + unique-counterparty count via Etherscan for
// one wallet, computes Score_ext, and writes through to both
// analyzed_agents.wallets and erc8004.wallet_external_cache.
func (a *App) enrichOneExplorer(ctx context.Context, client *ExplorerClient, w wallet.WalletDocument) {
	feat, err := client.FetchFeatures(w.ChainID, w.Address, time.Now())
	if err != nil {
		if IsExplorerPermanentError(err) {
			a.writeExplorerSkipped(ctx, w, err)
			return
		}
		log.Printf("extenrich: explorer %s: %v", w.ID, err)
		return
	}
	f := extscore.Features{
		BalanceUSD:            w.External.BalanceUSD,
		Nonce:                 w.External.Nonce,
		AgeDays:               feat.AgeDays,
		UniqueCounterparties:  feat.UniqueCounterparties,
		HasENS:                w.External.ENS != "",
		BalancePresent:        true,
		NoncePresent:          true,
		AgePresent:            true,
		CounterpartiesPresent: true,
		ENSApplicable:         w.ChainID == 1,
	}
	doc := w.External
	doc.Score = extscore.Score(f)
	doc.Complete = extscore.Complete(f)
	doc.Present = true
	doc.AgeDays = feat.AgeDays
	doc.Counterparties = feat.UniqueCounterparties
	doc.ExplorerAt = time.Now().Unix()
	doc.RichFetched = true

	update := wallet.ExternalUpdate{
		ID:  w.ID,
		Doc: doc,
	}
	if err := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err != nil {
		log.Printf("extenrich: explorer %s: write: %v", w.ID, err)
		return
	}
	log.Printf("extenrich: explorer %s: enriched (score=%.1f complete=%v)", w.ID, update.Doc.Score, update.Doc.Complete)
}

// writeExplorerSkipped marks a wallet as done with cheap-only enrichment when
// the explorer API permanently cannot serve its chain (e.g. free-tier limit).
// Score is renormalized without age/counterparties; sweep will not retry.
func (a *App) writeExplorerSkipped(ctx context.Context, w wallet.WalletDocument, err error) {
	f := extscore.Features{
		BalanceUSD:     w.External.BalanceUSD,
		Nonce:          w.External.Nonce,
		HasENS:         w.External.ENS != "",
		BalancePresent: true,
		NoncePresent:   true,
		ENSApplicable:  w.ChainID == 1,
	}
	doc := w.External
	doc.Score = extscore.Score(f)
	doc.Complete = false
	doc.Present = true
	doc.ExplorerSkipped = true
	doc.ExplorerAt = time.Now().Unix()
	doc.RichFetched = true

	update := wallet.ExternalUpdate{
		ID:  w.ID,
		Doc: doc,
	}
	if err := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err != nil {
		log.Printf("extenrich: explorer %s: skip write: %v", w.ID, err)
		return
	}
	log.Printf("extenrich: explorer %s: skipped (%v, score=%.1f)", w.ID, err, update.Doc.Score)
}
