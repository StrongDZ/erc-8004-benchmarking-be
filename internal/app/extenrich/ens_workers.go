package extenrich

// ens_workers.go — persistent ENS worker pool. A small number of goroutines
// drain ensJobs (filled by backlog.go) at a.ensRate requests/second per
// worker, fetching the ENS primary name + avatar from ensdata.net and
// writing through the resulting ens/ensAvatar/score to both
// analyzed_agents.wallets and erc8004.wallet_external_cache.

import (
	"context"
	"log"
	"sync"
	"time"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// runENSWorkers starts a.ensWorkers goroutines and blocks until ctx is
// cancelled. No-op if no ENS client is configured.
func (a *App) runENSWorkers(ctx context.Context) {
	if a.ensClient == nil {
		return
	}
	interval := time.Second
	if a.ensRate > 0 {
		interval = time.Duration(float64(time.Second) / a.ensRate)
	}

	var wg sync.WaitGroup
	for i := 0; i < a.ensWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.runENSWorker(ctx, interval)
		}()
	}
	wg.Wait()
}

// runENSWorker drains ensJobs at one job per interval until ctx is cancelled.
func (a *App) runENSWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case w := <-a.ensJobs:
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			a.enrichOneENS(ctx, w)
		}
	}
}

// enrichOneENS fetches the ENS primary name + avatar for one wallet,
// recomputes Score_ext, and writes through to both analyzed_agents.wallets
// and erc8004.wallet_external_cache. On lookup error, nothing is written so
// the next backlog sweep retries.
func (a *App) enrichOneENS(ctx context.Context, w wallet.WalletDocument) {
	res, err := a.ensClient.Lookup(w.Address)
	if err != nil {
		log.Printf("extenrich: ens %s: %v", w.ID, err)
		return
	}

	update := wallet.ExternalENSUpdate{
		ID:         w.ID,
		Score:      extscore.Score(ensFeatures(w, res.ENS)),
		ENS:        res.ENS,
		ENSAvatar:  res.Avatar,
		ENSAt:      time.Now().Unix(),
		ENSFetched: true,
	}
	if err := a.writeThroughENS(ctx, []wallet.ExternalENSUpdate{update}); err != nil {
		log.Printf("extenrich: ens %s: write: %v", w.ID, err)
		return
	}
	log.Printf("extenrich: ens %s: enriched (ens=%q score=%.1f)", w.ID, update.ENS, update.Score)
}

// ensFeatures rebuilds the Features used for the last Score_ext computation
// from a wallet's current external doc, substituting the freshly-resolved
// ENS name. Present/Complete persist the cheap/explorer presence flags, so
// this recomputes Score_ext incrementally without re-fetching RPC/Etherscan
// data.
func ensFeatures(w wallet.WalletDocument, ensName string) extscore.Features {
	// Complete on explorer-unsupported chains means balance+nonce only — not age/counterparties.
	explorerDataPresent := extscore.ExplorerApplicable(w.ChainID) &&
		!w.External.ExplorerSkipped &&
		w.External.Complete
	return extscore.Features{
		BalanceUSD:            w.External.BalanceUSD,
		Nonce:                 w.External.Nonce,
		AgeDays:               w.External.AgeDays,
		UniqueCounterparties:  w.External.Counterparties,
		HasENS:                ensName != "",
		BalancePresent:        w.External.Present,
		NoncePresent:          w.External.Present,
		AgePresent:            explorerDataPresent,
		CounterpartiesPresent: explorerDataPresent,
		ENSApplicable:         w.ChainID == 1,
	}
}
