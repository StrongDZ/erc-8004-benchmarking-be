package extenrich

// cache.go — erc8004.wallet_external_cache lookups and write-through for the
// cache-first RunCheap/RunExplorer flows (wallet-external-cache-multikey design).

import (
	"context"
	"log"

	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// cacheHitUpdate returns the cached external doc as a ready-to-apply
// ExternalUpdate when the cache already has cheap (RPC) data for this wallet
// (Present=true), letting RunCheap skip the RPC call entirely.
func cacheHitUpdate(id string, cache map[string]wallet.ExternalDoc) (wallet.ExternalUpdate, bool) {
	doc, ok := cache[id]
	if !ok || !doc.Present {
		return wallet.ExternalUpdate{}, false
	}
	return wallet.ExternalUpdate{ID: id, Doc: doc}, true
}

// explorerCacheHit returns the cached external doc as a ready-to-apply
// ExternalUpdate when the cache already has explorer data for this wallet
// (Complete=true), letting RunExplorer skip the Etherscan call entirely.
func explorerCacheHit(id string, cache map[string]wallet.ExternalDoc) (wallet.ExternalUpdate, bool) {
	doc, ok := cache[id]
	if !ok || !doc.Complete {
		return wallet.ExternalUpdate{}, false
	}
	return wallet.ExternalUpdate{ID: id, Doc: doc}, true
}

// lookupCache fetches cached external docs for the given wallet ids. On error
// (e.g. cache unreachable), logs and returns an empty map so callers fall back
// to RPC/Etherscan — the cache is an optimization, never a hard dependency.
func (a *App) lookupCache(ctx context.Context, ids []string) map[string]wallet.ExternalDoc {
	cached, err := a.cache.FindExternalByIDs(ctx, ids)
	if err != nil {
		log.Printf("extenrich: cache lookup: %v", err)
		return map[string]wallet.ExternalDoc{}
	}
	return cached
}

// writeThrough persists external updates to analyzed_agents.wallets (the
// runtime copy) and erc8004.wallet_external_cache (the permanent cache) so
// future enrichment passes — even after an analyzed_agents reset — can reuse
// this data without another RPC/Etherscan call.
func (a *App) writeThrough(ctx context.Context, updates []wallet.ExternalUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if err := a.wallets.BulkSetExternal(ctx, updates); err != nil {
		return err
	}
	return a.cache.BulkSetExternal(ctx, updates)
}
