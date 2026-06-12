package extenrich

// cache.go — pure cache-hit decision helpers for erc8004.wallet_external_cache,
// used by the cache-first RunCheap/RunExplorer flows (wallet-external-cache-multikey design).

import (
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
