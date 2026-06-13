package extenrich

import (
	"testing"

	"erc-8004-benchmarking-be/internal/repository/wallet"
)

func TestCacheHitUpdate(t *testing.T) {
	cache := map[string]wallet.ExternalDoc{
		"1:0xpresent": {Present: true, Score: 42},
		"1:0xpartial": {Present: false},
	}

	if upd, ok := cacheHitUpdate("1:0xpresent", cache); !ok || upd.Doc.Score != 42 {
		t.Fatalf("want cache hit with score 42, got ok=%v upd=%+v", ok, upd)
	}
	if _, ok := cacheHitUpdate("1:0xpartial", cache); ok {
		t.Fatal("want no hit for Present=false entry")
	}
	if _, ok := cacheHitUpdate("1:0xmissing", cache); ok {
		t.Fatal("want no hit for missing entry")
	}
}

func TestExplorerCacheHit(t *testing.T) {
	cache := map[string]wallet.ExternalDoc{
		"1:0xcomplete":   {Present: true, Complete: true, Score: 77},
		"1:0xincomplete": {Present: true, Complete: false},
	}

	if upd, ok := explorerCacheHit("1:0xcomplete", cache); !ok || upd.Doc.Score != 77 {
		t.Fatalf("want cache hit with score 77, got ok=%v upd=%+v", ok, upd)
	}
	if _, ok := explorerCacheHit("1:0xincomplete", cache); ok {
		t.Fatal("want no hit for Complete=false entry")
	}
	if _, ok := explorerCacheHit("1:0xmissing", cache); ok {
		t.Fatal("want no hit for missing entry")
	}
}
