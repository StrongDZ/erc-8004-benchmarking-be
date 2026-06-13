package extenrich

// app.go — Event-driven external wallet trust enrichment daemon.
//
// Three concurrent activities, started by Run():
//  1. Queue consumer (consumer_wallet.go): consumes erc8004.wallet_enrich,
//     published whenever UpsertCold/ReconcileOwnership inserts a brand-new
//     wallet. Runs a cache-first cheap RPC pass (balance+nonce) for that wallet.
//  2. Explorer workers (explorer_workers.go): one goroutine per configured
//     Etherscan API key, draining explorerJobs at a fixed rate.
//  3. Periodic backlog sweep (backlog.go): on a cron schedule (and once at
//     startup), finds wallets still missing cheap enrichment across the fixed
//     MVP chain set, and wallets with cheap-but-incomplete enrichment for the
//     explorer pass.
//
// All writes are cache-first against erc8004.wallet_external_cache (cache.go):
// a wallet already enriched (in this or a prior analyzed_agents lifetime) is
// never re-fetched via RPC/Etherscan.

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// nativeUSD is a static snapshot of native-token USD prices per chain.
// internal/extsource/price.Provider is the seam for a future live source.
var nativeUSD = map[int64]float64{
	1:     3000, // ETH (Ethereum)
	56:    600,  // BNB (BNB Chain)
	8453:  3000, // ETH (Base)
	42220: 0.6,  // CELO (Celo)
	43114: 30,   // AVAX (Avalanche)
}

// explorerJobsBufferSize bounds the explorer work queue. The backlog sweep
// blocks on send when full; the consumer's push is non-blocking and defers
// to the next sweep.
const explorerJobsBufferSize = 1000

// ensJobsBufferSize bounds the ENS work queue, mirroring explorerJobsBufferSize.
const ensJobsBufferSize = 1000

// DaemonConfig holds the queue connection and scheduling settings for Run().
type DaemonConfig struct {
	Conn      *amqp.Connection
	Prefetch  int    // AMQP QoS prefetch for the wallet_enrich consumer
	SweepCron string // cron expression for the periodic backlog sweep
}

type App struct {
	wallets         *wallet.Repository
	cache           *wallet.Repository
	rpcByChain      map[int64][]string
	httpc           *http.Client
	price           PriceProvider
	explorerClients []*ExplorerClient
	workers         int
	rate            float64
	ensClient       *ENSClient
	ensRate         float64
	ensWorkers      int

	conn         *amqp.Connection
	prefetch     int
	sweepCron    string
	explorerJobs chan wallet.WalletDocument
	ensJobs      chan wallet.WalletDocument
}

// New constructs the enrich daemon. explorerClients is empty when
// ETHERSCAN_API_KEYS is unset — the explorer pass becomes a permanent no-op.
// ensClient is nil when the ENS pass is disabled — also a permanent no-op.
func New(ctx context.Context, cfg config.Config, client *mongo.Client, httpc *http.Client, explorerClients []*ExplorerClient, workers int, rate float64, ensClient *ENSClient, ensRate float64, ensWorkers int, daemon DaemonConfig) *App {
	if workers < 1 {
		workers = 1
	}
	if ensWorkers < 1 {
		ensWorkers = 1
	}
	if daemon.Prefetch < 1 {
		daemon.Prefetch = 1
	}
	if daemon.SweepCron == "" {
		daemon.SweepCron = "*/15 * * * *"
	}
	return &App{
		wallets:         wallet.NewRepository(client.Database(cfg.AnalyzedDatabase), cfg.WalletColl),
		cache:           wallet.NewRepository(client.Database(cfg.MongoDatabase), cfg.WalletExternalCacheColl),
		rpcByChain:      loadRPCs(ctx, client, cfg),
		httpc:           httpc,
		price:           NewStaticPriceProvider(nativeUSD),
		explorerClients: explorerClients,
		workers:         workers,
		rate:            rate,
		ensClient:       ensClient,
		ensRate:         ensRate,
		ensWorkers:      ensWorkers,

		conn:         daemon.Conn,
		prefetch:     daemon.Prefetch,
		sweepCron:    daemon.SweepCron,
		explorerJobs: make(chan wallet.WalletDocument, explorerJobsBufferSize),
		ensJobs:      make(chan wallet.WalletDocument, ensJobsBufferSize),
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

// Run starts the queue consumer, explorer workers, and backlog sweep cron,
// and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.runConsumer(ctx); err != nil && ctx.Err() == nil {
			log.Printf("extenrich: consumer stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.runExplorerWorkers(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.runENSWorkers(ctx)
	}()

	c := cron.New()
	if _, err := c.AddFunc(a.sweepCron, func() { a.runSweep(ctx) }); err != nil {
		return fmt.Errorf("extenrich: add sweep cron: %w", err)
	}
	c.Start()
	go a.runSweep(ctx) // run once immediately on startup

	<-ctx.Done()
	<-c.Stop().Done()
	wg.Wait()
	return ctx.Err()
}

// PriceProvider — native-token USD price per chain.
type PriceProvider interface {
	// NativeUSD returns the USD price of the chain's native token; ok=false if unknown.
	NativeUSD(chainID int64) (float64, bool)
}

type staticPriceProvider struct{ m map[int64]float64 }

func NewStaticPriceProvider(m map[int64]float64) PriceProvider { return &staticPriceProvider{m: m} }

func (s *staticPriceProvider) NativeUSD(chainID int64) (float64, bool) {
	v, ok := s.m[chainID]
	return v, ok
}
