package trustrank

// app.go — TrustRank worker (Stream 2).
// Cron: each tick reads active chains, runs one goroutine per chain.
// Per chain: reads events from own checkpoint up to the URI cursor (set by Stream 1),
// processes events in strict chronological order (blockNumber, logIndex).
//
// Also manages per-chain ServiceURIConsumer goroutines started lazily on chain discovery.

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	bootstrapapp "erc-8004-benchmarking-be/internal/app/uribootstrap"
	domaintrustrank "erc-8004-benchmarking-be/internal/domain/trustrank"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
)

// App wraps domain trustrank App with application-level concerns.
type App struct {
	domainApp *domaintrustrank.App

	// per-chain service_uri consumer management
	consumersMu   sync.Mutex
	startedChains map[int64]struct{}
	startConsumer func(ctx context.Context, chainID int64) // injected by cmd/workers/trustrank
}

// NewApp wires repositories and tuning. proc may be nil (noop logger).
// startConsumer is called once per newly discovered chain to launch its ServiceURIConsumer;
// pass nil to disable service URI consuming.
func NewApp(
	contracts *contractsrepo.ContractsRepository,
	events *eventrepo.Repository,
	configRepo *configrepo.ConfigRepository,
	interval time.Duration,
	eventBatchSize int64,
	proc domaintrustrank.EventProcessor,
	startConsumer func(ctx context.Context, chainID int64),
) *App {
	return &App{
		domainApp: &domaintrustrank.App{
			Contracts:  contracts,
			Events:     events,
			ConfigRepo: configRepo,
			Interval:   interval,
			BatchSize:  eventBatchSize,
			Proc:       proc,
		},
		startedChains: make(map[int64]struct{}),
		startConsumer: startConsumer,
	}
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.domainApp.Interval)
	defer ticker.Stop()

	log.Printf("trustrank_worker: interval=%s event_batch=%d", a.domainApp.Interval, a.domainApp.BatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.runCycle(ctx); err != nil {
				log.Printf("trustrank_worker: cycle error: %v", err)
			}
		}
	}
}

func (a *App) runCycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	chains, err := a.domainApp.Contracts.FindActive(ctx)
	if err != nil {
		return fmt.Errorf("trustrank: find active chains: %w", err)
	}
	if len(chains) == 0 {
		log.Printf("trustrank_worker: no active chains")
		return nil
	}

	// Start service_uri consumers for any newly discovered chains.
	if a.startConsumer != nil {
		a.consumersMu.Lock()
		for _, c := range chains {
			if _, ok := a.startedChains[c.ChainID]; !ok {
				a.startedChains[c.ChainID] = struct{}{}
				chainID := c.ChainID
				go a.startConsumer(ctx, chainID)
			}
		}
		a.consumersMu.Unlock()
	}

	var wg sync.WaitGroup
	for _, c := range chains {
		chainID := c.ChainID
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.runChain(ctx, chainID); err != nil {
				log.Printf("trustrank_worker: chain=%d error: %v", chainID, err)
			}
		}()
	}
	wg.Wait()
	return nil
}

func trustrankCheckpointKey(chainID int64) string {
	return fmt.Sprintf("trustrank_worker_%d", chainID)
}

func (a *App) loadCheckpoint(ctx context.Context, chainID int64) *eventrepo.ChainEventCursor {
	if a.domainApp.ConfigRepo == nil {
		return nil
	}
	entry, err := a.domainApp.ConfigRepo.Get(ctx, trustrankCheckpointKey(chainID))
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil
		}
		log.Printf("trustrank: load checkpoint chain=%d: %v", chainID, err)
		return nil
	}
	return parseCursor(entry)
}

func (a *App) saveCheckpoint(ctx context.Context, chainID int64, cursor eventrepo.ChainEventCursor) {
	if a.domainApp.ConfigRepo == nil {
		return
	}
	key := trustrankCheckpointKey(chainID)
	metadata := map[string]any{
		"blockNumber": cursor.BlockNumber,
		"logIndex":    cursor.LogIndex,
	}

	inserted, err := a.domainApp.ConfigRepo.SetIfAbsent(ctx, key, metadata)
	if err != nil {
		log.Printf("trustrank: save checkpoint chain=%d: %v", chainID, err)
		return
	}
	if !inserted {
		if err := a.domainApp.ConfigRepo.UpdateMetadata(ctx, key, metadata); err != nil {
			log.Printf("trustrank: update checkpoint chain=%d: %v", chainID, err)
		}
	}
}

// loadURICursor reads the per-chain cursor set by Stream 1 (URI resolver).
func (a *App) loadURICursor(ctx context.Context, chainID int64) *eventrepo.ChainEventCursor {
	if a.domainApp.ConfigRepo == nil {
		return nil
	}
	entry, err := a.domainApp.ConfigRepo.Get(ctx, bootstrapapp.URICursorKey(chainID))
	if err != nil {
		return nil
	}
	return parseCursor(entry)
}

func (a *App) runChain(ctx context.Context, chainID int64) error {
	after := a.loadCheckpoint(ctx, chainID)
	uriCursor := a.loadURICursor(ctx, chainID)

	if uriCursor == nil {
		return nil
	}

	var total int

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := a.domainApp.Events.ListByChainBounded(ctx, chainID, after, uriCursor, a.domainApp.BatchSize)
		if err != nil {
			return fmt.Errorf("trustrank: chain=%d list events: %w", chainID, err)
		}
		if len(batch) == 0 {
			if total > 0 {
				log.Printf("trustrank_worker: chain=%d pass done events=%d", chainID, total)
			}
			return nil
		}

		if err := a.domainApp.Proc.ProcessBatch(ctx, chainID, batch); err != nil {
			return fmt.Errorf("trustrank: chain=%d batch: %w", chainID, err)
		}

		total += len(batch)
		last := batch[len(batch)-1]
		after = &eventrepo.ChainEventCursor{BlockNumber: last.BlockNumber, LogIndex: last.LogIndex}

		a.saveCheckpoint(ctx, chainID, *after)
	}
}

func parseCursor(entry *configrepo.ConfigEntry) *eventrepo.ChainEventCursor {
	if entry == nil || entry.Metadata == nil {
		return nil
	}

	bn, ok1 := entry.Metadata["blockNumber"]
	li, ok2 := entry.Metadata["logIndex"]
	if !ok1 || !ok2 {
		return nil
	}

	var blockNumber uint64
	var logIndex uint

	switch v := bn.(type) {
	case float64:
		blockNumber = uint64(v)
	case int64:
		blockNumber = uint64(v)
	case int32:
		blockNumber = uint64(v)
	default:
		return nil
	}

	switch v := li.(type) {
	case float64:
		logIndex = uint(v)
	case int64:
		logIndex = uint(v)
	case int32:
		logIndex = uint(v)
	default:
		return nil
	}

	return &eventrepo.ChainEventCursor{BlockNumber: blockNumber, LogIndex: logIndex}
}
