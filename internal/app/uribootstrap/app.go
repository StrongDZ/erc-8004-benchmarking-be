package uribootstrap

// app.go — URI resolver (Stream 1).
// Cron daemon: per-chain sequential scan of decoded events, publishes any URI
// found (identity or reputation) to uri.{chainID}.event_uri queue, then advances
// uri_scanned_X (producer scan position). TrustRank bounds on-chain processing
// using uri_fetched_X, updated by EventURIConsumer after offchain_data reaches
// a terminal fetch status.
//
// Actual HTTP/IPFS fetching is handled by the separate EventURIConsumer started
// alongside this app in cmd/workers/uri-bootstrap.

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	"erc-8004-benchmarking-be/internal/mq"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
)

// URICursorKey returns the config key for the per-chain producer scan cursor
// (how far decoded events have been scanned for URI queue messages).
func URICursorKey(chainID int64) string {
	return fmt.Sprintf("uri_scanned_%d", chainID)
}

// URIFetchedCursorKey returns the config key for the consumer-driven cursor:
// max event position whose URI has been resolved to a terminal offchain_data status.
func URIFetchedCursorKey(chainID int64) string {
	return fmt.Sprintf("uri_fetched_%d", chainID)
}

// App runs the URI resolver cron: scan events per chain, publish URIs to queue, save cursor.
type App struct {
	contracts    *contractsrepo.ContractsRepository
	configRepo   *configrepo.ConfigRepository
	eventsRepo   *eventrepo.Repository
	uriPublisher mq.Publisher
	batchSize    int64
	interval     time.Duration

	// per-chain consumer management
	consumersMu   sync.Mutex
	startedChains map[int64]struct{}
	startConsumer func(ctx context.Context, chainID int64) // injected by cmd/workers/uri-bootstrap
}

// NewApp returns a ready App.
func NewApp(
	contracts *contractsrepo.ContractsRepository,
	configRepo *configrepo.ConfigRepository,
	eventsRepo *eventrepo.Repository,
	uriPublisher mq.Publisher,
	batchSize int64,
	interval time.Duration,
	startConsumer func(ctx context.Context, chainID int64),
) *App {
	if batchSize < 1 {
		batchSize = 500
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &App{
		contracts:     contracts,
		configRepo:    configRepo,
		eventsRepo:    eventsRepo,
		uriPublisher:  uriPublisher,
		batchSize:     batchSize,
		interval:      interval,
		startedChains: make(map[int64]struct{}),
		startConsumer: startConsumer,
	}
}

// Run blocks until ctx is cancelled, running URI publication cycles on a timer.
func (a *App) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	log.Printf("uri_resolver: started interval=%s batch=%d", a.interval, a.batchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.runCycle(ctx); err != nil {
				log.Printf("uri_resolver: cycle error: %v", err)
			}
		}
	}
}

func (a *App) runCycle(ctx context.Context) error {
	chains, err := a.contracts.FindActive(ctx)
	if err != nil {
		return fmt.Errorf("uri_resolver: find active chains: %w", err)
	}

	// Start consumers for any newly discovered chains.
	if a.startConsumer != nil {
		a.consumersMu.Lock()
		for _, c := range chains {
			if _, ok := a.startedChains[c.ChainID]; !ok {
				a.startedChains[c.ChainID] = struct{}{}
				go a.startConsumer(ctx, c.ChainID)
			}
		}
		a.consumersMu.Unlock()
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, c := range chains {
		chainID := c.ChainID
		g.Go(func() error {
			return a.runChain(gctx, chainID)
		})
	}
	return g.Wait()
}

func (a *App) runChain(ctx context.Context, chainID int64) error {
	cursor := a.loadCursor(ctx, chainID)
	var total int

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		events, err := a.eventsRepo.ListByChainAscending(ctx, chainID, cursor, a.batchSize)
		if err != nil {
			return fmt.Errorf("uri_resolver: chain=%d list events: %w", chainID, err)
		}
		if len(events) == 0 {
			if total > 0 {
				log.Printf("uri_resolver: chain=%d pass done events=%d", chainID, total)
			}
			return nil
		}

		for _, ev := range events {
			if err := ctx.Err(); err != nil {
				return err
			}
			a.publishEventURI(ctx, ev)
		}

		total += len(events)
		last := events[len(events)-1]
		cursor = &eventrepo.ChainEventCursor{BlockNumber: last.BlockNumber, LogIndex: last.LogIndex}

		a.saveCursor(ctx, chainID, *cursor)
	}
}

// publishEventURI extracts any URI from the event and publishes it to the event_uri queue.
// The consumer upserts offchain_data idempotently whether or not the URI was cached.
func (a *App) publishEventURI(ctx context.Context, ev eventrepo.DecodedEvent) {
	domainEv := domainuri.Event{
		EventName:    ev.EventName,
		Args:         ev.Args,
		ChainID:      ev.ChainID,
		TxHash:       ev.TxHash,
		BlockNumber:  ev.BlockNumber,
		LogIndex:     ev.LogIndex,
		DecodedAt:    ev.DecodedAt,
		ContractType: ev.ContractType,
	}

	uri, ok := domainuri.ExtractURIFromEvent(domainEv)
	if !ok || uri == "" {
		return
	}

	if a.uriPublisher == nil {
		return
	}

	msg := mq.AgentURIMessage{
		URI:          uri,
		ChainID:      ev.ChainID,
		EventID:      ev.TxHash, // best-effort; full EventDocumentID not needed here
		BlockNumber:  ev.BlockNumber,
		LogIndex:     ev.LogIndex,
		DecodedAt:    ev.DecodedAt,
		EventName:    ev.EventName,
		ContractType: ev.ContractType,
		Source:       "bootstrap",
	}

	queueName := mq.EventURIQueueName(ev.ChainID)
	if err := a.uriPublisher.Publish(ctx, queueName, msg); err != nil {
		log.Printf("uri_resolver: publish to %s chain=%d tx=%s: %v", queueName, ev.ChainID, ev.TxHash, err)
	}
}

func (a *App) loadCursor(ctx context.Context, chainID int64) *eventrepo.ChainEventCursor {
	entry, err := a.configRepo.Get(ctx, URICursorKey(chainID))
	if err != nil {
		return nil
	}
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

func (a *App) saveCursor(ctx context.Context, chainID int64, cursor eventrepo.ChainEventCursor) {
	key := URICursorKey(chainID)
	metadata := map[string]any{
		"blockNumber": cursor.BlockNumber,
		"logIndex":    cursor.LogIndex,
	}

	inserted, err := a.configRepo.SetIfAbsent(ctx, key, metadata)
	if err != nil {
		log.Printf("uri_resolver: save cursor chain=%d: %v", chainID, err)
		return
	}
	if !inserted {
		if err := a.configRepo.UpdateMetadata(ctx, key, metadata); err != nil {
			log.Printf("uri_resolver: update cursor chain=%d: %v", chainID, err)
		}
	}
}
