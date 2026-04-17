package uri_bootstrap

// app.go — URI resolver (Stream 1).
// Cron daemon: per-chain sequential scan of decoded events, fetches any URI
// found (identity or reputation), persists to offchain_data, and advances
// a per-chain cursor so that TrustRank (Stream 2) knows which events are safe
// to process (all URIs resolved up to that point).

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/errgroup"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// URICursorKey returns the config key for the per-chain URI resolver cursor.
func URICursorKey(chainID int64) string {
	return fmt.Sprintf("uri_available_%d", chainID)
}

// App runs the URI resolver cron: scan events per chain, fetch URIs, save cursor.
type App struct {
	contracts  *configrepo.ContractsRepository
	configRepo *configrepo.ConfigRepository
	eventsRepo *eventrepo.Repository
	offchain   *offchainrepo.Repository
	uriFetcher domainuri.MetadataFetcher
	batchSize  int64
	interval   time.Duration
}

// NewApp returns a ready App.
func NewApp(
	contracts *configrepo.ContractsRepository,
	configRepo *configrepo.ConfigRepository,
	eventsRepo *eventrepo.Repository,
	offchain *offchainrepo.Repository,
	uriFetcher domainuri.MetadataFetcher,
	batchSize int64,
	interval time.Duration,
) *App {
	if batchSize < 1 {
		batchSize = 500
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &App{
		contracts:  contracts,
		configRepo: configRepo,
		eventsRepo: eventsRepo,
		offchain:   offchain,
		uriFetcher: uriFetcher,
		batchSize:  batchSize,
		interval:   interval,
	}
}

// Run blocks until ctx is cancelled, running URI resolution cycles on a timer.
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
			a.resolveEventURI(ctx, ev)
		}

		total += len(events)
		last := events[len(events)-1]
		cursor = &eventrepo.ChainEventCursor{BlockNumber: last.BlockNumber, LogIndex: last.LogIndex}

		a.saveCursor(ctx, chainID, *cursor)
	}
}

// resolveEventURI extracts any URI from the event and fetches it if not already cached.
func (a *App) resolveEventURI(ctx context.Context, ev eventrepo.DecodedEvent) {
	domainEv := domainuri.Event{
		EventName: ev.EventName,
		Args:      ev.Args,
		ChainID:   ev.ChainID,
		TxHash:    ev.TxHash,
		LogIndex:  ev.LogIndex,
		DecodedAt: ev.DecodedAt,
	}

	uri, ok := domainuri.ExtractURIFromEvent(domainEv)
	if !ok || uri == "" {
		return
	}

	ok, _ = a.offchain.HasSuccessfulFetch(ctx, uri)
	if ok {
		return
	}

	if a.uriFetcher == nil {
		return
	}

	sourceType := domainuri.DetectURIType(uri).String()
	eventType := ev.EventName
	contractType := ev.ContractType

	data, err := a.uriFetcher.FetchMetadata(ctx, uri)
	if err != nil {
		log.Printf("uri_resolver: fetch %s (chain=%d tx=%s): %v", uri, ev.ChainID, ev.TxHash, err)
		_ = a.offchain.UpsertFailure(ctx, uri, sourceType, eventType, contractType, err.Error())
		return
	}

	_ = a.offchain.UpsertSuccess(ctx, uri, string(data), sourceType, eventType, contractType)
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
