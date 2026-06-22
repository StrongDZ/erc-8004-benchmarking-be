package extenrich

// consumer_wallet.go — RabbitMQ consumer for erc8004.wallet_enrich. Each
// message names a single newly-discovered wallet (published by UpsertCold /
// ReconcileOwnership when wasNew==true). Messages are micro-batched for a
// cache-first cheap RPC pass (balance+nonce), then rich/ENS work is enqueued
// onto the rate-limited explorer/ENS worker pools.
//
// Errors are logged only — the message is always acked. backlog.go's
// periodic sweep is the source of truth and retries any wallet still missing
// enrichment on its next pass.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

const (
	walletEnrichConsumerTag  = "wallet-enrich-consumer"
	cheapBatchFlushInterval  = 200 * time.Millisecond
)

type cheapBatchEntry struct {
	msg      mq.WalletEnrichMessage
	delivery amqp.Delivery
}

// cheapBatcher micro-batches queue deliveries for the batched cheap RPC pass.
type cheapBatcher struct {
	app     *App
	maxSize int
	maxWait time.Duration

	mu      sync.Mutex
	entries []cheapBatchEntry
	timer   *time.Timer
	flushC  chan struct{}
}

func newCheapBatcher(app *App, maxSize int, maxWait time.Duration) *cheapBatcher {
	return &cheapBatcher{
		app:     app,
		maxSize: maxSize,
		maxWait: maxWait,
		flushC:  make(chan struct{}, 1),
	}
}

func (b *cheapBatcher) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			b.flush(flushCtx)
			cancel()
			return
		case <-b.flushC:
			b.flush(ctx)
		}
	}
}

func (b *cheapBatcher) submit(ctx context.Context, msg mq.WalletEnrichMessage, d amqp.Delivery) {
	w, err := b.app.wallets.GetByAddress(ctx, msg.ChainID, msg.Address)
	if err != nil {
		log.Printf("extenrich: wallet_enrich %d:%s: fetch: %v", msg.ChainID, msg.Address, err)
		_ = d.Ack(false)
		return
	}

	if w.External.CheapFetched {
		b.app.enqueueDownstream(*w)
		_ = d.Ack(false)
		return
	}

	if updated, ok, err := b.app.applyCheapCacheHit(ctx, *w); err != nil {
		log.Printf("extenrich: wallet_enrich %d:%s: cache hit write: %v", msg.ChainID, msg.Address, err)
		_ = d.Ack(false)
		return
	} else if ok {
		b.app.enqueueDownstream(updated)
		_ = d.Ack(false)
		return
	}

	b.mu.Lock()
	b.entries = append(b.entries, cheapBatchEntry{msg: msg, delivery: d})
	shouldFlush := len(b.entries) >= b.maxSize
	if len(b.entries) == 1 {
		b.scheduleFlush()
	}
	b.mu.Unlock()

	if shouldFlush {
		b.signalFlush()
	}
}

func (b *cheapBatcher) scheduleFlush() {
	if b.timer != nil {
		return
	}
	b.timer = time.AfterFunc(b.maxWait, func() {
		b.signalFlush()
	})
}

func (b *cheapBatcher) signalFlush() {
	select {
	case b.flushC <- struct{}{}:
	default:
	}
}

func (b *cheapBatcher) flush(ctx context.Context) {
	b.mu.Lock()
	entries := b.entries
	b.entries = nil
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	byChain := make(map[int64][]cheapBatchEntry)
	for _, e := range entries {
		byChain[e.msg.ChainID] = append(byChain[e.msg.ChainID], e)
	}

	for chainID, chainEntries := range byChain {
		b.flushChain(ctx, chainID, chainEntries)
	}
}

func (b *cheapBatcher) flushChain(ctx context.Context, chainID int64, entries []cheapBatchEntry) {
	rpcs := b.app.rpcByChain[chainID]
	if len(rpcs) == 0 {
		log.Printf("extenrich: wallet_enrich chain=%d: no rpcs configured, skip %d wallet(s)", chainID, len(entries))
		for _, e := range entries {
			_ = e.delivery.Ack(false)
		}
		return
	}

	var wallets []wallet.WalletDocument
	var pending []cheapBatchEntry
	for _, e := range entries {
		w, err := b.app.wallets.GetByAddress(ctx, e.msg.ChainID, e.msg.Address)
		if err != nil {
			log.Printf("extenrich: wallet_enrich %d:%s: re-fetch: %v", e.msg.ChainID, e.msg.Address, err)
			_ = e.delivery.Ack(false)
			continue
		}
		if w.External.CheapFetched {
			b.app.enqueueDownstream(*w)
			_ = e.delivery.Ack(false)
			continue
		}
		wallets = append(wallets, *w)
		pending = append(pending, e)
	}

	if len(wallets) > 0 {
		if err := b.app.cheapPassChain(ctx, chainID, rpcs, wallets); err != nil {
			log.Printf("extenrich: wallet_enrich chain=%d: cheap batch: %v", chainID, err)
		}
	}

	for _, e := range pending {
		w, err := b.app.wallets.GetByAddress(ctx, e.msg.ChainID, e.msg.Address)
		if err != nil {
			log.Printf("extenrich: wallet_enrich %d:%s: post-cheap fetch: %v", e.msg.ChainID, e.msg.Address, err)
		} else if w.External.CheapFetched {
			b.app.enqueueDownstream(*w)
		}
		_ = e.delivery.Ack(false)
	}
}

// applyCheapCacheHit writes a cache hit to analyzed_agents and returns the
// updated wallet when the permanent cache already has cheap data.
func (a *App) applyCheapCacheHit(ctx context.Context, w wallet.WalletDocument) (wallet.WalletDocument, bool, error) {
	cached := a.lookupCache(ctx, []string{w.ID})
	upd, ok := cacheHitUpdate(w.ID, cached)
	if !ok {
		return w, false, nil
	}
	if err := a.wallets.BulkSetExternal(ctx, []wallet.ExternalUpdate{upd}); err != nil {
		return w, false, err
	}
	w.External = upd.Doc
	return w, true, nil
}

// runConsumer consumes erc8004.wallet_enrich until ctx is cancelled.
func (a *App) runConsumer(ctx context.Context) error {
	ch, err := a.conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueWalletEnrich, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.Qos(a.prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(mq.QueueWalletEnrich, walletEnrichConsumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	batcher := newCheapBatcher(a, cheapBatchSize, cheapBatchFlushInterval)
	go batcher.run(ctx)

	return rmqinfra.GracefulConsumeLoop(ctx, ch, walletEnrichConsumerTag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(),
		func(hctx context.Context, d amqp.Delivery) error {
			var msg mq.WalletEnrichMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("extenrich: discard malformed message: %v", err)
				_ = d.Ack(false)
				return nil
			}
			batcher.submit(hctx, msg, d)
			return nil
		},
	)
}
