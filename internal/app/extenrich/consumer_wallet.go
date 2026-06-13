package extenrich

// consumer_wallet.go — RabbitMQ consumer for erc8004.wallet_enrich. Each
// message names a single newly-discovered wallet (published by UpsertCold /
// ReconcileOwnership when wasNew==true). enrichOneCheap runs the cache-first
// RPC balance+nonce pass for it.
//
// Errors are logged only — the message is always acked. backlog.go's
// periodic sweep is the source of truth and retries any wallet still missing
// external.present on its next pass, so this consumer is purely a latency
// optimization (enrich soon after discovery rather than waiting for the
// next sweep).

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/pkg/retry"
)

const walletEnrichConsumerTag = "wallet-enrich-consumer"

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

	return rmqinfra.GracefulConsumeLoop(ctx, ch, walletEnrichConsumerTag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(),
		func(hctx context.Context, d amqp.Delivery) error {
			var msg mq.WalletEnrichMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("extenrich: discard malformed message: %v", err)
				_ = d.Ack(false)
				return nil
			}
			a.handleWalletEnrich(hctx, msg)
			_ = d.Ack(false)
			return nil
		},
	)
}

// handleWalletEnrich runs the cheap RPC pass, the rich explorer pass, and the ENS pass for one wallet.
// If any of them gets rate limited, it retries 2 times using exponential retry. If it still fails, it stops.
func (a *App) handleWalletEnrich(ctx context.Context, msg mq.WalletEnrichMessage) {
	w, err := a.wallets.GetByAddress(ctx, msg.ChainID, msg.Address)
	if err != nil {
		log.Printf("extenrich: wallet_enrich %d:%s: fetch: %v", msg.ChainID, msg.Address, err)
		return
	}

	// 1. Cheap Signal
	if !w.External.CheapFetched {
		updated, err := retry.Do(ctx, 2, 500*time.Millisecond, retry.BackoffExponential, func() (wallet.WalletDocument, error) {
			res, err := a.FetchCheapSignal(ctx, *w)
			return res, wrapRetryError(err)
		})
		if err != nil {
			log.Printf("extenrich: wallet_enrich %d:%s: cheap pass failed: %v", msg.ChainID, msg.Address, err)
			return
		}
		w = &updated
	}

	// 2. Rich Signal (Explorer)
	if len(a.explorerClients) > 0 && !w.External.RichFetched {
		updated, err := retry.Do(ctx, 2, 500*time.Millisecond, retry.BackoffExponential, func() (wallet.WalletDocument, error) {
			res, err := a.FetchRichSignal(ctx, *w)
			return res, wrapRetryError(err)
		})
		if err != nil {
			log.Printf("extenrich: wallet_enrich %d:%s: rich pass failed: %v", msg.ChainID, msg.Address, err)
		} else {
			w = &updated
		}
	}

	// 3. ENS Signal
	if a.ensClient != nil && !w.External.ENSFetched {
		_, err := retry.Do(ctx, 2, 500*time.Millisecond, retry.BackoffExponential, func() (wallet.WalletDocument, error) {
			res, err := a.FetchENSSignal(ctx, *w)
			return res, wrapRetryError(err)
		})
		if err != nil {
			log.Printf("extenrich: wallet_enrich %d:%s: ens pass failed: %v", msg.ChainID, msg.Address, err)
		}
	}
}

// wrapRetryError wraps non-rate-limit errors as NonRetryableError so we only retry on rate limits.
func wrapRetryError(err error) error {
	if err == nil {
		return nil
	}
	if IsRateLimitError(err) {
		return err
	}
	return retry.NonRetryableError{Err: err}
}

// IsRateLimitError detects if an error represents a rate limit / throttling condition.
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "throttled")
}
