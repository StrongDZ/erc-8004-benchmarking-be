package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// DetachedHandlerContext returns a context suitable for finishing in-flight handler work
// after the parent shutdown context is canceled. When parent is still active, it returns
// parent and a no-op cancel. When parent is already done, it returns a fresh timeout context.
func DetachedHandlerContext(parent context.Context, shutdownHandlerTimeout time.Duration) (context.Context, context.CancelFunc) {
	if parent.Err() == nil {
		return parent, func() {}
	}
	if shutdownHandlerTimeout <= 0 {
		shutdownHandlerTimeout = 45 * time.Second
	}
	return context.WithTimeout(context.Background(), shutdownHandlerTimeout)
}

// GracefulConsumeParams configures shutdown draining behavior for GracefulConsumeLoop.
type GracefulConsumeParams struct {
	HandlerTimeout time.Duration // default 45s if 0
	DrainDeadline  time.Duration // default 2m if 0
}

// GracefulConsumeParamsDefaults returns the standard handler timeout and drain deadline used by consumers.
func GracefulConsumeParamsDefaults() GracefulConsumeParams {
	return GracefulConsumeParams{
		HandlerTimeout: 45 * time.Second,
		DrainDeadline:  2 * time.Minute,
	}
}

func normalizeGracefulConsumeParams(p GracefulConsumeParams) GracefulConsumeParams {
	if p.HandlerTimeout <= 0 {
		p.HandlerTimeout = 45 * time.Second
	}
	if p.DrainDeadline <= 0 {
		p.DrainDeadline = 2 * time.Minute
	}
	return p
}

// GracefulConsumeLoop reads deliveries until ctx is canceled, then Basic.Cancel's the consumer,
// and drains in-flight prefetch until DrainDeadline or the deliveries channel closes.
// For each delivery, onDelivery receives hctx from DetachedHandlerContext relative to the runtime ctx,
// so handlers keep a workable deadline during drain while ctx may already be canceled.
func GracefulConsumeLoop(
	ctx context.Context,
	ch *amqp091.Channel,
	consumerTag string,
	deliveries <-chan amqp091.Delivery,
	params GracefulConsumeParams,
	onDelivery func(hctx context.Context, d amqp091.Delivery) error,
) error {
	p := normalizeGracefulConsumeParams(params)

	for {
		select {
		case <-ctx.Done():
			if err := ch.Cancel(consumerTag, false); err != nil {
				log.Printf("rabbitmq graceful consume: cancel consumer (shutdown): %v", err)
			}
			return drainGracefulConsume(ctx, deliveries, p, onDelivery)
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq graceful consume: delivery channel closed")
			}
			hctx, cancel := DetachedHandlerContext(ctx, p.HandlerTimeout)
			_ = onDelivery(hctx, d)
			cancel()
		}
	}
}

func drainGracefulConsume(
	runCtx context.Context,
	deliveries <-chan amqp091.Delivery,
	p GracefulConsumeParams,
	onDelivery func(hctx context.Context, d amqp091.Delivery) error,
) error {
	deadline := time.NewTimer(p.DrainDeadline)
	defer deadline.Stop()

	for {
		select {
		case <-deadline.C:
			return fmt.Errorf("rabbitmq graceful consume: graceful drain exceeded %s", p.DrainDeadline)
		case d, ok := <-deliveries:
			if !ok {
				if err := runCtx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("rabbitmq graceful consume: delivery channel closed during drain")
			}
			hctx, cancel := DetachedHandlerContext(runCtx, p.HandlerTimeout)
			_ = onDelivery(hctx, d)
			cancel()
		}
	}
}
