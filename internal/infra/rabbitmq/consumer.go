package rabbitmq

// consumer.go — RabbitMQ raw-log consumer with JSON decode + ack/nack.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/mq"
)

// Handler processes one decoded RawLogMessage. Return a non-nil error to nack+requeue the message.
type Handler func(msg mq.RawLogMessage) error

// Consumer reads RawLogMessages from the raw-logs queue and dispatches them to a Handler.
type Consumer struct {
	ch       *amqp.Channel
	queue    string
	prefetch int
}

// NewConsumer opens a channel, declares the durable queue, and applies QoS prefetch.
func NewConsumer(conn *amqp.Connection, queueName string, prefetch int) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq consumer: open channel: %w", err)
	}

	if _, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq consumer: declare queue %q: %w", queueName, err)
	}

	if prefetch < 1 {
		prefetch = 1
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("rabbitmq consumer: set qos: %w", err)
	}

	return &Consumer{ch: ch, queue: queueName, prefetch: prefetch}, nil
}

// Consume blocks until ctx is cancelled or the AMQP channel is closed.
// Each delivery is passed to handler; on handler error the message is nack+requeued.
// JSON parse errors are acked (discarded) since they are unrecoverable.
func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	deliveries, err := c.ch.Consume(
		c.queue,
		"",    // consumer tag (auto)
		false, // manual ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq consumer: start consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq consumer: delivery channel closed")
			}

			var msg mq.RawLogMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("rabbitmq consumer: discard malformed message: %v", err)
				_ = d.Ack(false)
				continue
			}

			if err := handler(msg); err != nil {
				log.Printf("rabbitmq consumer: handler error (nack+requeue): %v", err)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

// Close closes the underlying AMQP channel.
func (c *Consumer) Close() { _ = c.ch.Close() }

