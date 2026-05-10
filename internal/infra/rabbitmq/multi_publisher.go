package rabbitmq

// multi_publisher.go — RabbitMQ publisher that routes to arbitrary named queues.
// Queues are declared durable on first use and cached for the lifetime of the publisher.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MultiPublisher publishes JSON messages to arbitrary named queues.
// Queues are declared durable on first publish and cached in-process.
// Safe for concurrent use across goroutines.
type MultiPublisher struct {
	ch     *amqp.Channel
	mu     sync.Mutex
	queues map[string]struct{} // declared queues cache
}

// NewMultiPublisher opens a channel and returns a ready MultiPublisher.
// Caller must call Close() when done.
func NewMultiPublisher(conn *amqp.Connection) (*MultiPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq multi_publisher: open channel: %w", err)
	}
	return &MultiPublisher{
		ch:     ch,
		queues: make(map[string]struct{}),
	}, nil
}

// Publish serialises msg as JSON and sends it to queueName as a persistent AMQP message.
// The queue is declared durable if it has not been seen before in this session.
func (p *MultiPublisher) Publish(ctx context.Context, queueName string, msg any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, declared := p.queues[queueName]; !declared {
		if _, err := p.ch.QueueDeclare(
			queueName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,
		); err != nil {
			return fmt.Errorf("rabbitmq multi_publisher: declare queue %q: %w", queueName, err)
		}
		p.queues[queueName] = struct{}{}
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rabbitmq multi_publisher: marshal: %w", err)
	}

	return p.ch.PublishWithContext(ctx,
		"",        // default exchange
		queueName, // routing key = queue name
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
}

// Close closes the underlying AMQP channel.
func (p *MultiPublisher) Close() { _ = p.ch.Close() }
