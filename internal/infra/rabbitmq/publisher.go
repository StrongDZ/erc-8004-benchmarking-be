package rabbitmq

// publisher.go — RabbitMQ JSON publisher used by indexer/bootstrap services.

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher sends JSON messages to one queue.
type Publisher struct {
	ch    *amqp.Channel
	queue string
}

// NewPublisher opens a channel, declares the durable queue, and returns a ready Publisher.
// Caller must call Close() when done.
func NewPublisher(conn *amqp.Connection, queueName string) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq publisher: open channel: %w", err)
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
		return nil, fmt.Errorf("rabbitmq publisher: declare queue %q: %w", queueName, err)
	}

	return &Publisher{ch: ch, queue: queueName}, nil
}

// Publish serialises msg as JSON and sends it as a persistent AMQP message.
func (p *Publisher) Publish(ctx context.Context, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rabbitmq publish: marshal: %w", err)
	}
	return p.ch.PublishWithContext(ctx,
		"",      // default exchange
		p.queue, // routing key = queue name
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
}

// Close closes the underlying AMQP channel.
func (p *Publisher) Close() { _ = p.ch.Close() }

