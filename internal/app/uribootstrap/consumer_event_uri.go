package uribootstrap

// consumer_event_uri.go — Consumer for uri.{chainID}.event_uri queues.
//
// Retry policy:
//   - Infrastructure failures (DB write error, context error): nack + requeue.
//   - HTTP/network fetch failure:                              ack + record status=-1.
//   - Fetched but content is not JSON:                        ack + record status=1.
//   - Successful JSON fetch:                                   ack + record status=5.
//
// Fetch failures are NOT retried to avoid blocking the URI cursor for broken links.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	"erc-8004-benchmarking-be/internal/mq"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// EventURIConsumer fetches URIs from the event_uri queue and writes results to offchain_data.
type EventURIConsumer struct {
	conn     *amqp.Connection
	offchain *offchainrepo.Repository
	resolver *domainuri.Resolver
	prefetch int
}

// NewEventURIConsumer constructs a consumer. prefetch controls AMQP QoS prefetch count.
func NewEventURIConsumer(
	conn *amqp.Connection,
	offchain *offchainrepo.Repository,
	resolver *domainuri.Resolver,
	prefetch int,
) *EventURIConsumer {
	if prefetch < 1 {
		prefetch = 1
	}
	return &EventURIConsumer{
		conn:     conn,
		offchain: offchain,
		resolver: resolver,
		prefetch: prefetch,
	}
}

// RunChain opens a channel, declares the queue, and consumes until ctx is cancelled.
// Intended to run as a goroutine; one instance per chain.
func (c *EventURIConsumer) RunChain(ctx context.Context, chainID int64) error {
	queueName := mq.EventURIQueueName(chainID)

	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("event_uri_consumer chain=%d: open channel: %w", chainID, err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("event_uri_consumer chain=%d: declare queue: %w", chainID, err)
	}

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("event_uri_consumer chain=%d: set qos: %w", chainID, err)
	}

	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("event_uri_consumer chain=%d: start consume: %w", chainID, err)
	}

	log.Printf("event_uri_consumer: chain=%d started queue=%s", chainID, queueName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("event_uri_consumer chain=%d: delivery channel closed", chainID)
			}

			var msg mq.AgentURIMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				// Malformed message — discard permanently (ack).
				log.Printf("event_uri_consumer chain=%d: discard malformed message: %v", chainID, err)
				_ = d.Ack(false)
				continue
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				// Infrastructure error (DB write, context) — requeue for retry.
				log.Printf("event_uri_consumer chain=%d: nack+requeue uri=%s: %v", chainID, msg.URI, err)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

// handleMessage fetches the URI and writes the result to offchain_data.
// Returns non-nil error only for infrastructure failures (triggers nack+requeue).
// Fetch errors and non-JSON content are recorded and return nil (triggers ack).
func (c *EventURIConsumer) handleMessage(ctx context.Context, msg mq.AgentURIMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	sourceType := domainuri.DetectURIType(msg.URI).String()
	eventType := msg.EventName
	contractType := "" // not carried in AgentURIMessage; not required for offchain_data

	body, isJSON, fetchErr := c.resolver.FetchRaw(ctx, msg.URI)

	switch {
	case fetchErr != nil:
		// Network/HTTP error — record failure, ack (no retry).
		if dbErr := c.offchain.UpsertFailure(ctx, msg.URI, sourceType, eventType, contractType, fetchErr.Error()); dbErr != nil {
			return fmt.Errorf("event_uri_consumer: write fetch-failure: %w", dbErr)
		}
		log.Printf("event_uri_consumer: fetch error (recorded status=-1) uri=%s: %v", msg.URI, fetchErr)
		return nil

	case !isJSON:
		// Fetched but not valid JSON — record, ack.
		if dbErr := c.offchain.UpsertFetchedNotJSON(ctx, msg.URI, string(body), sourceType, eventType, contractType); dbErr != nil {
			return fmt.Errorf("event_uri_consumer: write not-json: %w", dbErr)
		}
		log.Printf("event_uri_consumer: non-JSON content (recorded status=1) uri=%s size=%d", msg.URI, len(body))
		return nil

	default:
		// Valid JSON — record success.
		if dbErr := c.offchain.UpsertSuccess(ctx, msg.URI, string(body), sourceType, eventType, contractType); dbErr != nil {
			return fmt.Errorf("event_uri_consumer: write success: %w", dbErr)
		}
		return nil
	}
}
