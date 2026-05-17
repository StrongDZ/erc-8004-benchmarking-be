package trustrank

// consumer_service_uri.go — Consumer for uri.{chainID}.service_uri queues.
//
// Fetches service endpoint URIs published by the TrustRank processor when it
// encounters agent card service entries (identity registry events).
//
// Policy: always ack — no retry. Errors are logged clearly. Results are written
// to offchain_data with the appropriate status (-1 / 1 / 5).

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// ServiceURIConsumer fetches service endpoint URIs and writes results to offchain_data.
type ServiceURIConsumer struct {
	conn     *amqp.Connection
	offchain *offchainrepo.Repository
	resolver *domainuri.Resolver
	prefetch int
}

// NewServiceURIConsumer constructs a consumer. prefetch controls AMQP QoS prefetch count.
func NewServiceURIConsumer(
	conn *amqp.Connection,
	offchain *offchainrepo.Repository,
	resolver *domainuri.Resolver,
	prefetch int,
) *ServiceURIConsumer {
	if prefetch < 1 {
		prefetch = 1
	}
	return &ServiceURIConsumer{
		conn:     conn,
		offchain: offchain,
		resolver: resolver,
		prefetch: prefetch,
	}
}

// RunChain opens a channel, declares the queue, and consumes until ctx is cancelled.
// Intended to run as a goroutine; one instance per chain.
func (c *ServiceURIConsumer) RunChain(ctx context.Context, chainID int64) error {
	queueName := mq.ServiceURIQueueName(chainID)

	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("service_uri_consumer chain=%d: open channel: %w", chainID, err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("service_uri_consumer chain=%d: declare queue: %w", chainID, err)
	}

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("service_uri_consumer chain=%d: set qos: %w", chainID, err)
	}

	tag := fmt.Sprintf("service-uri-chain%d-%d", chainID, time.Now().UnixNano())
	deliveries, err := ch.Consume(queueName, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("service_uri_consumer chain=%d: start consume: %w", chainID, err)
	}

	log.Printf("service_uri_consumer: chain=%d started queue=%s", chainID, queueName)

	return rmqinfra.GracefulConsumeLoop(ctx, ch, tag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(), func(hctx context.Context, d amqp.Delivery) error {
		var msg mq.ServiceURIMessage
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			log.Printf("service_uri_consumer chain=%d: discard malformed message: %v", chainID, err)
			_ = d.Ack(false)
			return nil
		}

		// Always ack — service URI fetch is best-effort, no retry.
		c.HandleServiceURI(hctx, msg)
		_ = d.Ack(false)
		return nil
	})
}

// HandleServiceURI fetches the service endpoint URI and writes the result to offchain_data.
// Used by RunChain and ServiceURIConsumerPool workers.
func (c *ServiceURIConsumer) HandleServiceURI(ctx context.Context, msg mq.ServiceURIMessage) {
	c.handleMessage(ctx, msg)
}

// handleMessage fetches the service endpoint URI and writes the result to offchain_data.
// All errors are logged; nothing is returned (always ack).
func (c *ServiceURIConsumer) handleMessage(ctx context.Context, msg mq.ServiceURIMessage) {
	if ctx.Err() != nil {
		return
	}

	sourceType := domainuri.DetectURIType(msg.URI).String()
	const eventType = "service_endpoint"
	const contractType = "identity"

	body, isJSON, fetchErr := c.resolver.FetchRaw(ctx, msg.URI)

	switch {
	case fetchErr != nil:
		log.Printf("service_uri_consumer: fetch error (status=-1) chain=%d agent=%s service=%s uri=%s: %v",
			msg.ChainID, msg.AgentID, msg.ServiceName, msg.URI, fetchErr)
		if dbErr := c.offchain.UpsertFailure(ctx, msg.URI, sourceType, eventType, contractType, fetchErr.Error()); dbErr != nil {
			log.Printf("service_uri_consumer: write fetch-failure chain=%d uri=%s: %v",
				msg.ChainID, msg.URI, dbErr)
		}

	case !isJSON:
		log.Printf("service_uri_consumer: non-JSON content (status=1) chain=%d agent=%s service=%s uri=%s size=%d",
			msg.ChainID, msg.AgentID, msg.ServiceName, msg.URI, len(body))
		if dbErr := c.offchain.UpsertFetchedNotJSON(ctx, msg.URI, string(body), sourceType, eventType, contractType); dbErr != nil {
			log.Printf("service_uri_consumer: write not-json chain=%d uri=%s: %v",
				msg.ChainID, msg.URI, dbErr)
		}

	default:
		if dbErr := c.offchain.UpsertSuccess(ctx, msg.URI, string(body), sourceType, eventType, contractType); dbErr != nil {
			log.Printf("service_uri_consumer: write success chain=%d uri=%s: %v",
				msg.ChainID, msg.URI, dbErr)
		}
	}
}
