package trustrank

// consumer_service_uri.go — Consumer for uri.{chainID}.service_uri queues.
//
// Fetches service endpoint URIs published by the TrustRank processor when it
// encounters agent card service entries (identity registry events).
//
// Policy: always ack — no retry. Errors are logged clearly. Results are written
// to offchain_data with the appropriate status (-1 / 1 / 5).
//
// Manages a shared ConsumerPool; call EnsureChain to register a reader per chain.

import (
	"context"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// ServiceURIConsumer fetches service endpoint URIs and writes results to offchain_data.
// It manages a shared ConsumerPool; call EnsureChain per chain.
type ServiceURIConsumer struct {
	rmqinfra.BaseHandler[mq.ServiceURIMessage]
	pool     *rmqinfra.ConsumerPool[mq.ServiceURIMessage]
	offchain *offchainrepo.Repository
	resolver *domainuri.Resolver
	prefetch int
	workers  int
}

// NewServiceURIConsumer constructs a consumer pool. prefetch controls AMQP QoS per chain reader;
// workers is the number of concurrent HTTP handler goroutines shared across all chains.
func NewServiceURIConsumer(
	conn *amqp.Connection,
	offchain *offchainrepo.Repository,
	resolver *domainuri.Resolver,
	prefetch, workers int,
) *ServiceURIConsumer {
	if prefetch < 1 {
		prefetch = 1
	}
	if workers < 1 {
		workers = 8
	}
	c := &ServiceURIConsumer{
		offchain: offchain,
		resolver: resolver,
		prefetch: prefetch,
		workers:  workers,
	}
	c.pool = rmqinfra.New[mq.ServiceURIMessage](conn, c)
	return c
}

// EnsureChain registers a reader for chainID (idempotent, non-blocking).
func (c *ServiceURIConsumer) EnsureChain(ctx context.Context, chainID int64) {
	c.pool.EnsureReader(ctx, strconv.FormatInt(chainID, 10))
}

func (c *ServiceURIConsumer) Init(_ string) rmqinfra.ReaderConfig {
	return rmqinfra.ReaderConfig{Prefetch: c.prefetch, Workers: c.workers}
}

func (c *ServiceURIConsumer) QueueName(key string) string {
	chainID, _ := strconv.ParseInt(key, 10, 64)
	return mq.ServiceURIQueueName(chainID)
}

// Handle fetches the service URI and records the result. Always returns nil (best-effort, always ack).
func (c *ServiceURIConsumer) Handle(ctx context.Context, msg mq.ServiceURIMessage) error {
	c.handleMessage(ctx, msg)
	return nil
}

// OnError always returns false — service URI fetch is best-effort, no retry.
func (c *ServiceURIConsumer) OnError(_ context.Context, _ error, _ mq.ServiceURIMessage) bool {
	return false
}

// handleMessage fetches the service endpoint URI and writes the result to offchain_data.
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
