package uribootstrap

// consumer_event_uri.go — Consumer for uri.{chainID}.event_uri queues.
//
// Retry policy:
//   - Infrastructure failures (DB write error, context error): nack + requeue.
//   - HTTP/network fetch failure:                              ack + record status=-1.
//   - Fetched but content is not JSON:                        ack + record status=1.
//   - Successful JSON fetch:                                   ack + record status=5.
//
// Fetch failures are NOT retried to avoid blocking uri_fetched advance for broken links.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	mongodrv "go.mongodb.org/mongo-driver/mongo"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// EventURIConsumer fetches URIs from the event_uri queue and writes results to offchain_data.
type EventURIConsumer struct {
	conn       *amqp.Connection
	offchain   *offchainrepo.Repository
	resolver   *domainuri.Resolver
	configRepo *configrepo.ConfigRepository
	prefetch   int
}

// NewEventURIConsumer constructs a consumer. prefetch controls AMQP QoS prefetch count.
// configRepo may be nil (cursor advance is skipped).
func NewEventURIConsumer(
	conn *amqp.Connection,
	offchain *offchainrepo.Repository,
	resolver *domainuri.Resolver,
	configRepo *configrepo.ConfigRepository,
	prefetch int,
) *EventURIConsumer {
	if prefetch < 1 {
		prefetch = 1
	}
	return &EventURIConsumer{
		conn:       conn,
		offchain:   offchain,
		resolver:   resolver,
		configRepo: configRepo,
		prefetch:   prefetch,
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

	tag := fmt.Sprintf("event-uri-chain%d-%d", chainID, time.Now().UnixNano())
	deliveries, err := ch.Consume(queueName, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("event_uri_consumer chain=%d: start consume: %w", chainID, err)
	}

	log.Printf("event_uri_consumer: chain=%d started queue=%s", chainID, queueName)

	return rmqinfra.GracefulConsumeLoop(ctx, ch, tag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(), func(hctx context.Context, d amqp.Delivery) error {
		var msg mq.AgentURIMessage
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			// Malformed message — discard permanently (ack).
			log.Printf("event_uri_consumer chain=%d: discard malformed message: %v", chainID, err)
			_ = d.Ack(false)
			return nil
		}

		if err := c.handleMessage(hctx, msg); err != nil {
			// Infrastructure error (DB write, context) — requeue for retry.
			log.Printf("event_uri_consumer chain=%d: nack+requeue uri=%s: %v", chainID, msg.URI, err)
			_ = d.Nack(false, true)
			return nil
		}
		c.advanceFetchedCursor(hctx, msg.ChainID, msg.BlockNumber, msg.LogIndex)
		_ = d.Ack(false)
		return nil
	})
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
	contractType := msg.ContractType

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

// advanceFetchedCursor moves uri_fetched_{chainID} forward to at least (blockNumber, logIndex)
// after a terminal offchain upsert. Skips legacy queue payloads with no position.
func (c *EventURIConsumer) advanceFetchedCursor(ctx context.Context, chainID int64, blockNumber uint64, logIndex uint) {
	if c.configRepo == nil {
		return
	}
	if blockNumber == 0 && logIndex == 0 {
		return
	}

	key := URIFetchedCursorKey(chainID)
	entry, err := c.configRepo.Get(ctx, key)
	if err != nil && err != mongodrv.ErrNoDocuments {
		log.Printf("event_uri_consumer: load fetched cursor chain=%d: %v", chainID, err)
		return
	}
	if entry != nil && entry.Metadata != nil {
		oldBN, oldLI, ok := parseBlockLogFromMetadata(entry.Metadata)
		if ok {
			if blockNumber < oldBN || (blockNumber == oldBN && logIndex <= oldLI) {
				return
			}
		}
	}

	md := map[string]any{"blockNumber": blockNumber, "logIndex": logIndex}
	inserted, err := c.configRepo.SetIfAbsent(ctx, key, md)
	if err != nil {
		log.Printf("event_uri_consumer: save fetched cursor chain=%d: %v", chainID, err)
		return
	}
	if !inserted {
		if err := c.configRepo.UpdateMetadata(ctx, key, md); err != nil {
			log.Printf("event_uri_consumer: update fetched cursor chain=%d: %v", chainID, err)
		}
	}
}

func parseBlockLogFromMetadata(md map[string]any) (blockNumber uint64, logIndex uint, ok bool) {
	bn, ok1 := md["blockNumber"]
	li, ok2 := md["logIndex"]
	if !ok1 || !ok2 {
		return 0, 0, false
	}

	switch v := bn.(type) {
	case float64:
		blockNumber = uint64(v)
	case int64:
		blockNumber = uint64(v)
	case int32:
		blockNumber = uint64(v)
	case uint32:
		blockNumber = uint64(v)
	case uint64:
		blockNumber = v
	default:
		return 0, 0, false
	}

	switch v := li.(type) {
	case float64:
		logIndex = uint(v)
	case int64:
		logIndex = uint(v)
	case int32:
		logIndex = uint(v)
	case uint32:
		logIndex = uint(v)
	case uint64:
		logIndex = uint(v)
	default:
		return 0, 0, false
	}

	return blockNumber, logIndex, true
}
