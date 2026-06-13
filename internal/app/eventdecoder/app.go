package eventdecoder

// app.go — Consumer application orchestrator.
// Consumes raw log messages, decodes them, and persists decoded events to MongoDB.
// Optionally publishes decoded events to a Redis Pub/Sub channel so the API
// server can fan them out to WebSocket clients in realtime.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	evmdecoder "erc-8004-benchmarking-be/internal/decoder/evm"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	redisinfra "erc-8004-benchmarking-be/internal/infra/redis"
	"erc-8004-benchmarking-be/internal/mq"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/utils"
	wsock "erc-8004-benchmarking-be/internal/websocket"
)

// App orchestrates raw-log consuming, decoding, and event persistence.
// It implements rabbitmq.MessageHandler[mq.RawLogMessage] directly.
type App struct {
	rmqinfra.BaseHandler[mq.RawLogMessage]
	pool          *rmqinfra.ConsumerPool[mq.RawLogMessage]
	eventsRepo    *eventrepo.Repository
	redis         *redisinfra.Client
	eventsChannel string
	queueName     string
	prefetch      int
}

// NewApp returns a ready App. Pass redis=nil to skip realtime publishing.
func NewApp(
	conn *amqp.Connection,
	queueName string,
	prefetch int,
	eventsRepo *eventrepo.Repository,
	redis *redisinfra.Client,
	eventsChannel string,
) *App {
	if eventsChannel == "" {
		eventsChannel = wsock.ChannelRealtimeEvents
	}
	a := &App{
		eventsRepo:    eventsRepo,
		redis:         redis,
		eventsChannel: eventsChannel,
		queueName:     queueName,
		prefetch:      prefetch,
	}
	a.pool = rmqinfra.New[mq.RawLogMessage](conn, a)
	return a
}

func (a *App) Init(_ string) rmqinfra.ReaderConfig {
	return rmqinfra.ReaderConfig{Prefetch: a.prefetch, Workers: 4}
}

func (a *App) QueueName(_ string) string { return a.queueName }

// Handle decodes and persists one raw log message.
// Decode errors are acked (dropped) — they are unrecoverable.
// Mongo write errors are returned so the pool nacks+requeues.
func (a *App) Handle(ctx context.Context, msg mq.RawLogMessage) error {
	eventName, args, err := evmdecoder.DecodeLog(msg)
	if err != nil {
		return nil // unrecognized log → ack+drop
	}

	now := time.Now().Unix()
	ev := eventrepo.DecodedEvent{
		ChainID:         msg.ChainID,
		ContractAddress: msg.ContractAddress,
		ContractType:    msg.ContractType,
		EventName:       eventName,
		BlockNumber:     msg.BlockNumber,
		BlockHash:       msg.BlockHash,
		TxHash:          msg.TxHash,
		LogIndex:        msg.LogIndex,
		Args:            args,
		Removed:         msg.Removed,
		Timestamp:       msg.Timestamp,
		IngestedAt:      msg.IngestedAt,
		DecodedAt:       now,
	}

	if err := a.eventsRepo.BulkUpsert(ctx, []eventrepo.DecodedEvent{ev}); err != nil {
		return fmt.Errorf("events bulk upsert: %w", err)
	}

	a.publishRealtime(ctx, ev)
	return nil
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	a.pool.EnsureReader(ctx, "")
	<-ctx.Done()
	return ctx.Err()
}

// publishRealtime best-effort publishes the decoded event to Redis. Failures are
// logged and swallowed so they never block consumer ack.
func (a *App) publishRealtime(ctx context.Context, ev eventrepo.DecodedEvent) {
	if a.redis == nil {
		return
	}
	agentID, _ := utils.GetStringArg(ev.Args, "agentId")
	frame := wsock.RealtimeEvent{
		Type:         wsock.TypeEventDecoded,
		EventName:    ev.EventName,
		ContractType: ev.ContractType,
		ChainID:      ev.ChainID,
		AgentID:      agentID,
		TxHash:       ev.TxHash,
		BlockNumber:  ev.BlockNumber,
		LogIndex:     ev.LogIndex,
		Timestamp:    ev.Timestamp,
		Args:         ev.Args,
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		log.Printf("consumer: marshal realtime event: %v", err)
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := a.redis.Publish(pubCtx, a.eventsChannel, payload); err != nil {
		log.Printf("consumer: publish realtime event: %v", err)
	}
}
