package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ReaderConfig is returned by MessageHandler.Init for each reader.
type ReaderConfig struct {
	// Prefetch is the AMQP QoS prefetch count for this reader's channel.
	Prefetch int
	// Workers is the number of concurrent handler goroutines shared across all readers.
	// Only read from the first EnsureReader call; subsequent values are ignored.
	Workers int
}

// MessageHandler defines the lifecycle hooks a concrete consumer must implement.
// ConsumerPool calls them in a fixed sequence for every delivery.
type MessageHandler[T any] interface {
	// Init is called once per EnsureReader. Returns AMQP + worker config for that reader.
	// Use key="" for single-queue consumers.
	Init(key string) ReaderConfig
	// QueueName maps the reader key to an AMQP queue name.
	QueueName(key string) string
	// Handle processes one decoded message.
	// nil → ack; non-nil → OnError decides ack vs nack+requeue.
	Handle(ctx context.Context, msg T) error
	// OnError is called when Handle returns non-nil.
	// Return true to nack+requeue, false to ack-drop.
	OnError(ctx context.Context, err error, msg T) bool
	// OnDone is called after the ack/nack decision.
	// acked=true for both success and drop; acked=false only for nack+requeue.
	OnDone(ctx context.Context, msg T, acked bool)
}

// BaseHandler provides default implementations of Init, OnError and OnDone.
// Embed it in a concrete handler so only QueueName and Handle must be implemented.
type BaseHandler[T any] struct{}

func (BaseHandler[T]) Init(_ string) ReaderConfig              { return ReaderConfig{Prefetch: 1, Workers: 8} }
func (BaseHandler[T]) OnError(_ context.Context, _ error, _ T) bool { return true }
func (BaseHandler[T]) OnDone(_ context.Context, _ T, _ bool)        {}

// ConsumerPool manages a shared worker pool draining deliveries from one or more AMQP queues.
// Each reader owns one AMQP channel; all readers share the same worker goroutines.
type ConsumerPool[T any] struct {
	conn    *amqp.Connection
	handler MessageHandler[T]

	jobs          chan poolJob[T]
	closeJobsOnce sync.Once

	readersMu sync.Mutex
	readers   map[string]struct{}
	readersWG sync.WaitGroup

	startOnce sync.Once
}

type poolJob[T any] struct {
	key   string
	msg   T
	d     amqp.Delivery
	ackMu *sync.Mutex
	done  func() // signals inflight.Done to the owning reader
}

// New creates a ConsumerPool. Workers are started lazily on the first EnsureReader call.
func New[T any](conn *amqp.Connection, handler MessageHandler[T]) *ConsumerPool[T] {
	return &ConsumerPool[T]{
		conn:    conn,
		handler: handler,
		readers: make(map[string]struct{}),
	}
}

// EnsureReader starts at most one AMQP reader goroutine for key (idempotent, concurrency-safe).
// Worker goroutines are started on the first call using the Workers value from handler.Init(key).
func (p *ConsumerPool[T]) EnsureReader(ctx context.Context, key string) {
	if ctx.Err() != nil {
		return
	}

	cfg := p.handler.Init(key)
	if cfg.Prefetch < 1 {
		cfg.Prefetch = 1
	}
	if cfg.Workers < 1 {
		cfg.Workers = 8
	}

	p.startOnce.Do(func() {
		buf := cfg.Workers * 16
		if buf < 64 {
			buf = 64
		}
		p.jobs = make(chan poolJob[T], buf)
		go p.closeJobsAfterReadersExit(ctx)
		for i := 0; i < cfg.Workers; i++ {
			go p.workerLoop(ctx, i)
		}
		log.Printf("consumer_pool: started workers=%d buf=%d", cfg.Workers, buf)
	})

	p.readersMu.Lock()
	if _, ok := p.readers[key]; ok {
		p.readersMu.Unlock()
		return
	}
	p.readers[key] = struct{}{}
	p.readersMu.Unlock()

	p.readersWG.Add(1)
	go func() {
		defer p.readersWG.Done()
		if err := p.runReader(ctx, key, cfg.Prefetch); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("consumer_pool: reader key=%q stopped: %v", key, err)
		}
	}()
}

func (p *ConsumerPool[T]) closeJobsAfterReadersExit(ctx context.Context) {
	<-ctx.Done()
	p.readersWG.Wait()
	p.closeJobsOnce.Do(func() { close(p.jobs) })
}

func (p *ConsumerPool[T]) workerLoop(ctx context.Context, id int) {
	for j := range p.jobs {
		p.processJob(ctx, j, id)
	}
}

func (p *ConsumerPool[T]) processJob(parentCtx context.Context, j poolJob[T], workerID int) {
	defer j.done()

	hctx, cancel := DetachedHandlerContext(parentCtx, GracefulConsumeParamsDefaults().HandlerTimeout)
	defer cancel()

	err := p.handler.Handle(hctx, j.msg)
	retry := err != nil && p.handler.OnError(hctx, err, j.msg)

	j.ackMu.Lock()
	if retry {
		if ackErr := j.d.Nack(false, true); ackErr != nil {
			log.Printf("consumer_pool worker=%d key=%q: nack: %v", workerID, j.key, ackErr)
		}
	} else {
		if ackErr := j.d.Ack(false); ackErr != nil {
			log.Printf("consumer_pool worker=%d key=%q: ack: %v", workerID, j.key, ackErr)
		}
	}
	j.ackMu.Unlock()

	p.handler.OnDone(hctx, j.msg, !retry)
}

func (p *ConsumerPool[T]) runReader(ctx context.Context, key string, prefetch int) error {
	queueName := p.handler.QueueName(key)

	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("consumer_pool reader key=%q: open channel: %w", key, err)
	}

	var ackMu sync.Mutex
	var inflight sync.WaitGroup

	defer func() {
		inflight.Wait()
		if closeErr := ch.Close(); closeErr != nil {
			log.Printf("consumer_pool reader key=%q: channel close: %v", key, closeErr)
		}
	}()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("consumer_pool reader key=%q: declare queue: %w", key, err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("consumer_pool reader key=%q: set qos: %w", key, err)
	}

	tag := fmt.Sprintf("pool-%s-%d", key, time.Now().UnixNano())
	deliveries, err := ch.Consume(queueName, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consumer_pool reader key=%q: start consume: %w", key, err)
	}

	log.Printf("consumer_pool: reader started key=%q queue=%s prefetch=%d", key, queueName, prefetch)

	for {
		select {
		case <-ctx.Done():
			if cancelErr := ch.Cancel(tag, false); cancelErr != nil {
				log.Printf("consumer_pool reader key=%q: cancel consume: %v", key, cancelErr)
			}
			if drainErr := p.drainReader(ctx, key, deliveries, &ackMu); drainErr != nil && !errors.Is(drainErr, context.Canceled) {
				log.Printf("consumer_pool reader key=%q: drain: %v", key, drainErr)
			}
			return ctx.Err()

		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("consumer_pool reader key=%q: delivery channel closed", key)
			}

			var msg T
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("consumer_pool reader key=%q: discard malformed message: %v", key, err)
				ackMu.Lock()
				_ = d.Ack(false)
				ackMu.Unlock()
				continue
			}

			inflight.Add(1)
			j := poolJob[T]{key: key, msg: msg, d: d, ackMu: &ackMu, done: inflight.Done}
			select {
			case p.jobs <- j:
			case <-ctx.Done():
				inflight.Done()
				ackMu.Lock()
				_ = d.Nack(false, true)
				ackMu.Unlock()
				if cancelErr := ch.Cancel(tag, false); cancelErr != nil {
					log.Printf("consumer_pool reader key=%q: cancel consume: %v", key, cancelErr)
				}
				if drainErr := p.drainReader(ctx, key, deliveries, &ackMu); drainErr != nil && !errors.Is(drainErr, context.Canceled) {
					log.Printf("consumer_pool reader key=%q: drain: %v", key, drainErr)
				}
				return ctx.Err()
			}
		}
	}
}

// drainReader processes prefetched deliveries synchronously after ch.Cancel during shutdown.
// Runs in the reader goroutine to avoid deadlock when the jobs buffer is full.
func (p *ConsumerPool[T]) drainReader(ctx context.Context, key string, deliveries <-chan amqp.Delivery, ackMu *sync.Mutex) error {
	return DrainPrefetchedDeliveries(ctx, deliveries, GracefulConsumeParamsDefaults(), func(hctx context.Context, d amqp.Delivery) error {
		var msg T
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			log.Printf("consumer_pool drain key=%q: discard malformed: %v", key, err)
			ackMu.Lock()
			_ = d.Ack(false)
			ackMu.Unlock()
			return nil
		}

		err := p.handler.Handle(hctx, msg)
		retry := err != nil && p.handler.OnError(hctx, err, msg)

		ackMu.Lock()
		if retry {
			_ = d.Nack(false, true)
		} else {
			_ = d.Ack(false)
		}
		ackMu.Unlock()

		p.handler.OnDone(hctx, msg, !retry)
		return nil
	})
}
