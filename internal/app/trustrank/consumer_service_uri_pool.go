package trustrank

// consumer_service_uri_pool.go — Shared worker pool for all uri.{chainID}.service_uri queues.
//
// One AMQP reader goroutine per chain forwards deliveries to a bounded job channel;
// N workers run HTTP fetch + Mongo upserts in parallel. Per-chain mutex serializes
// Delivery.Ack on the chain's channel (amqp091 channel is not goroutine-safe).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// ServiceURIConsumerPool drains every per-chain service_uri queue through one shared worker pool.
type ServiceURIConsumerPool struct {
	conn     *amqp.Connection
	handler  *ServiceURIConsumer
	prefetch int
	workers  int

	jobs chan serviceURIJob

	chainsMu sync.Mutex
	chains   map[int64]struct{}

	readersWG     sync.WaitGroup // readers exit after AMQP drain; then jobs channel closes
	closeJobsOnce sync.Once
}

type serviceURIJob struct {
	chainID int64
	d       amqp.Delivery
	ackMu   *sync.Mutex
	onDone  func()
}

// NewServiceURIConsumerPool builds a pool. workers is the number of concurrent HTTP handlers
// across all chains. prefetch is AMQP QoS per chain reader channel.
func NewServiceURIConsumerPool(
	conn *amqp.Connection,
	offchain *offchainrepo.Repository,
	resolver *domainuri.Resolver,
	prefetch, workers int,
) *ServiceURIConsumerPool {
	if workers < 1 {
		workers = 8
	}
	if prefetch < 1 {
		prefetch = 1
	}
	return &ServiceURIConsumerPool{
		conn:     conn,
		handler:  NewServiceURIConsumer(conn, offchain, resolver, prefetch),
		prefetch: prefetch,
		workers:  workers,
		chains:   make(map[int64]struct{}),
	}
}

// Start launches worker goroutines. Call once before EnsureChainReader.
func (p *ServiceURIConsumerPool) Start(ctx context.Context) {
	buf := p.workers * 16
	if buf < 64 {
		buf = 64
	}
	p.jobs = make(chan serviceURIJob, buf)
	go p.closeJobsAfterReadersExit(ctx)
	for i := 0; i < p.workers; i++ {
		go p.workerLoop(ctx, i)
	}
	log.Printf("service_uri_consumer_pool: workers=%d job_buf=%d prefetch_per_chain_reader=%d",
		p.workers, buf, p.prefetch)
}

// closeJobsAfterReadersExit waits for shutdown and every chain reader to finish AMQP drain, then closes jobs.
func (p *ServiceURIConsumerPool) closeJobsAfterReadersExit(ctx context.Context) {
	<-ctx.Done()
	p.readersWG.Wait()
	p.closeJobsOnce.Do(func() { close(p.jobs) })
}

// EnsureChainReader starts at most one AMQP reader goroutine per chain (non-blocking).
func (p *ServiceURIConsumerPool) EnsureChainReader(ctx context.Context, chainID int64) {
	if ctx.Err() != nil {
		return
	}
	p.chainsMu.Lock()
	if p.chains == nil {
		p.chains = make(map[int64]struct{})
	}
	if _, ok := p.chains[chainID]; ok {
		p.chainsMu.Unlock()
		return
	}
	p.chains[chainID] = struct{}{}
	p.chainsMu.Unlock()

	p.readersWG.Add(1)
	go func() {
		defer p.readersWG.Done()
		if err := p.runChainReader(ctx, chainID); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("service_uri_consumer_pool: reader chain=%d stopped: %v", chainID, err)
		}
	}()
}

func (p *ServiceURIConsumerPool) workerLoop(ctx context.Context, workerID int) {
	for job := range p.jobs {
		p.runJob(ctx, job, workerID)
	}
}

func (p *ServiceURIConsumerPool) runJob(parentCtx context.Context, job serviceURIJob, workerID int) {
	defer job.onDone()
	p.processDelivery(parentCtx, job.chainID, job.d, job.ackMu, workerID)
}

// processDelivery unmarshals, runs HandleServiceURI, and Ack's under ackMu. workerID < 0 means shutdown drain on reader.
func (p *ServiceURIConsumerPool) processDelivery(parentCtx context.Context, chainID int64, d amqp.Delivery, ackMu *sync.Mutex, workerID int) {
	params := rmqinfra.GracefulConsumeParamsDefaults()
	hctx, cancel := rmqinfra.DetachedHandlerContext(parentCtx, params.HandlerTimeout)
	defer cancel()

	var msg mq.ServiceURIMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("service_uri_consumer chain=%d: discard malformed message: %v", chainID, err)
	} else {
		p.handler.HandleServiceURI(hctx, msg)
	}

	ackMu.Lock()
	if err := d.Ack(false); err != nil {
		if workerID >= 0 {
			log.Printf("service_uri_consumer_pool worker=%d chain=%d: ack: %v", workerID, chainID, err)
		} else {
			log.Printf("service_uri_consumer_pool reader_drain chain=%d: ack: %v", chainID, err)
		}
	}
	ackMu.Unlock()
}

func (p *ServiceURIConsumerPool) drainReaderDeliveries(ctx context.Context, chainID int64, deliveries <-chan amqp.Delivery, ackMu *sync.Mutex) error {
	return rmqinfra.DrainPrefetchedDeliveries(ctx, deliveries, rmqinfra.GracefulConsumeParamsDefaults(), func(hctx context.Context, d amqp.Delivery) error {
		p.processDelivery(hctx, chainID, d, ackMu, -1)
		return nil
	})
}

func (p *ServiceURIConsumerPool) runChainReader(ctx context.Context, chainID int64) error {
	if p.jobs == nil {
		return fmt.Errorf("service_uri_consumer_pool: Start not called before reader chain=%d", chainID)
	}

	queueName := mq.ServiceURIQueueName(chainID)
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("service_uri_pool reader chain=%d: open channel: %w", chainID, err)
	}

	var ackMu sync.Mutex
	var inflight sync.WaitGroup

	defer func() {
		inflight.Wait()
		if err := ch.Close(); err != nil {
			log.Printf("service_uri_pool reader chain=%d: channel close: %v", chainID, err)
		}
	}()

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("service_uri_pool reader chain=%d: declare queue: %w", chainID, err)
	}
	if err := ch.Qos(p.prefetch, 0, false); err != nil {
		return fmt.Errorf("service_uri_pool reader chain=%d: set qos: %w", chainID, err)
	}

	tag := fmt.Sprintf("service-uri-pool-chain%d-%d", chainID, time.Now().UnixNano())
	deliveries, err := ch.Consume(queueName, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("service_uri_pool reader chain=%d: start consume: %w", chainID, err)
	}

	log.Printf("service_uri_consumer_pool: reader chain=%d queue=%s", chainID, queueName)

	for {
		select {
		case <-ctx.Done():
			if err := ch.Cancel(tag, false); err != nil {
				log.Printf("service_uri_pool reader chain=%d: cancel consume: %v", chainID, err)
			}
			if err := p.drainReaderDeliveries(ctx, chainID, deliveries, &ackMu); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("service_uri_pool reader chain=%d: AMQP drain: %v", chainID, err)
			}
			return ctx.Err()

		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("service_uri_pool reader chain=%d: deliveries channel closed", chainID)
			}
			inflight.Add(1)
			job := serviceURIJob{chainID: chainID, d: d, ackMu: &ackMu, onDone: inflight.Done}
			select {
			case p.jobs <- job:
			case <-ctx.Done():
				inflight.Done()
				if err := d.Nack(false, true); err != nil {
					log.Printf("service_uri_pool reader chain=%d: nack on shutdown: %v", chainID, err)
				}
				if err := ch.Cancel(tag, false); err != nil {
					log.Printf("service_uri_pool reader chain=%d: cancel consume: %v", chainID, err)
				}
				if err := p.drainReaderDeliveries(ctx, chainID, deliveries, &ackMu); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("service_uri_pool reader chain=%d: AMQP drain: %v", chainID, err)
				}
				return ctx.Err()
			}
		}
	}
}
