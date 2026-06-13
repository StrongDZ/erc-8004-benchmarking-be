package trustgraph

// app.go — Trust graph updater: consumes erc8004.feedback.classified,
// processes each event through validate→weight→persist using N goroutines.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/propagation"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

// AppConfig holds runtime tunables.
type AppConfig struct {
	ColdStartT0 float64 // default trust for new wallets
	Workers     int     // worker goroutines
	Prefetch    int     // AMQP QoS prefetch
}

// Deps groups external dependencies.
type Deps struct {
	Conn         *amqp.Connection
	FeedbackRepo *feedbackrepo.Repository
	WalletRepo   *walletrepo.Repository
	PropCfg      propagation.PropagationConfig
	Cfg          AppConfig
	// Classifier is optional; when non-nil the worker calls the LLM for
	// feedback records where rule.category="others" and fallback is absent.
	Classifier *classifier.HybridClassifier
	// Publisher is optional; when non-nil, handle() publishes a
	// WalletEnrichMessage whenever UpsertCold inserts a brand-new sender wallet.
	Publisher mq.Publisher
}

// App is the trust-graph-updater worker.
type App struct {
	deps        Deps
	walletLocks sync.Map // map[string]*sync.Mutex — per-wallet serialization
}

// NewApp constructs App with sane defaults.
func NewApp(deps Deps) *App {
	if deps.Cfg.Workers <= 0 {
		deps.Cfg.Workers = 8
	}
	if deps.Cfg.Prefetch <= 0 {
		deps.Cfg.Prefetch = 10
	}
	if deps.Cfg.ColdStartT0 <= 0 {
		deps.Cfg.ColdStartT0 = 10
	}
	return &App{deps: deps}
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ch, err := a.deps.Conn.Channel()
	if err != nil {
		return fmt.Errorf("trustgraph: open channel: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueFeedbackClassified, true, false, false, false, nil); err != nil {
		return fmt.Errorf("trustgraph: declare queue: %w", err)
	}
	if err := ch.Qos(a.deps.Cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("trustgraph: set qos: %w", err)
	}

	tag := fmt.Sprintf("trust-graph-updater-%d", a.deps.Cfg.Workers)
	deliveries, err := ch.Consume(mq.QueueFeedbackClassified, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("trustgraph: start consume: %w", err)
	}

	type job struct {
		delivery amqp.Delivery
		msg      mq.FeedbackClassifiedMessage
	}

	jobs := make(chan job, a.deps.Cfg.Workers*16)

	var wg sync.WaitGroup
	for i := 0; i < a.deps.Cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				hctx, cancel := rmqinfra.DetachedHandlerContext(ctx, 0)
				err := a.handle(hctx, j.msg.FeedbackID, j.msg.ChainID)
				cancel()

				switch {
				case err == nil:
					_ = j.delivery.Ack(false)
				case err == ErrTransient:
					_ = j.delivery.Nack(false, true)
				default:
					log.Printf("trustgraph: permanent reject feedback=%s: %v", j.msg.FeedbackID, err)
					_ = j.delivery.Ack(false)
				}
			}
		}()
	}

	err = rmqinfra.GracefulConsumeLoop(ctx, ch, tag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(),
		func(_ context.Context, d amqp.Delivery) error {
			var msg mq.FeedbackClassifiedMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("trustgraph: discard malformed message: %v", err)
				_ = d.Ack(false)
				return nil
			}
			jobs <- job{delivery: d, msg: msg}
			return nil
		},
	)

	close(jobs)
	wg.Wait()
	return err
}

// walletMutex returns the per-address mutex, creating lazily.
func (a *App) walletMutex(addr string) *sync.Mutex {
	v, _ := a.walletLocks.LoadOrStore(addr, &sync.Mutex{})
	return v.(*sync.Mutex)
}
