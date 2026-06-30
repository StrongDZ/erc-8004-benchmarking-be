package feedbackother

// app.go — feedback-others worker: consumes erc8004.feedback.others,
// runs LLM fallback to resolve the category, and persists it via UpdateFallback.
//
// All rule-undecided ("others") feedback lands here after the event-decoder publishes it
// to QueueFeedbackOthers.  The worker resolves the LLM verdict and writes it back to
// MongoDB.  Grading, wallet counters, weighting, and reputation are handled by the
// score-refresh replay in the next cycle.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// FeedbackRepository is the subset of feedbackrepo.Repository used by this worker.
// Defined here so the handler can be tested with stubs.
type FeedbackRepository interface {
	FindByID(ctx context.Context, id string) (*feedbackrepo.FeedbackRecord, error)
	UpdateFallback(ctx context.Context, feedbackID string, f feedbackrepo.FallbackClassification) error
}

// AgentRepository is the subset of agentrepo.Repository used by this worker.
// nil is valid — agent context is loaded on a best-effort basis.
type AgentRepository interface {
	FindByAgentID(ctx context.Context, chainID int64, agentID string) (*agentrepo.AgentDocument, error)
}

// AppConfig holds runtime tunables.
type AppConfig struct {
	Workers  int // worker goroutines
	Prefetch int // AMQP QoS prefetch
}

// Deps groups external dependencies.
type Deps struct {
	Conn         *amqp.Connection
	FeedbackRepo FeedbackRepository
	// AgentRepo is optional; when non-nil resolveLLM loads the agent's
	// description/services/OASF so the classifier's domain stage has context.
	AgentRepo AgentRepository
	Cfg       AppConfig
	// Classifier is optional; when nil (or when the LLM is down) handle()
	// returns ErrTransient so the message is nack'd and requeued.
	Classifier *classifier.HybridClassifier
	// Publisher is optional; when non-nil, handle() publishes a
	// FeedbackClassifiedMessage to QueueFeedbackClassified after UpdateFallback
	// succeeds, so downstream consumers see the resolved category without waiting
	// for the next score-refresh cycle.
	Publisher mq.Publisher
}

// App is the feedback-others worker.
type App struct {
	deps Deps
}

// NewApp constructs App with sane defaults.
func NewApp(deps Deps) *App {
	if deps.Cfg.Workers <= 0 {
		deps.Cfg.Workers = 8
	}
	if deps.Cfg.Prefetch <= 0 {
		deps.Cfg.Prefetch = 10
	}
	return &App{deps: deps}
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ch, err := a.deps.Conn.Channel()
	if err != nil {
		return fmt.Errorf("feedbackother: open channel: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueFeedbackOthers, true, false, false, false, nil); err != nil {
		return fmt.Errorf("feedbackother: declare queue: %w", err)
	}
	if err := ch.Qos(a.deps.Cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("feedbackother: set qos: %w", err)
	}

	tag := fmt.Sprintf("feedback-others-%d", a.deps.Cfg.Workers)
	deliveries, err := ch.Consume(mq.QueueFeedbackOthers, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("feedbackother: start consume: %w", err)
	}

	type job struct {
		delivery amqp.Delivery
		msg      mq.FeedbackOthersMessage
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
					log.Printf("feedbackother: permanent reject feedback=%s: %v", j.msg.FeedbackID, err)
					_ = j.delivery.Ack(false)
				}
			}
		}()
	}

	err = rmqinfra.GracefulConsumeLoop(ctx, ch, tag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(),
		func(_ context.Context, d amqp.Delivery) error {
			var msg mq.FeedbackOthersMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("feedbackother: discard malformed message: %v", err)
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
