package feedbackgrade

// app.go — feedback-grader worker: consumes erc8004.feedback.classified, grades each
// resolved feedback, and live-updates the agent's reputation score (a fast approximation
// the authoritative score-refresh replay subsumes each cycle).
//
// All fully-classified feedback (rule-decided at ingest or LLM-resolved by feedback-others)
// is published to QueueFeedbackClassified. This worker grades the row and — for quality
// feedback — applies one weighted-mean mass increment to agent_score_stats, mirroring a
// single iteration of scorerefresh.replayAgent's loop exactly. The verdict (the IsGraded
// marker) is written LAST so a crash mid-update is re-processed cleanly.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
)

// FeedbackRepository is the subset of feedbackrepo.Repository used by this worker.
type FeedbackRepository interface {
	FindByID(ctx context.Context, id string) (*feedbackrepo.FeedbackRecord, error)
	UpdateWeighting(ctx context.Context, feedbackID string, u feedbackrepo.WeightingUpdate) error
}

// ScoreStatsRepository is the subset of scorestats.Repository used by this worker.
type ScoreStatsRepository interface {
	FindByID(ctx context.Context, chainID int64, agentID string) (*scorestats.AgentScoreStats, error)
	UpsertFromWritePath(
		ctx context.Context,
		chainID int64, agentID string,
		reputationScore, weightedScoreSum, weightMass float64, scoreUpdateAt int64,
		consecutiveFails, totalTasks, totalPassed, totalFailed int64, monthUniqueUsers int,
		composite, reputationNorm, adoption, services, publisher float64, publisherPresent bool, compliance float64,
		serviceWarnings []string,
		serviceScores []scorestats.ServiceReputationStats,
	) error
}

// AgentRepository is the subset of agentrepo.Repository used by this worker. It is used to
// sync the live composite to the agents collection (leaderboard denormalization) while
// preserving totalTasks/totalFeedbacks owned by the score-refresh cycle.
type AgentRepository interface {
	FindByAgentID(ctx context.Context, chainID int64, agentID string) (*agentrepo.AgentDocument, error)
	BulkUpdateScores(ctx context.Context, updates []agentrepo.ScoreUpdate) error
}

// WalletTrustReader is the subset of walletrepo.Repository used to read reviewer trust.
// BulkGetTrustScores is the SAME source scorerefresh.WalletTrustBatch.TrustScore derives
// from (the wallet document's trustScore field), so the live weight matches the replay.
type WalletTrustReader interface {
	BulkGetTrustScores(ctx context.Context, ids []string) (map[string]float64, error)
}

// AppConfig holds runtime tunables.
type AppConfig struct {
	Workers  int // worker goroutines
	Prefetch int // AMQP QoS prefetch
}

// Deps groups external dependencies.
type Deps struct {
	Conn             *amqp.Connection
	FeedbackRepo     FeedbackRepository
	ScoreStatsRepo   ScoreStatsRepository
	AgentRepo        AgentRepository
	WalletRepo       WalletTrustReader
	Cfg              AppConfig
	QWCfg            scoring.QualityWeightConfig
	FormulaCfg       scoring.FormulaConfig
	CompositeWeights scoring.CompositeWeights
}

// App is the feedback-grader worker.
type App struct {
	deps             Deps
	qwCfg            scoring.QualityWeightConfig
	formulaCfg       scoring.FormulaConfig
	compositeWeights scoring.CompositeWeights
	// locks holds a *sync.Mutex per "chainId:agentId" so the read-modify-write of one
	// agent's agent_score_stats is serialized across worker goroutines. Entries are never
	// deleted — bounded by the agent count.
	locks sync.Map
}

// NewApp constructs App with sane defaults.
func NewApp(deps Deps) *App {
	if deps.Cfg.Workers <= 0 {
		deps.Cfg.Workers = 8
	}
	if deps.Cfg.Prefetch <= 0 {
		deps.Cfg.Prefetch = 10
	}
	return &App{
		deps:             deps,
		qwCfg:            deps.QWCfg,
		formulaCfg:       deps.FormulaCfg,
		compositeWeights: deps.CompositeWeights,
	}
}

// lockFor acquires the per-agent mutex and returns its Unlock func.
func (a *App) lockFor(chainID int64, agentID string) func() {
	key := fmt.Sprintf("%d:%s", chainID, agentID)
	mu, _ := a.locks.LoadOrStore(key, &sync.Mutex{})
	m := mu.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ch, err := a.deps.Conn.Channel()
	if err != nil {
		return fmt.Errorf("feedbackgrade: open channel: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueFeedbackClassified, true, false, false, false, nil); err != nil {
		return fmt.Errorf("feedbackgrade: declare queue: %w", err)
	}
	if err := ch.Qos(a.deps.Cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("feedbackgrade: set qos: %w", err)
	}

	tag := fmt.Sprintf("feedback-grader-%d", a.deps.Cfg.Workers)
	deliveries, err := ch.Consume(mq.QueueFeedbackClassified, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("feedbackgrade: start consume: %w", err)
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
				err := a.handle(hctx, j.msg.FeedbackID)
				cancel()

				switch {
				case err == nil:
					_ = j.delivery.Ack(false)
				case err == ErrTransient:
					_ = j.delivery.Nack(false, true)
				default:
					log.Printf("feedbackgrade: permanent reject feedback=%s: %v", j.msg.FeedbackID, err)
					_ = j.delivery.Ack(false)
				}
			}
		}()
	}

	err = rmqinfra.GracefulConsumeLoop(ctx, ch, tag, deliveries, rmqinfra.GracefulConsumeParamsDefaults(),
		func(_ context.Context, d amqp.Delivery) error {
			var msg mq.FeedbackClassifiedMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("feedbackgrade: discard malformed message: %v", err)
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
