package scorerefresh

// app.go — Periodic score-refresh worker.
// Every cronExpr cycle: replay feedback_history per agent → upsert agent_score_stats
// + sync agent.accumulatedScore with the exact replay result.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
)

// App runs the periodic score-refresh cycle.
type App struct {
	agents     *agentrepo.Repository
	feedbacks  *feedbackrepo.Repository
	scoreStats *scorestats.Repository
	formulaCfg scoring.FormulaConfig
	cronExpr   string
}

// NewApp creates a new scorerefresh App.
func NewApp(
	agents *agentrepo.Repository,
	feedbacks *feedbackrepo.Repository,
	scoreStats *scorestats.Repository,
	formulaCfg scoring.FormulaConfig,
	cronExpr string,
) *App {
	return &App{
		agents:     agents,
		feedbacks:  feedbacks,
		scoreStats: scoreStats,
		formulaCfg: formulaCfg,
		cronExpr:   cronExpr,
	}
}

// Run starts the cron scheduler and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	c := cron.New()
	if _, err := c.AddFunc(a.cronExpr, func() { a.runCycle(ctx) }); err != nil {
		return fmt.Errorf("score-refresh: add cron func: %w", err)
	}
	c.Start()
	<-ctx.Done()
	<-c.Stop().Done()
	return ctx.Err()
}

func (a *App) runCycle(ctx context.Context) {
	now := time.Now().Unix()
	log.Printf("score-refresh: cycle start ts=%d", now)

	const batchSize = 200
	skip := int64(0)
	total := 0

	for {
		agents, err := a.agents.FindAll(ctx, skip, batchSize)
		if err != nil {
			log.Printf("score-refresh: list agents skip=%d: %v", skip, err)
			return
		}
		if len(agents) == 0 {
			break
		}

		statsBatch := make([]scorestats.AgentScoreStats, 0, len(agents))
		for _, ag := range agents {
			fbs, err := a.feedbacks.ListByAgent(ctx, ag.ChainID, ag.AgentID)
			if err != nil {
				log.Printf("score-refresh: feedbacks chain=%d agent=%s: %v", ag.ChainID, ag.AgentID, err)
				continue
			}
			stats := replayAgent(ag.ChainID, ag.AgentID, fbs, now, a.formulaCfg)
			statsBatch = append(statsBatch, stats)
		}

		if err := a.scoreStats.BulkUpsert(ctx, statsBatch); err != nil {
			log.Printf("score-refresh: bulk upsert stats: %v", err)
		}

		// Sync agent.accumulatedScore with the authoritative replay result.
		for _, stats := range statsBatch {
			if err := a.agents.UpdateAccumulatedScore(ctx, stats.ChainID, stats.AgentID, stats.Score, now); err != nil {
				log.Printf("score-refresh: update acc score chain=%d agent=%s: %v", stats.ChainID, stats.AgentID, err)
			}
		}

		total += len(agents)
		skip += int64(len(agents))
		if len(agents) < batchSize {
			break
		}
	}

	log.Printf("score-refresh: cycle done agents=%d", total)
}
