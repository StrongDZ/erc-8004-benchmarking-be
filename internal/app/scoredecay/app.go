package scoredecay

// app.go — Decay worker: periodically applies time decay to all agents' accumulatedScore.
// Runs every N hours (default 4), materializes the decay so ComputeCurrentScore stays O(1).

import (
	"context"
	"fmt"
	"log"
	"time"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/agent"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"
)

const decayBatchSize int64 = 200

// App orchestrates the periodic decay cron.
type App struct {
	agentRepo  *agent.Repository
	scoreRepo  *scorerepo.Repository
	formulaCfg scoring.FormulaConfig
	interval   time.Duration
}

// NewApp constructs a decay worker.
func NewApp(
	agentRepo *agent.Repository,
	scoreRepo *scorerepo.Repository,
	formulaCfg scoring.FormulaConfig,
	interval time.Duration,
) *App {
	return &App{
		agentRepo:  agentRepo,
		scoreRepo:  scoreRepo,
		formulaCfg: formulaCfg,
		interval:   interval,
	}
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	log.Printf("decay_worker: interval=%s batch=%d", a.interval, decayBatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.runCycle(ctx); err != nil {
				log.Printf("decay_worker: cycle error: %v", err)
			}
		}
	}
}

func (a *App) runCycle(ctx context.Context) error {
	now := time.Now().Unix()
	var skip int64
	var processed int

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		agents, err := a.agentRepo.FindAll(ctx, skip, decayBatchSize)
		if err != nil {
			return fmt.Errorf("decay: find agents skip=%d: %w", skip, err)
		}
		if len(agents) == 0 {
			break
		}

		var snapshotItems []struct {
			ChainID  int64
			AgentID  string
			Snapshot scorerepo.ScoreSnapshotItem
		}

		for i := range agents {
			ag := &agents[i]

			lambda := scoring.ComputeDecayRate(a.formulaCfg.Alpha, a.formulaCfg.TBaseDays)
			deltaDays := float64(now-ag.ScoreUpdateAt) / 86400.0
			if deltaDays <= 0 {
				continue
			}

			newAcc := ag.AccumulatedScore * scoring.ComputeDecayFactor(lambda, deltaDays)
			if err := a.agentRepo.UpdateAccumulatedScore(ctx, ag.ChainID, ag.AgentID, newAcc, now); err != nil {
				return fmt.Errorf("decay: update agent %s:%s: %w", ag.AgentID, fmt.Sprintf("%d", ag.ChainID), err)
			}

			displayScore := scoring.ComputeCurrentScore(newAcc, now, ag.ConsecutiveFails, now, a.formulaCfg)
			snapshotItems = append(snapshotItems, struct {
				ChainID  int64
				AgentID  string
				Snapshot scorerepo.ScoreSnapshotItem
			}{
				ChainID: ag.ChainID,
				AgentID: ag.AgentID,
				Snapshot: scorerepo.ScoreSnapshotItem{
					AgentScore: displayScore,
					Type:       "decay",
					Timestamp:  now,
				},
			})
			processed++
		}

		if len(snapshotItems) > 0 {
			err := a.scoreRepo.BulkAppendSnapshots(ctx, snapshotItems)
			if err != nil {
				return fmt.Errorf("decay: bulk append score snapshots: %w", err)
			}
		}

		skip += int64(len(agents))
		if int64(len(agents)) < decayBatchSize {
			break
		}
	}

	if processed > 0 {
		log.Printf("decay_worker: cycle done, decayed %d agents", processed)
	}
	return nil
}
