package rescale

// app.go — Rescale worker: retroactively corrects agent reputationScore when a
// tag1's detected scale changes (e.g. "starred" recognized as star5 instead of pct100).
//
// Four phases per correction record:
//   Phase 1 (Compute deltas):  read-only pass over feedback_history; write rescale_deltas.
//   Phase 2 (Apply deltas):    cursor-based; each agent's $inc is committed in a transaction
//                              alongside advancing the idempotency cursor.
//   Phase 3 (Update Vi):       bulk idempotent $set of Vi on each affected feedback.
//   Phase 4 (Mark done).
//
// Runs in parallel with live scoring — uses $inc (not $set) so no lock is required.

import (
	"context"
	"fmt"
	"log"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	scorestatsrepo "erc-8004-benchmarking-be/internal/repository/scorestats"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
)

const rescaleBatchSize = 50

// App orchestrates the rescale correction cron.
type App struct {
	mongoClient  *mongodrv.Client
	agentRepo    *agentrepo.Repository
	statsRepo    *scorestatsrepo.Repository
	feedbackRepo *feedbackrepo.Repository
	corrRepo     *tagstats.CorrectionRepository
	deltaRepo    *tagstats.DeltaRepository
	formulaCfg   scoring.FormulaConfig
	interval     time.Duration
}

// NewApp constructs a rescale worker.
func NewApp(
	mongoClient *mongodrv.Client,
	agentRepo *agentrepo.Repository,
	statsRepo *scorestatsrepo.Repository,
	feedbackRepo *feedbackrepo.Repository,
	corrRepo *tagstats.CorrectionRepository,
	deltaRepo *tagstats.DeltaRepository,
	formulaCfg scoring.FormulaConfig,
	interval time.Duration,
) *App {
	return &App{
		mongoClient:  mongoClient,
		agentRepo:    agentRepo,
		statsRepo:    statsRepo,
		feedbackRepo: feedbackRepo,
		corrRepo:     corrRepo,
		deltaRepo:    deltaRepo,
		formulaCfg:   formulaCfg,
		interval:     interval,
	}
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	log.Printf("rescale_worker: interval=%s", a.interval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.runCycle(ctx); err != nil {
				log.Printf("rescale_worker: cycle error: %v", err)
			}
		}
	}
}

func (a *App) runCycle(ctx context.Context) error {
	now := time.Now().Unix()

	rec, err := a.corrRepo.ClaimPending(ctx, now)
	if err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}
	if rec == nil {
		return nil // nothing to do
	}

	log.Printf("rescale_worker: processing correction %s (%s → %s)", rec.ID, rec.OldScale, rec.NewScale)

	if err := a.processCorrection(ctx, rec); err != nil {
		log.Printf("rescale_worker: correction %s failed: %v", rec.ID, err)
		_ = a.corrRepo.MarkFailed(ctx, rec.ID, err.Error(), time.Now().Unix())
	}
	return nil
}

func (a *App) processCorrection(ctx context.Context, rec *tagstats.ScaleChangeCorrection) error {
	// ── Phase 1: Compute delta snapshot ───────────────────────────────────────
	if !rec.DeltaSnapshotReady {
		if err := a.computeDeltas(ctx, rec); err != nil {
			return fmt.Errorf("phase 1 compute deltas: %w", err)
		}
	}

	// ── Phase 2: Apply agent deltas (cursor-based, transactional) ────────────
	if err := a.applyDeltas(ctx, rec); err != nil {
		return fmt.Errorf("phase 2 apply deltas: %w", err)
	}

	// ── Phase 3: Update feedback ValueScale (idempotent bulk write) ─────────
	if err := a.updateFeedbackScale(ctx, rec); err != nil {
		return fmt.Errorf("phase 3 update value scale: %w", err)
	}

	// ── Phase 4: Mark done ────────────────────────────────────────────────────
	return a.corrRepo.MarkDone(ctx, rec.ID, time.Now().Unix())
}

// ── Phase 1 ───────────────────────────────────────────────────────────────────

func (a *App) computeDeltas(ctx context.Context, rec *tagstats.ScaleChangeCorrection) error {
	fbs, err := a.feedbackRepo.FindServiceFeedbacksByTagPair(ctx, rec.Tag1, rec.Tag2, rec.DetectedAt)
	if err != nil {
		return err
	}

	nowUnix := time.Now().Unix()

	// Group delta by agentID.
	type agentInfo struct {
		chainID int64
		delta   float64
	}
	agentDeltas := make(map[string]*agentInfo, len(fbs)/4)

	for i := range fbs {
		fb := &fbs[i]
		real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals))
		if !ok {
			continue
		}
		// viOld is derived from the scale stored on the feedback (= rec.OldScale equivalent).
		// vi is not stored directly — always computed from raw value + valueScale.
		viOld := classifier.NormalizeValueWithScale(real, fb.ValueScale)
		viNew := classifier.NormalizeValueWithScale(real, rec.NewScale)
		if viNew == viOld {
			continue
		}

		wi := fb.Wi
		lambda := scoring.ComputeDecayRate(max(wi, a.formulaCfg.Alpha), a.formulaCfg.TBaseDays)
		deltaDays := float64(nowUnix-fb.Timestamp) / 86400.0
		if deltaDays < 0 {
			deltaDays = 0
		}
		decay := scoring.ComputeDecayFactor(lambda, deltaDays)
		contribution := wi * (viNew - viOld) * decay

		ai := agentDeltas[fb.AgentID]
		if ai == nil {
			ai = &agentInfo{chainID: fb.ChainID}
			agentDeltas[fb.AgentID] = ai
		}
		ai.delta += contribution
	}

	// Write pre-computed deltas to rescale_deltas collection.
	deltas := make([]tagstats.RescaleDelta, 0, len(agentDeltas))
	for agentID, ai := range agentDeltas {
		deltas = append(deltas, tagstats.RescaleDelta{
			ID:           rec.ID + ":" + agentID,
			CorrectionID: rec.ID,
			AgentID:      agentID,
			ChainID:      ai.chainID,
			Delta:        ai.delta,
			Applied:      false,
		})
	}

	if err := a.deltaRepo.BulkUpsertDeltas(ctx, deltas); err != nil {
		return err
	}

	return a.corrRepo.SetDeltaSnapshotReady(ctx, rec.ID, int64(len(agentDeltas)))
}

// ── Phase 2 ───────────────────────────────────────────────────────────────────

func (a *App) applyDeltas(ctx context.Context, rec *tagstats.ScaleChangeCorrection) error {
	for {
		page, err := a.deltaRepo.FindPendingForCorrection(ctx, rec.ID, rec.LastAgentIDCursor)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}

		// Process up to rescaleBatchSize agents per iteration.
		batch := page
		if len(batch) > rescaleBatchSize {
			batch = batch[:rescaleBatchSize]
		}

		for _, d := range batch {
			if err := a.applyOneDelta(ctx, rec, d); err != nil {
				return fmt.Errorf("apply delta agent=%s: %w", d.AgentID, err)
			}
		}

		if len(page) < rescaleBatchSize {
			break
		}
	}
	return nil
}

// applyOneDelta commits a single agent's delta and advances the idempotency cursor
// inside a MongoDB session transaction so both writes are atomic.
func (a *App) applyOneDelta(ctx context.Context, rec *tagstats.ScaleChangeCorrection, d tagstats.RescaleDelta) error {
	session, err := a.mongoClient.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongodrv.SessionContext) (interface{}, error) {
		// 1. $inc reputationScore on agent_score_stats (single source of truth).
		if err := a.statsRepo.IncReputation(sessCtx, d.ChainID, d.AgentID, d.Delta); err != nil {
			return nil, err
		}

		// 2. Mark delta as applied (score correction picked up by score-refresh worker on next cycle).
		if err := a.deltaRepo.MarkApplied(sessCtx, d.ID); err != nil {
			return nil, err
		}

		// 4. Advance cursor in correction record.
		if err := a.corrRepo.UpdateCursor(sessCtx, rec.ID, d.AgentID); err != nil {
			return nil, err
		}

		return nil, nil
	}, options.Transaction())

	return err
}

// ── Phase 3 ───────────────────────────────────────────────────────────────────

func (a *App) updateFeedbackScale(ctx context.Context, rec *tagstats.ScaleChangeCorrection) error {
	fbs, err := a.feedbackRepo.FindServiceFeedbacksByTagPair(ctx, rec.Tag1, rec.Tag2, rec.DetectedAt)
	if err != nil {
		return err
	}

	var updates []feedbackrepo.ScaleUpdate
	for i := range fbs {
		fb := &fbs[i]
		if fb.ValueScale == rec.NewScale {
			continue // already up to date (idempotent)
		}
		updates = append(updates, feedbackrepo.ScaleUpdate{ID: fb.ID, NewScale: rec.NewScale})
	}

	if err := a.feedbackRepo.BulkUpdateValueScale(ctx, updates); err != nil {
		return err
	}
	return a.corrRepo.IncrFeedbacksRescaled(ctx, rec.ID, int64(len(updates)))
}

// max returns the larger of two float64 values.
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
