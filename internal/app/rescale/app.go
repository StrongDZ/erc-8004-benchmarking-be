package rescale

// app.go — Rescale worker: retroactively corrects feedback ValueScale + category when a
// tag1's detected scale changes (e.g. "starred" recognized as star5 instead of pct100).
//
// Two phases per correction record:
//   Phase 3 (Update ValueScale): bulk idempotent $set of ValueScale + re-derived category
//                                on each affected feedback row.
//   Phase 4 (Mark done).
//
// Score mass is reconciled on the next score-refresh replay cycle — no incremental
// $inc is required or meaningful (reputationScore is derived, not an accumulator).

import (
	"context"
	"fmt"
	"log"
	"time"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
)

// App orchestrates the rescale correction cron.
type App struct {
	feedbackRepo *feedbackrepo.Repository
	corrRepo     *tagstats.CorrectionRepository
	interval     time.Duration
}

// NewApp constructs a rescale worker.
func NewApp(
	feedbackRepo *feedbackrepo.Repository,
	corrRepo *tagstats.CorrectionRepository,
	interval time.Duration,
) *App {
	return &App{
		feedbackRepo: feedbackRepo,
		corrRepo:     corrRepo,
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
	// Phase 3: Update feedback ValueScale (idempotent bulk write).
	// Score mass is reconciled by the next score-refresh replay cycle.
	if err := a.updateFeedbackScale(ctx, rec); err != nil {
		return fmt.Errorf("phase 3 update value scale: %w", err)
	}

	// Phase 4: Mark done.
	return a.corrRepo.MarkDone(ctx, rec.ID, time.Now().Unix())
}

// ── Phase 3 ───────────────────────────────────────────────────────────────────

func (a *App) updateFeedbackScale(ctx context.Context, rec *tagstats.ScaleChangeCorrection) error {
	fbs, err := a.feedbackRepo.FindServiceFeedbacksByTagPair(ctx, rec.Tag1, rec.Tag2, rec.DetectedAt)
	if err != nil {
		return err
	}

	updates := buildScaleUpdates(fbs, rec.NewScale)

	if err := a.feedbackRepo.BulkUpdateValueScale(ctx, updates); err != nil {
		return err
	}
	return a.corrRepo.IncrFeedbacksRescaled(ctx, rec.ID, int64(len(updates)))
}

// buildScaleUpdates derives the Phase 3 bulk-update list from a slice of feedback records
// and the target scale. For each record it:
//   - re-derives the classification with the new scale (classifier.Classify),
//   - skips the record when both ValueScale and category/feature are already correct,
//   - includes category/feature/classification.rule.* in the update when the new
//     classification differs from the stored one (e.g. quality-tagged feedback that
//     becomes quantity because the new scale is "unbounded").
//
// Extracted as a pure function so it can be unit-tested without a database.
func buildScaleUpdates(fbs []feedbackrepo.FeedbackRecord, newScale string) []feedbackrepo.ScaleUpdate {
	var updates []feedbackrepo.ScaleUpdate
	for i := range fbs {
		fb := &fbs[i]
		newCls := classifier.Classify(fb.Tag1, fb.Tag2, newScale)

		scaleUnchanged := fb.ValueScale == newScale
		categoryUnchanged := fb.Category == string(newCls.Category) && fb.Feature == string(newCls.Feature)
		if scaleUnchanged && categoryUnchanged {
			continue // fully up to date — idempotent skip
		}

		upd := feedbackrepo.ScaleUpdate{ID: fb.ID, NewScale: newScale}
		if !categoryUnchanged {
			upd.NewCategory = string(newCls.Category)
			upd.NewFeature = string(newCls.Feature)
		}
		updates = append(updates, upd)
	}
	return updates
}
