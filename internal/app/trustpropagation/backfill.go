package trustpropagation

// backfill.go — Periodic catch-up: re-publish feedback rows that the
// trust-graph-updater has not yet graded into erc8004.feedback.classified.
//
// The event-decoder only publishes during the same batch in which it
// upserts a feedback row. Any feedback persisted before the consumer was
// wired up (or while it was offline) sits with validationVerdict=null and
// never gets graded. This task scans those rows each trustrank-pass tick
// and re-publishes them so the consumer can finish the work.

import (
	"context"
	"fmt"

	"erc-8004-benchmarking-be/internal/mq"
	feedbackrep "erc-8004-benchmarking-be/internal/repository/feedback"
)

// BackfillDeps wires the dependencies needed to scan and re-publish.
type BackfillDeps struct {
	FeedbackRepo *feedbackrep.Repository
	Publisher    mq.Publisher
	BatchSize    int
}

// BackfillUnprocessed scans feedback_history for rows with no validationVerdict
// and publishes a FeedbackClassifiedMessage for each into mq.QueueFeedbackClassified.
// Returns the number of messages successfully published. Best-effort: a publish
// error stops the batch and surfaces; the next tick retries from the start.
func BackfillUnprocessed(ctx context.Context, chainID int64, deps BackfillDeps) (int, error) {
	if deps.Publisher == nil || deps.FeedbackRepo == nil {
		return 0, nil
	}
	limit := deps.BatchSize
	if limit <= 0 {
		limit = 5000
	}
	items, err := deps.FeedbackRepo.ListUnprocessedIDs(ctx, chainID, limit)
	if err != nil {
		return 0, fmt.Errorf("backfill: list unprocessed: %w", err)
	}
	published := 0
	for _, it := range items {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		msg := mq.FeedbackClassifiedMessage{FeedbackID: it.ID, ChainID: it.ChainID}
		if err := deps.Publisher.Publish(ctx, mq.QueueFeedbackClassified, msg); err != nil {
			return published, fmt.Errorf("backfill: publish %s: %w", it.ID, err)
		}
		published++
	}
	return published, nil
}
