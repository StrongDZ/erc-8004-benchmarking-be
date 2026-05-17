package tagstats

// repo.go — Repositories for tag_value_stats, changed_tag_scales, rescale_deltas.

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// NewStatsRepository returns a StatsRepository bound to the named collection.
func NewStatsRepository(db *mongodrv.Database, collectionName string) *StatsRepository {
	m := mongorepo.NewMongoRepo[TagValueStats](db, collectionName)
	return &StatsRepository{MongoRepoImpl: *m}
}

// NewCorrectionRepository returns a CorrectionRepository bound to the named collection.
func NewCorrectionRepository(db *mongodrv.Database, collectionName string) *CorrectionRepository {
	m := mongorepo.NewMongoRepo[ScaleChangeCorrection](db, collectionName)
	return &CorrectionRepository{MongoRepoImpl: *m}
}

// NewDeltaRepository returns a DeltaRepository bound to the named collection.
func NewDeltaRepository(db *mongodrv.Database, collectionName string) *DeltaRepository {
	m := mongorepo.NewMongoRepo[RescaleDelta](db, collectionName)
	return &DeltaRepository{MongoRepoImpl: *m}
}

// BulkIncrementTiers atomically increments the tier count and total count for each TierUpdate.
// Empty feedbacks only increment emptyCount. Returns the updated stats for all affected tag1s.
func (r *StatsRepository) BulkIncrementTiers(ctx context.Context, updates []TierUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	now := time.Now().Unix()

	// Collapse updates: group by (tag1, tag2) pair to reduce round trips.
	type pairKey = string // TagPairKey(tag1, tag2)
	type tagAccum struct {
		tag1       string
		tag2       string
		count      int64
		emptyCount int64
		tiers      map[string]int64 // tier -> delta
	}
	grouped := make(map[pairKey]*tagAccum, len(updates))
	for _, u := range updates {
		key := TagPairKey(u.Tag1, u.Tag2)
		acc := grouped[key]
		if acc == nil {
			acc = &tagAccum{tag1: u.Tag1, tag2: u.Tag2, tiers: make(map[string]int64)}
			grouped[key] = acc
		}
		if u.IsEmpty {
			acc.emptyCount++
		} else {
			acc.count++
			if u.Tier != "" {
				acc.tiers[u.Tier]++
			}
		}
	}

	var models []mongodrv.WriteModel
	for key, acc := range grouped {
		inc := bson.M{"lastUpdatedAt": now}
		if acc.count > 0 {
			inc["count"] = acc.count
		}
		if acc.emptyCount > 0 {
			inc["emptyCount"] = acc.emptyCount
		}
		for tier, delta := range acc.tiers {
			inc["tierCounts."+tier] = delta
		}
		models = append(models, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": key}).
			SetUpdate(bson.M{
				"$inc": inc,
				"$setOnInsert": bson.M{"tag1": acc.tag1, "tag2": acc.tag2},
			}).
			SetUpsert(true))
	}

	opts := options.BulkWrite().SetOrdered(false)
	if _, err := r.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("tagstats: bulk increment tiers: %w", err)
	}
	return nil
}

// GetByKeys fetches the current stats for a set of (tag1, tag2) composite keys.
func (r *StatsRepository) GetByKeys(ctx context.Context, keys []string) ([]TagValueStats, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	docs, err := r.Find(ctx, bson.M{"_id": bson.M{"$in": keys}})
	if err != nil {
		return nil, fmt.Errorf("tagstats: get by keys: %w", err)
	}
	return docs, nil
}

// SetDetectedScale atomically updates detectedScale and increments scaleVersion for a (tag1, tag2) pair.
// lockedAt is set only when the scale is first detected (lockedAt == 0).
func (r *StatsRepository) SetDetectedScale(ctx context.Context, tag1, tag2, newScale string, nowUnix int64) error {
	key := TagPairKey(tag1, tag2)
	update := bson.M{
		"$set": bson.M{
			"detectedScale": newScale,
			"lastUpdatedAt": nowUnix,
		},
		"$inc": bson.M{"scaleVersion": 1},
	}
	// Set lockedAt only the first time (i.e., when lockedAt is missing/zero).
	_, err := r.UpdateOne(ctx, bson.M{"_id": key, "lockedAt": bson.M{"$in": bson.A{0, nil}}},
		bson.M{
			"$set": bson.M{
				"detectedScale": newScale,
				"lastUpdatedAt": nowUnix,
				"lockedAt":      nowUnix,
			},
			"$inc": bson.M{"scaleVersion": 1},
		})
	if err != nil {
		return err
	}
	// Also run unconditionally for subsequent scale changes (lockedAt already set).
	_, err = r.UpdateOne(ctx, bson.M{"_id": key, "lockedAt": bson.M{"$gt": 0}}, update)
	return err
}

// GetByTagPair fetches a single TagValueStats document for a (tag1, tag2) pair.
func (r *StatsRepository) GetByTagPair(ctx context.Context, tag1, tag2 string) (*TagValueStats, error) {
	doc, err := r.FindOne(ctx, bson.M{"_id": TagPairKey(tag1, tag2)})
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("tagstats: get %s:%s: %w", tag1, tag2, err)
	}
	return &doc, nil
}

// ── CorrectionRepository ──────────────────────────────────────────────────────

// UpsertCorrection writes a new correction record (idempotent: same _id = no-op if already exists).
func (r *CorrectionRepository) UpsertCorrection(ctx context.Context, c ScaleChangeCorrection) error {
	opts := options.Update().SetUpsert(true)
	_, err := r.UpdateOne(ctx, bson.M{"_id": c.ID},
		bson.M{"$setOnInsert": c},
		opts)
	if err != nil {
		return fmt.Errorf("correction repo: upsert %s: %w", c.ID, err)
	}
	return nil
}

// ClaimPending atomically transitions one pending record to in_progress.
// Returns nil if no pending record is available.
func (r *CorrectionRepository) ClaimPending(ctx context.Context, nowUnix int64) (*ScaleChangeCorrection, error) {
	filter := bson.M{"status": CorrectionPending}
	update := bson.M{"$set": bson.M{
		"status":    CorrectionInProgress,
		"startedAt": nowUnix,
	}}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetSort(bson.D{{Key: "detectedAt", Value: 1}}) // oldest first
	var doc ScaleChangeCorrection
	err := r.Coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("correction repo: claim pending: %w", err)
	}
	return &doc, nil
}

// SetDeltaSnapshotReady marks Phase 1 as complete.
func (r *CorrectionRepository) SetDeltaSnapshotReady(ctx context.Context, correctionID string, agentsTotal int64) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": correctionID}, bson.M{
		"$set": bson.M{
			"deltaSnapshotReady": true,
			"agentsTotal":        agentsTotal,
		},
	})
	return err
}

// UpdateCursor advances the idempotency cursor after an agent's delta is committed.
func (r *CorrectionRepository) UpdateCursor(ctx context.Context, correctionID, agentID string) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": correctionID}, bson.M{
		"$set": bson.M{"lastAgentIDCursor": agentID},
		"$inc": bson.M{"agentsProcessed": 1},
	})
	return err
}

// IncrFeedbacksRescaled increments the feedbacksRescaled counter.
func (r *CorrectionRepository) IncrFeedbacksRescaled(ctx context.Context, correctionID string, n int64) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": correctionID}, bson.M{
		"$inc": bson.M{"feedbacksRescaled": n},
	})
	return err
}

// MarkDone transitions the correction to done status.
func (r *CorrectionRepository) MarkDone(ctx context.Context, correctionID string, nowUnix int64) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": correctionID}, bson.M{
		"$set": bson.M{
			"status":     CorrectionDone,
			"finishedAt": nowUnix,
		},
	})
	return err
}

// MarkFailed records an error and sets status to failed.
func (r *CorrectionRepository) MarkFailed(ctx context.Context, correctionID, errMsg string, nowUnix int64) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": correctionID}, bson.M{
		"$set": bson.M{
			"status":     CorrectionFailed,
			"errorMsg":   errMsg,
			"finishedAt": nowUnix,
		},
	})
	return err
}

// ── DeltaRepository ───────────────────────────────────────────────────────────

// BulkUpsertDeltas writes pre-computed per-agent deltas (Phase 1 output).
func (r *DeltaRepository) BulkUpsertDeltas(ctx context.Context, deltas []RescaleDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	var models []mongodrv.WriteModel
	for _, d := range deltas {
		models = append(models, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": d.ID}).
			SetUpdate(bson.M{"$setOnInsert": d}).
			SetUpsert(true))
	}
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := r.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("delta repo: bulk upsert: %w", err)
	}
	return nil
}

// FindPendingForCorrection returns all unapplied deltas for a correction, sorted by agentID ASC,
// skipping entries at or before lastAgentIDCursor (cursor-based resume).
func (r *DeltaRepository) FindPendingForCorrection(ctx context.Context, correctionID, afterAgentID string) ([]RescaleDelta, error) {
	filter := bson.M{
		"correctionID": correctionID,
		"applied":      false,
	}
	if afterAgentID != "" {
		filter["agentID"] = bson.M{"$gt": afterAgentID}
	}
	opts := options.Find().SetSort(bson.D{{Key: "agentID", Value: 1}})
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("delta repo: find pending: %w", err)
	}
	return docs, nil
}

// MarkApplied marks a delta as applied (called within a session transaction).
func (r *DeltaRepository) MarkApplied(ctx context.Context, deltaID string) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": deltaID}, bson.M{"$set": bson.M{"applied": true}})
	return err
}
