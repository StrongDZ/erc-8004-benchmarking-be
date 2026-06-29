package feedback

// repo_weighting.go — UpdateWeighting method: writes wi / qualityScore /
// validationVerdict / validationReason / wiComputedAt for a single feedback record.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// WeightingUpdate is the payload for UpdateWeighting.
type WeightingUpdate struct {
	Wi           float64
	QualityScore float64
	Verdict      string // "valid" | "junk" | "missing_fields" | "self" | "pending"
	Reason       string // optional detail when verdict != valid
	ComputedAt   int64  // Unix seconds
}

// buildWeightingUpdate constructs the $set document, omitting reason when empty.
func buildWeightingUpdate(u WeightingUpdate) map[string]any {
	set := map[string]any{
		"wi":                u.Wi,
		"qualityScore":      u.QualityScore,
		"validationVerdict": u.Verdict,
		"wiComputedAt":      u.ComputedAt,
	}
	if u.Reason != "" {
		set["validationReason"] = u.Reason
	}
	return map[string]any{"$set": set}
}

// UpdateWeighting writes the weighting fields for one feedback record by _id.
func (r *Repository) UpdateWeighting(ctx context.Context, feedbackID string, u WeightingUpdate) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": feedbackID}, buildWeightingUpdate(u))
	return err
}

// GradeBackfill is one feedback's recomputed grade fields for BulkBackfillGrades.
// It mirrors WeightingUpdate's $set shape; ComputedAt is the cycle timestamp.
type GradeBackfill struct {
	ID           string
	Wi           float64
	QualityScore float64
	Verdict      string // "valid" | "junk" | "missing_fields" | "self"
	Reason       string // optional detail when verdict != valid
	ComputedAt   int64  // Unix seconds; written to wiComputedAt
}

// BulkBackfillGrades writes wi / qualityScore / validationVerdict / validationReason /
// wiComputedAt for many feedback records by _id using an unordered bulk write. The $set
// is idempotent (replay derives grades deterministically), so it is safe to retry.
func (r *Repository) BulkBackfillGrades(ctx context.Context, updates []GradeBackfill) error {
	if len(updates) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(updates))
	for _, u := range updates {
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": u.ID}).
			SetUpdate(buildWeightingUpdate(WeightingUpdate{
				Wi:           u.Wi,
				QualityScore: u.QualityScore,
				Verdict:      u.Verdict,
				Reason:       u.Reason,
				ComputedAt:   u.ComputedAt,
			})))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("feedback repo: bulk backfill grades: %w", err)
	}
	return nil
}

// UpdateFallback writes the LLM fallback classification and effective category/feature for one feedback record by _id.
func (r *Repository) UpdateFallback(ctx context.Context, feedbackID string, f FallbackClassification) error {
	set := bson.M{
		"classification.fallback": f,
		"category":                f.Category,
	}
	if f.Feature != "" {
		set["feature"] = f.Feature
	}
	_, err := r.UpdateOne(ctx, bson.M{"_id": feedbackID}, bson.M{"$set": set})
	return err
}
