package feedback

// repo_weighting.go — UpdateWeighting method: writes wi / qualityScore /
// validationVerdict / validationReason / wiComputedAt for a single feedback record.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
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

// UpdateFallback writes the LLM fallback classification for one feedback record by _id.
func (r *Repository) UpdateFallback(ctx context.Context, feedbackID string, f FallbackClassification) error {
	_, err := r.UpdateOne(ctx, bson.M{"_id": feedbackID}, bson.M{"$set": bson.M{"classification.fallback": f}})
	return err
}
