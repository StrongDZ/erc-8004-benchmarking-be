package tagstats

// types.go — types for tag_value_stats and changed_tag_scales collections.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// TierCounts tracks how many service_feedback values fell into each scale tier.
// Tier is assigned from the real value (value / 10^valueDecimals).
type TierCounts struct {
	Binary    int64 `bson:"binary"`    // |real| <= 1.0
	Star5     int64 `bson:"star5"`     // 1.0 < real <= 5.0
	Star10    int64 `bson:"star10"`    // 5.0 < real <= 10.0
	Pct100    int64 `bson:"pct100"`    // 10.0 < |real| <= 100.0
	Unbounded int64 `bson:"unbounded"` // |real| > 100
}

// TagPairKey returns the composite _id for a (tag1, tag2) pair.
// tag2 may be empty ("starred:" is a valid key).
func TagPairKey(tag1, tag2 string) string { return tag1 + ":" + tag2 }

// TagValueStats stores the running tier-vote counters for a (tag1, tag2) pair.
// One document per pair. _id == TagPairKey(tag1, tag2).
type TagValueStats struct {
	ID            string     `bson:"_id"`
	Tag1          string     `bson:"tag1"`
	Tag2          string     `bson:"tag2"`
	Count         int64      `bson:"count"`         // total non-empty feedbacks
	EmptyCount    int64      `bson:"emptyCount"`    // feedbacks with value=""
	TierCounts    TierCounts `bson:"tierCounts"`
	DetectedScale string     `bson:"detectedScale"` // "" | "binary" | "star5" | "star10" | "pct100" | "unbounded"
	ScaleVersion  int64      `bson:"scaleVersion"`  // increments on each scale change
	LockedAt      int64      `bson:"lockedAt"`      // Unix seconds when scale first detected
	LastUpdatedAt int64      `bson:"lastUpdatedAt"`
}

// ScaleChangeCorrectionStatus is the lifecycle of a rescale correction job.
type ScaleChangeCorrectionStatus string

const (
	CorrectionPending    ScaleChangeCorrectionStatus = "pending"
	CorrectionInProgress ScaleChangeCorrectionStatus = "in_progress"
	CorrectionDone       ScaleChangeCorrectionStatus = "done"
	CorrectionFailed     ScaleChangeCorrectionStatus = "failed"
)

// ScaleChangeCorrection is written to changed_tag_scales when the detected scale for a
// (tag1, tag2) pair changes. It drives the rescale worker (Phase 1–4 retroactive correction).
// _id == "{tag1}:{tag2}:v{scaleVersion}".
type ScaleChangeCorrection struct {
	ID   string `bson:"_id"`
	Tag1 string `bson:"tag1"`
	Tag2 string `bson:"tag2"`

	OldScale string `bson:"oldScale"` // "" means first detection (null → newScale)
	NewScale string `bson:"newScale"`

	DetectedAt int64                       `bson:"detectedAt"` // feedbacks before this timestamp are re-scored
	Status     ScaleChangeCorrectionStatus `bson:"status"`

	// Idempotency tracking across phases.
	DeltaSnapshotReady bool   `bson:"deltaSnapshotReady"` // true after Phase 1 is complete
	LastAgentIDCursor  string `bson:"lastAgentIDCursor"`  // agentID of last successfully committed agent
	AgentsTotal        int64  `bson:"agentsTotal"`
	AgentsProcessed    int64  `bson:"agentsProcessed"`
	FeedbacksRescaled  int64  `bson:"feedbacksRescaled"`

	StartedAt  int64  `bson:"startedAt,omitempty"`
	FinishedAt int64  `bson:"finishedAt,omitempty"`
	ErrorMsg   string `bson:"errorMsg,omitempty"`
}

// RescaleDelta stores the pre-computed reputationScore delta for one agent, keyed to a correction.
// Written in Phase 1 (compute), consumed in Phase 2 (apply). One doc per (correctionID, agentID).
// _id == "{correctionID}:{agentID}".
type RescaleDelta struct {
	ID           string `bson:"_id"`
	CorrectionID string `bson:"correctionID"`
	AgentID      string `bson:"agentID"`
	ChainID      int64  `bson:"chainID"`
	Delta        float64 `bson:"delta"` // Σ wi*(vi_new - vi_old)*decay
	Applied      bool    `bson:"applied"`
}

// TierUpdate is an in-memory struct (not persisted) collected during Phase 2
// and flushed in Phase 3 of the trustrank batch.
type TierUpdate struct {
	Tag1    string
	Tag2    string
	Tier    string // "" when value is empty
	IsEmpty bool
}

// StatsRepository wraps the tag_value_stats collection.
type StatsRepository struct {
	mongorepo.MongoRepoImpl[TagValueStats]
}

// CorrectionRepository wraps the changed_tag_scales collection.
type CorrectionRepository struct {
	mongorepo.MongoRepoImpl[ScaleChangeCorrection]
}

// DeltaRepository wraps the rescale_deltas collection.
type DeltaRepository struct {
	mongorepo.MongoRepoImpl[RescaleDelta]
}
