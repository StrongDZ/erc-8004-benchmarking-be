package snapshot

// writer.go — Write snapshot data into bachelor_sensitivity_<id> database.
// Each call creates the target database fresh (drops any existing data with same ID).

import (
	"context"
	"fmt"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// Collection names within a snapshot database.
const (
	CollAgents       = "agents"
	CollFeedbacks    = "feedbacks"
	CollGraphEdges   = "graph_edges"
	CollBaseline     = "baseline_scores"
	CollSnapshotMeta = "metadata"
)

// SnapshotData holds the four data collections plus the filter config used to build them.
type SnapshotData struct {
	Agents    []AgentSnapshot
	Feedbacks []FeedbackSnapshot
	Edges     []GraphEdge
	Baseline  []BaselineScore
	Filter    FilterConfig
}

// SnapshotDBName returns the database name for a given snapshot ID.
func SnapshotDBName(snapshotID string) string {
	return "bachelor_sensitivity_" + snapshotID
}

// NewSnapshotID builds a snapshot ID from current time. Format: snap_YYYYMMDD_HHMMSS.
func NewSnapshotID(at time.Time) string {
	return "snap_" + at.UTC().Format("20060102_150405")
}

// Writer writes data into a single snapshot database.
type Writer struct {
	client *mongodrv.Client
	dbName string
}

// NewWriter returns a Writer bound to a specific snapshot ID.
func NewWriter(client *mongodrv.Client, snapshotID string) *Writer {
	return &Writer{client: client, dbName: SnapshotDBName(snapshotID)}
}

// Write inserts all collections into the snapshot database. Existing database is dropped first.
func (w *Writer) Write(ctx context.Context, data SnapshotData) error {
	db := w.client.Database(w.dbName)
	if err := db.Drop(ctx); err != nil {
		return fmt.Errorf("drop existing %s: %w", w.dbName, err)
	}

	if err := insertMany(ctx, db.Collection(CollAgents), data.Agents); err != nil {
		return fmt.Errorf("insert agents: %w", err)
	}
	if err := insertMany(ctx, db.Collection(CollFeedbacks), data.Feedbacks); err != nil {
		return fmt.Errorf("insert feedbacks: %w", err)
	}
	if err := insertMany(ctx, db.Collection(CollGraphEdges), data.Edges); err != nil {
		return fmt.Errorf("insert edges: %w", err)
	}
	if err := insertMany(ctx, db.Collection(CollBaseline), data.Baseline); err != nil {
		return fmt.Errorf("insert baseline: %w", err)
	}
	// Persist filter config in a singleton metadata document.
	_, err := db.Collection(CollSnapshotMeta).InsertOne(ctx, map[string]any{
		"_id":    "filter",
		"filter": data.Filter,
	})
	if err != nil {
		return fmt.Errorf("insert filter meta: %w", err)
	}
	return nil
}

func insertMany[T any](ctx context.Context, coll *mongodrv.Collection, docs []T) error {
	if len(docs) == 0 {
		return nil
	}
	bulk := make([]any, len(docs))
	for i := range docs {
		bulk[i] = docs[i]
	}
	_, err := coll.InsertMany(ctx, bulk)
	return err
}
