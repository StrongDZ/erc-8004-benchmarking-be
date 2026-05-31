package snapshot

// reader.go — Read snapshot collections back from bachelor_sensitivity_<id> DB.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// Reader loads collections from a single snapshot database.
type Reader struct {
	client *mongodrv.Client
	dbName string
}

// NewReader returns a Reader bound to the snapshot ID.
func NewReader(client *mongodrv.Client, snapshotID string) *Reader {
	return &Reader{client: client, dbName: SnapshotDBName(snapshotID)}
}

func (r *Reader) ListAgents(ctx context.Context) ([]AgentSnapshot, error) {
	return listAll[AgentSnapshot](ctx, r.client.Database(r.dbName).Collection(CollAgents))
}

func (r *Reader) ListFeedbacks(ctx context.Context) ([]FeedbackSnapshot, error) {
	return listAll[FeedbackSnapshot](ctx, r.client.Database(r.dbName).Collection(CollFeedbacks))
}

func (r *Reader) ListEdges(ctx context.Context) ([]GraphEdge, error) {
	return listAll[GraphEdge](ctx, r.client.Database(r.dbName).Collection(CollGraphEdges))
}

func (r *Reader) ListBaseline(ctx context.Context) ([]BaselineScore, error) {
	return listAll[BaselineScore](ctx, r.client.Database(r.dbName).Collection(CollBaseline))
}

// FilterConfig reads the persisted filter from the metadata singleton.
// Returns DefaultFilter() if not found.
func (r *Reader) FilterConfig(ctx context.Context) (FilterConfig, error) {
	var doc struct {
		Filter FilterConfig `bson:"filter"`
	}
	err := r.client.Database(r.dbName).Collection(CollSnapshotMeta).
		FindOne(ctx, bson.M{"_id": "filter"}).Decode(&doc)
	if err == mongodrv.ErrNoDocuments {
		return DefaultFilter(), nil
	}
	if err != nil {
		return FilterConfig{}, err
	}
	return doc.Filter, nil
}

func listAll[T any](ctx context.Context, coll *mongodrv.Collection) ([]T, error) {
	cur, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []T
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
