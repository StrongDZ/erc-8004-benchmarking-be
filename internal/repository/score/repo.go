package score

// repo.go — ScoreHistoryRepository for the "score_history" MongoDB collection.
// Agent-centric design: one document per agent, with score_snapshot array.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[ScoreHistory](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the score_history collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_agent").SetUnique(true),
		},
	})
	return err
}

// AppendSnapshot appends a single snapshot to the agent's score_snapshot array.
// If the document doesn't exist, it creates one with the snapshot as the first element.
func (r *Repository) AppendSnapshot(ctx context.Context, chainID int64, agentID string, snapshot ScoreSnapshotItem) error {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	update := bson.M{
		"$push": bson.M{
			"score_snapshot": snapshot,
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := r.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("score repo: append snapshot: %w", err)
	}
	return nil
}

// BulkAppendSnapshots appends multiple snapshots in a single batch.
// Each snapshot is keyed by (chainID, agentID) and will be appended to the respective document.
func (r *Repository) BulkAppendSnapshots(ctx context.Context, items []struct {
	ChainID  int64
	AgentID  string
	Snapshot ScoreSnapshotItem
}) error {
	if len(items) == 0 {
		return nil
	}

	var models []mongodrv.WriteModel

	for _, item := range items {
		filter := bson.M{"chainId": item.ChainID, "agentId": item.AgentID}
		update := bson.M{
			"$push": bson.M{
				"score_snapshot": item.Snapshot,
			},
		}
		models = append(models, mongodrv.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := r.BulkWrite(ctx, models, opts)
	if err != nil {
		return fmt.Errorf("score repo: bulk append snapshots: %w", err)
	}
	return nil
}

// GetByAgent returns the full score history document for an agent.
func (r *Repository) GetByAgent(ctx context.Context, chainID int64, agentID string) (*ScoreHistory, error) {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	result, err := r.FindOne(ctx, filter)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("score repo: get by agent: %w", err)
	}
	return &result, nil
}

// ListByAgent returns the agent's score history with optional limit on snapshot array size (newest first).
func (r *Repository) ListByAgent(ctx context.Context, chainID int64, agentID string, limit int64) (*ScoreHistory, error) {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	projection := bson.M{
		"score_snapshot": bson.M{
			"$slice": -int(limit), // last N elements (newest)
		},
	}
	opts := options.FindOne().SetProjection(projection)

	result, err := r.FindOne(ctx, filter, opts)
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("score repo: list by agent: %w", err)
	}
	return &result, nil
}
