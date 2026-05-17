package scorestats

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
	m := mongorepo.NewMongoRepo[AgentScoreStats](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the agent_score_stats collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys:    bson.D{{Key: "chainId", Value: 1}, {Key: "agentId", Value: 1}},
			Options: options.Index().SetName("idx_chain_agent").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "chainId", Value: 1}, {Key: "delta24h", Value: -1}},
			Options: options.Index().SetName("idx_chain_delta24h"),
		},
		{
			Keys:    bson.D{{Key: "chainId", Value: 1}, {Key: "delta7d", Value: -1}},
			Options: options.Index().SetName("idx_chain_delta7d"),
		},
		{
			Keys:    bson.D{{Key: "chainId", Value: 1}, {Key: "delta30d", Value: -1}},
			Options: options.Index().SetName("idx_chain_delta30d"),
		},
	})
	if err != nil {
		return fmt.Errorf("scorestats: ensure indexes: %w", err)
	}
	return nil
}

// BulkUpsert replaces (or inserts) stats documents in an unordered bulk write.
func (r *Repository) BulkUpsert(ctx context.Context, stats []AgentScoreStats) error {
	if len(stats) == 0 {
		return nil
	}
	models := make([]mongodrv.WriteModel, len(stats))
	for i, s := range stats {
		models[i] = mongodrv.NewReplaceOneModel().
			SetFilter(bson.M{"chainId": s.ChainID, "agentId": s.AgentID}).
			SetReplacement(s).
			SetUpsert(true)
	}
	_, err := r.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("scorestats: bulk upsert: %w", err)
	}
	return nil
}

// FindByPeriodDelta returns the top `limit` agents sorted by the delta for the given period.
// period must be one of: "24h", "1d", "7d", "30d".
func (r *Repository) FindByPeriodDelta(ctx context.Context, chainID int64, period string, limit int64) ([]AgentScoreStats, error) {
	field := deltaField(period)
	opts := options.Find().
		SetSort(bson.D{{Key: field, Value: -1}}).
		SetLimit(limit)
	docs, err := r.Find(ctx, bson.M{"chainId": chainID}, opts)
	if err != nil {
		return nil, fmt.Errorf("scorestats: find by period delta: %w", err)
	}
	return docs, nil
}

// FindByAgentIDs returns stats for a set of agent IDs on a given chain.
func (r *Repository) FindByAgentIDs(ctx context.Context, chainID int64, agentIDs []string) ([]AgentScoreStats, error) {
	docs, err := r.Find(ctx, bson.M{"chainId": chainID, "agentId": bson.M{"$in": agentIDs}}, nil)
	if err != nil {
		return nil, fmt.Errorf("scorestats: find by agent ids: %w", err)
	}
	return docs, nil
}

// FindByAgentID returns the stats for a single agent, or nil if not found.
func (r *Repository) FindByAgentID(ctx context.Context, chainID int64, agentID string) (*AgentScoreStats, error) {
	doc, err := r.FindOne(ctx, bson.M{"chainId": chainID, "agentId": agentID})
	if err != nil {
		if err == mongodrv.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("scorestats: find by agent id: %w", err)
	}
	return &doc, nil
}

func deltaField(period string) string {
	switch period {
	case "7d":
		return "delta7d"
	case "30d":
		return "delta30d"
	default: // "24h", "1d"
		return "delta24h"
	}
}
