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
		{
			Keys:    bson.D{{Key: "compositeScore", Value: -1}},
			Options: options.Index().SetName("idx_compositeScore_desc"),
		},
	})
	if err != nil {
		return fmt.Errorf("scorestats: ensure indexes: %w", err)
	}
	return nil
}

// UpsertFromWritePath writes the O(1) accumulator state + composite + breakdown
// produced by the trustrank processor per NewFeedback event.
// Preserves delta/consistency fields by using $set only on the provided fields.
func (r *Repository) UpsertFromWritePath(
	ctx context.Context,
	chainID int64, agentID string,
	reputationScore float64, scoreUpdateAt int64,
	consecutiveFails, totalTasks, totalPassed, totalFailed int64,
	composite, reputationNorm, services, publisher, compliance float64,
	serviceWarnings []string,
) error {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	update := bson.M{"$set": bson.M{
		"chainId":          chainID,
		"agentId":          agentID,
		"reputationScore":  reputationScore,
		"scoreUpdateAt":    scoreUpdateAt,
		"consecutiveFails": consecutiveFails,
		"totalTasks":       totalTasks,
		"totalPassed":      totalPassed,
		"totalFailed":      totalFailed,
		"compositeScore":   composite,
		"reputationNorm":   reputationNorm,
		"servicesScore":    services,
		"publisherScore":   publisher,
		"complianceScore":  compliance,
		"serviceWarnings":  serviceWarnings,
	}}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("scorestats: upsert from write path chainId=%d agentId=%s: %w", chainID, agentID, err)
	}
	return nil
}

// IncReputation atomically adds delta to reputationScore (used by rescale worker).
func (r *Repository) IncReputation(ctx context.Context, chainID int64, agentID string, delta float64) error {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	_, err := r.UpdateOne(ctx, filter, bson.M{"$inc": bson.M{"reputationScore": delta}})
	if err != nil {
		return fmt.Errorf("scorestats: inc reputation chainId=%d agentId=%s: %w", chainID, agentID, err)
	}
	return nil
}

// FindByID returns the stats doc or nil if not found.
func (r *Repository) FindByID(ctx context.Context, chainID int64, agentID string) (*AgentScoreStats, error) {
	return r.FindByAgentID(ctx, chainID, agentID)
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

// AgentCompositeScore is a lightweight projection for seed vector construction.
type AgentCompositeScore struct {
	ChainID        int64   `bson:"chainId"`
	AgentID        string  `bson:"agentId"`
	CompositeScore float64 `bson:"compositeScore"`
}

// ScanCompositeScores returns compositeScore for all agents. Pass chainID=0 for all chains.
func (r *Repository) ScanCompositeScores(ctx context.Context, chainID int64) ([]AgentCompositeScore, error) {
	filter := bson.M{}
	if chainID > 0 {
		filter["chainId"] = chainID
	}
	cur, err := r.Coll.Find(ctx, filter, options.Find().
		SetProjection(bson.M{
			"chainId": 1, "agentId": 1, "compositeScore": 1, "_id": 0,
		}).
		SetBatchSize(5000),
	)
	if err != nil {
		return nil, fmt.Errorf("scorestats: scan composite scores: %w", err)
	}
	defer cur.Close(ctx)

	var out []AgentCompositeScore
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("scorestats: decode composite scores: %w", err)
	}
	return out, nil
}
