package feedback

// queries_sidebar.go — Distinct wallet/agent lists for feedback sidebars.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/utils"
)

// DistinctClientRow is one wallet that submitted feedback to an agent.
type DistinctClientRow struct {
	ClientAddress string
	FeedbackCount int64
	LastTimestamp int64
}

// DistinctAgentRow is one agent that received feedback from a wallet.
type DistinctAgentRow struct {
	ChainID       int64
	AgentID       string
	FeedbackCount int64
	LastTimestamp int64
}

// CountDistinctClientsByAgent returns the number of unique clientAddress values
// that submitted feedback to the given agent.
func (r *Repository) CountDistinctClientsByAgent(ctx context.Context, chainID int64, agentID string) (int64, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID, "agentId": agentID}}},
		{{Key: "$group", Value: bson.M{"_id": "$clientAddress"}}},
		{{Key: "$count", Value: "total"}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("feedback repo: distinct clients count: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		Total int64 `bson:"total"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return 0, fmt.Errorf("feedback repo: distinct clients count decode: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Total, nil
}

// ListDistinctClientsByAgent returns paginated distinct client wallets for an agent,
// sorted by most recent feedback first.
func (r *Repository) ListDistinctClientsByAgent(ctx context.Context, chainID int64, agentID string, skip, limit int64) ([]DistinctClientRow, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID, "agentId": agentID}}},
		{{Key: "$group", Value: bson.M{
			"_id":           "$clientAddress",
			"feedbackCount": bson.M{"$sum": 1},
			"lastTimestamp": bson.M{"$max": "$timestamp"},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "lastTimestamp", Value: -1}}}},
		{{Key: "$skip", Value: skip}},
		{{Key: "$limit", Value: limit}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("feedback repo: distinct clients list: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		ID            string `bson:"_id"`
		FeedbackCount int64  `bson:"feedbackCount"`
		LastTimestamp int64  `bson:"lastTimestamp"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("feedback repo: distinct clients list decode: %w", err)
	}
	out := make([]DistinctClientRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, DistinctClientRow{
			ClientAddress: row.ID,
			FeedbackCount: row.FeedbackCount,
			LastTimestamp: row.LastTimestamp,
		})
	}
	return out, nil
}

// CountDistinctAgentsByClient returns the number of unique (chainId, agentId) pairs
// that received feedback from the given wallet.
func (r *Repository) CountDistinctAgentsByClient(ctx context.Context, address string) (int64, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"clientAddress": utils.NormalizeAddress(address)}}},
		{{Key: "$group", Value: bson.M{"_id": bson.M{"chainId": "$chainId", "agentId": "$agentId"}}}},
		{{Key: "$count", Value: "total"}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, fmt.Errorf("feedback repo: distinct agents count: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		Total int64 `bson:"total"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return 0, fmt.Errorf("feedback repo: distinct agents count decode: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Total, nil
}

// ListDistinctAgentsByClient returns paginated distinct agents that received feedback
// from the wallet, sorted by most recent feedback first.
func (r *Repository) ListDistinctAgentsByClient(ctx context.Context, address string, skip, limit int64) ([]DistinctAgentRow, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"clientAddress": utils.NormalizeAddress(address)}}},
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{
				"chainId": "$chainId",
				"agentId": "$agentId",
			},
			"feedbackCount": bson.M{"$sum": 1},
			"lastTimestamp": bson.M{"$max": "$timestamp"},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "lastTimestamp", Value: -1}}}},
		{{Key: "$skip", Value: skip}},
		{{Key: "$limit", Value: limit}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("feedback repo: distinct agents list: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		ID struct {
			ChainID int64  `bson:"chainId"`
			AgentID string `bson:"agentId"`
		} `bson:"_id"`
		FeedbackCount int64 `bson:"feedbackCount"`
		LastTimestamp int64 `bson:"lastTimestamp"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("feedback repo: distinct agents list decode: %w", err)
	}
	out := make([]DistinctAgentRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, DistinctAgentRow{
			ChainID:       row.ID.ChainID,
			AgentID:       row.ID.AgentID,
			FeedbackCount: row.FeedbackCount,
			LastTimestamp: row.LastTimestamp,
		})
	}
	return out, nil
}
