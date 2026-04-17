package score

// queries.go — Read-side aggregations for /agents/:id/score-history and rising stars.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// SnapshotPage wraps the projected snapshots for an agent within a time range.
type SnapshotPage struct {
	ChainID   int64               `bson:"chainId"`
	AgentID   string              `bson:"agentId"`
	Snapshots []ScoreSnapshotItem `bson:"score_snapshot"`
	Total     int64               `bson:"totalSnapshots"`
}

// FindSnapshotsInRange returns the score_snapshot items for an agent filtered by
// [from, to] Unix-second range. If both from and to are zero, returns the full array.
// `limit` caps the number of items returned (0 = no cap).
func (r *Repository) FindSnapshotsInRange(ctx context.Context, chainID int64, agentID string, from, to int64, limit int64) (*SnapshotPage, error) {
	match := bson.M{"chainId": chainID, "agentId": agentID}

	filterSnap := bson.M{}
	if from > 0 {
		filterSnap["$gte"] = bson.A{"$$s.timestamp", from}
	}
	if to > 0 {
		filterSnap["$lte"] = bson.A{"$$s.timestamp", to}
	}

	var condExpr any = true
	if from > 0 && to > 0 {
		condExpr = bson.M{"$and": bson.A{
			bson.M{"$gte": bson.A{"$$s.timestamp", from}},
			bson.M{"$lte": bson.A{"$$s.timestamp", to}},
		}}
	} else if from > 0 {
		condExpr = bson.M{"$gte": bson.A{"$$s.timestamp", from}}
	} else if to > 0 {
		condExpr = bson.M{"$lte": bson.A{"$$s.timestamp", to}}
	}

	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$project", Value: bson.M{
			"chainId": 1,
			"agentId": 1,
			"score_snapshot": bson.M{
				"$filter": bson.M{
					"input": "$score_snapshot",
					"as":    "s",
					"cond":  condExpr,
				},
			},
		}}},
	}
	if limit > 0 {
		pipeline = append(pipeline,
			bson.D{{Key: "$project", Value: bson.M{
				"chainId":        1,
				"agentId":        1,
				"totalSnapshots": bson.M{"$size": "$score_snapshot"},
				"score_snapshot": bson.M{"$slice": bson.A{"$score_snapshot", -limit}},
			}}},
		)
	} else {
		pipeline = append(pipeline,
			bson.D{{Key: "$project", Value: bson.M{
				"chainId":        1,
				"agentId":        1,
				"totalSnapshots": bson.M{"$size": "$score_snapshot"},
				"score_snapshot": 1,
			}}},
		)
	}

	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("score repo: find snapshots in range: %w", err)
	}
	defer cur.Close(ctx)
	var out []SnapshotPage
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("score repo: decode snapshots: %w", err)
	}
	if len(out) == 0 {
		return &SnapshotPage{ChainID: chainID, AgentID: agentID}, nil
	}
	return &out[0], nil
}

// RisingStar represents a single rising-star row (§2.3).
type RisingStar struct {
	AgentID     string  `bson:"agentId"`
	ChainID     int64   `bson:"chainId"`
	ScoreBefore float64 `bson:"scoreBefore"`
	ScoreNow    float64 `bson:"scoreNow"`
	Delta       float64 `bson:"delta"`
}

// FindRisingStars returns agents with the largest (scoreNow − scoreBefore) over
// the given [since, now] range. Uses the score_snapshot array only; callers join
// to agents separately for name/image.
func (r *Repository) FindRisingStars(ctx context.Context, chainID int64, sinceUnix int64, limit int64) ([]RisingStar, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID}}},
		{{Key: "$project", Value: bson.M{
			"agentId": 1,
			"chainId": 1,
			"before":  bson.M{"$filter": bson.M{"input": "$score_snapshot", "as": "s", "cond": bson.M{"$lt": bson.A{"$$s.timestamp", sinceUnix}}}},
			"after":   bson.M{"$filter": bson.M{"input": "$score_snapshot", "as": "s", "cond": bson.M{"$gte": bson.A{"$$s.timestamp", sinceUnix}}}},
		}}},
		{{Key: "$match", Value: bson.M{
			"$expr": bson.M{"$and": bson.A{
				bson.M{"$gt": bson.A{bson.M{"$size": "$before"}, 0}},
				bson.M{"$gt": bson.A{bson.M{"$size": "$after"}, 0}},
			}},
		}}},
		{{Key: "$project", Value: bson.M{
			"agentId":     1,
			"chainId":     1,
			"scoreBefore": bson.M{"$arrayElemAt": bson.A{"$before.agent_score", -1}},
			"scoreNow":    bson.M{"$arrayElemAt": bson.A{"$after.agent_score", -1}},
		}}},
		{{Key: "$project", Value: bson.M{
			"agentId":     1,
			"chainId":     1,
			"scoreBefore": 1,
			"scoreNow":    1,
			"delta":       bson.M{"$subtract": bson.A{"$scoreNow", "$scoreBefore"}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "delta", Value: -1}}}},
		{{Key: "$limit", Value: limit}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("score repo: rising stars aggregate: %w", err)
	}
	defer cur.Close(ctx)
	var out []RisingStar
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("score repo: rising stars decode: %w", err)
	}
	return out, nil
}
