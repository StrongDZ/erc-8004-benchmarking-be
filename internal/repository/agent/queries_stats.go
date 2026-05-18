package agent

// queries_stats.go — Stats aggregations: leaderboard stats, tag counts.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Stats bundles the metrics required by GET /leaderboard/stats (§2.4).
type Stats struct {
	TotalAgents      int64
	ActiveAgents     int64
	AvgAccScore      float64
	MedianAccScore   float64
	Top10AccScoreAvg float64
}

// TagCount is one entry of /leaderboard/tags aggregation output.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int64  `json:"count"`
}

// ComputeStatsMulti runs the same aggregations as ComputeStats but across a set of
// chain IDs. An empty slice means "all chains".
func (r *Repository) ComputeStatsMulti(ctx context.Context, chainIDs []int64) (*Stats, error) {
	match := bson.M{}
	if len(chainIDs) == 1 {
		match["chainId"] = chainIDs[0]
	} else if len(chainIDs) > 1 {
		match["chainId"] = bson.M{"$in": chainIDs}
	}

	total, err := r.Count(ctx, match)
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats total: %w", err)
	}
	activeMatch := bson.M{"active": true}
	for k, v := range match {
		activeMatch[k] = v
	}
	active, err := r.Count(ctx, activeMatch)
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats active: %w", err)
	}

	avgPipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$compositeScore"},
		}}},
	}
	avg, err := runScalarAgg(ctx, r.StatsColl, avgPipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats avg: %w", err)
	}

	top10Pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$sort", Value: bson.D{{Key: "compositeScore", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$compositeScore"},
		}}},
	}
	top10, err := runScalarAgg(ctx, r.StatsColl, top10Pipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats top10: %w", err)
	}

	median, err := r.computeMedianReputationScoreMulti(ctx, match, total)
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats median: %w", err)
	}

	return &Stats{
		TotalAgents:      total,
		ActiveAgents:     active,
		AvgAccScore:      avg,
		MedianAccScore:   median,
		Top10AccScoreAvg: top10,
	}, nil
}

func (r *Repository) computeMedianReputationScoreMulti(ctx context.Context, match bson.M, total int64) (float64, error) {
	if total <= 0 || r.StatsColl == nil {
		return 0, nil
	}
	mid := total / 2
	opts := options.Find().
		SetSort(bson.D{{Key: "compositeScore", Value: 1}}).
		SetSkip(mid).
		SetLimit(1).
		SetProjection(bson.M{"compositeScore": 1})
	cur, err := r.StatsColl.Find(ctx, match, opts)
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var row struct {
		CompositeScore float64 `bson:"compositeScore"`
	}
	if !cur.Next(ctx) {
		return 0, nil
	}
	if err := cur.Decode(&row); err != nil {
		return 0, err
	}
	return row.CompositeScore, nil
}

// TopTags returns the most common tags across agents matching the given chain
// scope. Empty chainIDs means "all chains". Results sorted by count desc.
func (r *Repository) TopTags(ctx context.Context, chainIDs []int64, query string, limit int64) ([]TagCount, error) {
	if limit <= 0 {
		limit = 50
	}
	match := bson.M{"tags": bson.M{"$exists": true, "$ne": nil}}
	if len(chainIDs) == 1 {
		match["chainId"] = chainIDs[0]
	} else if len(chainIDs) > 1 {
		match["chainId"] = bson.M{"$in": chainIDs}
	}

	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$unwind", Value: "$tags"}},
	}
	if q := strings.ToLower(strings.TrimSpace(query)); q != "" {
		pipeline = append(pipeline, bson.D{
			{Key: "$match", Value: bson.M{"tags": bson.M{"$regex": regexp.QuoteMeta(q), "$options": "i"}}},
		})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$group", Value: bson.M{"_id": "$tags", "count": bson.M{"$sum": 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}, {Key: "_id", Value: 1}}}},
		bson.D{{Key: "$limit", Value: limit}},
	)

	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("agent repo: top tags: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		ID    string `bson:"_id"`
		Count int64  `json:"count" bson:"count"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("agent repo: top tags decode: %w", err)
	}

	out := make([]TagCount, 0, len(rows))
	for _, r := range rows {
		out = append(out, TagCount{Tag: r.ID, Count: r.Count})
	}
	return out, nil
}

// ComputeStats runs three small aggregations over the agents collection for one chain.
// All figures are based on reputationScore — the handler may replace them with
// lazy-decayed display scores if needed (current impl returns raw accumulated).
func (r *Repository) ComputeStats(ctx context.Context, chainID int64) (*Stats, error) {
	total, err := r.Count(ctx, bson.M{"chainId": chainID})
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats total: %w", err)
	}
	active, err := r.Count(ctx, bson.M{"chainId": chainID, "active": true})
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats active: %w", err)
	}

	avgPipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID}}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$compositeScore"},
		}}},
	}
	avg, err := runScalarAgg(ctx, r.StatsColl, avgPipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats avg: %w", err)
	}

	top10Pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID}}},
		{{Key: "$sort", Value: bson.D{{Key: "compositeScore", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$compositeScore"},
		}}},
	}
	top10, err := runScalarAgg(ctx, r.StatsColl, top10Pipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats top10: %w", err)
	}

	median, err := r.computeMedianReputationScore(ctx, chainID, total)
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats median: %w", err)
	}

	return &Stats{
		TotalAgents:      total,
		ActiveAgents:     active,
		AvgAccScore:      avg,
		MedianAccScore:   median,
		Top10AccScoreAvg: top10,
	}, nil
}

// computeMedianReputationScore uses $sort + $skip + $limit (O(n log n) in Mongo but simple).
// For large collections, consider precomputing on the decay worker.
func (r *Repository) computeMedianReputationScore(ctx context.Context, chainID, total int64) (float64, error) {
	if total <= 0 || r.StatsColl == nil {
		return 0, nil
	}
	mid := total / 2
	opts := options.Find().
		SetSort(bson.D{{Key: "compositeScore", Value: 1}}).
		SetSkip(mid).
		SetLimit(1).
		SetProjection(bson.M{"compositeScore": 1})
	cur, err := r.StatsColl.Find(ctx, bson.M{"chainId": chainID}, opts)
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var row struct {
		CompositeScore float64 `bson:"compositeScore"`
	}
	if !cur.Next(ctx) {
		return 0, nil
	}
	if err := cur.Decode(&row); err != nil {
		return 0, err
	}
	return row.CompositeScore, nil
}

// runScalarAgg runs a pipeline whose final $group emits one document with a numeric
// `field` value, and returns that value as float64. Returns 0 if the pipeline is empty.
func runScalarAgg(ctx context.Context, coll *mongodrv.Collection, pipeline mongodrv.Pipeline, field string) (float64, error) {
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var out []bson.M
	if err := cur.All(ctx, &out); err != nil {
		return 0, err
	}
	if len(out) == 0 {
		return 0, nil
	}
	v, ok := out[0][field]
	if !ok || v == nil {
		return 0, nil
	}
	switch n := v.(type) {
	case float64:
		return n, nil
	case int32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, nil
	}
}
