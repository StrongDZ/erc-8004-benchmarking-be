package agent

// queries.go — Read-side aggregations & search used by the REST API.
// All stored agentId values remain strings (uint256-safe) per spec §3.7.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// LeaderboardFilter expresses the server-side filterable attributes of /leaderboard.
// Score-based ("minScore") filters are applied in the handler after lazy decay because
// the stored accumulatedScore is a pre-decay quantity.
type LeaderboardFilter struct {
	ChainID  int64    // deprecated single-chain shortcut; prefer ChainIDs
	ChainIDs []int64  // OR logic across chainId; overrides ChainID when non-empty
	Skills   []string // AND logic across oasfSkills
	Domains  []string // AND logic across oasfDomains
	Services []string // OR logic across services.name (case-insensitive)
	Tags     []string // OR logic across tags
	X402     *bool    // x402Support filter
	HasOASF  *bool
	Active   *bool
	MinTasks int64  // totalTasks >= MinTasks when > 0
	Query    string // free-text regex on name / description
}

// LeaderboardSort selects the Mongo sort order. Score ordering is done server-side on
// accumulatedScore (a close proxy for the displayed score before decay/penalty).
type LeaderboardSort string

const (
	SortScoreDesc LeaderboardSort = "score_desc"
	SortScoreAsc  LeaderboardSort = "score_asc"
	SortTasksDesc LeaderboardSort = "tasks_desc"
	SortRecent    LeaderboardSort = "recent"
)

// FindLeaderboard returns a filtered, sorted, paginated slice of agents + total count.
// `limit` is capped by the caller; this function does not clamp.
func (r *Repository) FindLeaderboard(ctx context.Context, f LeaderboardFilter, sort LeaderboardSort, skip, limit int64) ([]AgentDocument, int64, error) {
	query := buildLeaderboardQuery(f)

	var sortDoc bson.D
	switch sort {
	case SortScoreAsc:
		sortDoc = bson.D{{Key: "accumulatedScore", Value: 1}}
	case SortTasksDesc:
		sortDoc = bson.D{{Key: "totalTasks", Value: -1}}
	case SortRecent:
		sortDoc = bson.D{{Key: "createdAt", Value: -1}}
	default:
		sortDoc = bson.D{{Key: "accumulatedScore", Value: -1}}
	}

	total, err := r.Count(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("agent repo: leaderboard count: %w", err)
	}

	opts := options.Find().SetSort(sortDoc).SetSkip(skip).SetLimit(limit)
	docs, err := r.Find(ctx, query, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("agent repo: leaderboard find: %w", err)
	}
	return docs, total, nil
}

func buildLeaderboardQuery(f LeaderboardFilter) bson.M {
	q := bson.M{}
	if len(f.ChainIDs) > 0 {
		if len(f.ChainIDs) == 1 {
			q["chainId"] = f.ChainIDs[0]
		} else {
			q["chainId"] = bson.M{"$in": f.ChainIDs}
		}
	} else if f.ChainID > 0 {
		q["chainId"] = f.ChainID
	}
	if len(f.Skills) > 0 {
		q["oasfSkills"] = bson.M{"$all": f.Skills}
	}
	if len(f.Domains) > 0 {
		q["oasfDomains"] = bson.M{"$all": f.Domains}
	}
	if len(f.Services) > 0 {
		lowered := make([]string, 0, len(f.Services))
		for _, s := range f.Services {
			s = strings.ToLower(strings.TrimSpace(s))
			if s != "" {
				lowered = append(lowered, s)
			}
		}
		if len(lowered) > 0 {
			q["services.name"] = bson.M{"$in": lowered}
		}
	}
	if len(f.Tags) > 0 {
		lowered := make([]string, 0, len(f.Tags))
		for _, t := range f.Tags {
			t = strings.ToLower(strings.TrimSpace(t))
			if t != "" {
				lowered = append(lowered, t)
			}
		}
		if len(lowered) > 0 {
			q["tags"] = bson.M{"$in": lowered}
		}
	}
	if f.X402 != nil {
		q["x402Support"] = *f.X402
	}
	if f.HasOASF != nil {
		q["hasOASF"] = *f.HasOASF
	}
	if f.Active != nil {
		q["active"] = *f.Active
	}
	if f.MinTasks > 0 {
		q["totalTasks"] = bson.M{"$gte": f.MinTasks}
	}
	if strings.TrimSpace(f.Query) != "" {
		pattern := regexp.QuoteMeta(strings.TrimSpace(f.Query))
		q["$or"] = bson.A{
			bson.M{"name": bson.M{"$regex": pattern, "$options": "i"}},
			bson.M{"description": bson.M{"$regex": pattern, "$options": "i"}},
		}
	}
	return q
}

// SearchByNamePrefix is a lightweight autocomplete helper. Returns up to `limit` agents
// whose name starts with the (case-insensitive) query prefix.
func (r *Repository) SearchByNamePrefix(ctx context.Context, chainID int64, query string, limit int64) ([]AgentDocument, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, nil
	}
	pattern := "^" + regexp.QuoteMeta(q)
	filter := bson.M{
		"chainId": chainID,
		"name":    bson.M{"$regex": pattern, "$options": "i"},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "accumulatedScore", Value: -1}}).
		SetLimit(limit)
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("agent repo: search by name: %w", err)
	}
	return docs, nil
}

// Stats bundles the metrics required by GET /leaderboard/stats (§2.4).
type Stats struct {
	TotalAgents       int64
	ActiveAgents      int64
	AvgAccScore       float64
	MedianAccScore    float64
	Top10AccScoreAvg  float64
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
			"avg": bson.M{"$avg": "$accumulatedScore"},
		}}},
	}
	avg, err := runScalarAgg(ctx, r.Coll, avgPipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats avg: %w", err)
	}

	top10Pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$sort", Value: bson.D{{Key: "accumulatedScore", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$accumulatedScore"},
		}}},
	}
	top10, err := runScalarAgg(ctx, r.Coll, top10Pipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats top10: %w", err)
	}

	median, err := r.computeMedianAccScoreMulti(ctx, match, total)
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

func (r *Repository) computeMedianAccScoreMulti(ctx context.Context, match bson.M, total int64) (float64, error) {
	if total <= 0 {
		return 0, nil
	}
	mid := total / 2
	opts := options.Find().
		SetSort(bson.D{{Key: "accumulatedScore", Value: 1}}).
		SetSkip(mid).
		SetLimit(1).
		SetProjection(bson.M{"accumulatedScore": 1})
	docs, err := r.Find(ctx, match, opts)
	if err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	return docs[0].AccumulatedScore, nil
}

// TagCount is one entry of /leaderboard/tags aggregation output.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int64  `json:"count"`
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
		Count int64  `bson:"count"`
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
// All figures are based on accumulatedScore — the handler may replace them with
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
			"avg": bson.M{"$avg": "$accumulatedScore"},
		}}},
	}
	avg, err := runScalarAgg(ctx, r.Coll, avgPipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats avg: %w", err)
	}

	top10Pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID}}},
		{{Key: "$sort", Value: bson.D{{Key: "accumulatedScore", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
		{{Key: "$group", Value: bson.M{
			"_id": nil,
			"avg": bson.M{"$avg": "$accumulatedScore"},
		}}},
	}
	top10, err := runScalarAgg(ctx, r.Coll, top10Pipeline, "avg")
	if err != nil {
		return nil, fmt.Errorf("agent repo: stats top10: %w", err)
	}

	median, err := r.computeMedianAccScore(ctx, chainID, total)
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

// computeMedianAccScore uses $sort + $skip + $limit (O(n log n) in Mongo but simple).
// For large collections, consider precomputing on the decay worker.
func (r *Repository) computeMedianAccScore(ctx context.Context, chainID, total int64) (float64, error) {
	if total <= 0 {
		return 0, nil
	}
	mid := total / 2
	opts := options.Find().
		SetSort(bson.D{{Key: "accumulatedScore", Value: 1}}).
		SetSkip(mid).
		SetLimit(1).
		SetProjection(bson.M{"accumulatedScore": 1})
	docs, err := r.Find(ctx, bson.M{"chainId": chainID}, opts)
	if err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	return docs[0].AccumulatedScore, nil
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

// FindRelated returns agents that share any OASF skill or domain with the given agent,
// ordered by accumulatedScore desc, excluding the agent itself.
func (r *Repository) FindRelated(ctx context.Context, chainID int64, excludeAgentID string, skills, domains []string, limit int64) ([]AgentDocument, error) {
	if len(skills) == 0 && len(domains) == 0 {
		return nil, nil
	}
	or := bson.A{}
	if len(skills) > 0 {
		or = append(or, bson.M{"oasfSkills": bson.M{"$in": skills}})
	}
	if len(domains) > 0 {
		or = append(or, bson.M{"oasfDomains": bson.M{"$in": domains}})
	}
	filter := bson.M{
		"chainId": chainID,
		"agentId": bson.M{"$ne": excludeAgentID},
		"$or":     or,
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "accumulatedScore", Value: -1}}).
		SetLimit(limit)
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("agent repo: find related: %w", err)
	}
	return docs, nil
}
