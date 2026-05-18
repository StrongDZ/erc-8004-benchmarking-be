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
// the stored reputationScore is a pre-decay quantity.
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
	Owner    string // exact owner address filter (case-insensitive)
}

// LeaderboardSort selects the Mongo sort order. Score ordering is done server-side on
// reputationScore (a close proxy for the displayed score before decay/penalty).
type LeaderboardSort string

const (
	SortScoreDesc LeaderboardSort = "score_desc"
	SortScoreAsc  LeaderboardSort = "score_asc"
	SortTasksDesc LeaderboardSort = "tasks_desc"
	SortRecent    LeaderboardSort = "recent"
)

// FindLeaderboard returns a filtered, sorted, paginated slice of agents + total count.
// `limit` is capped by the caller; this function does not clamp.
//
// For score-based sorts (SortScoreDesc / SortScoreAsc) the method performs a two-phase query:
//   Phase 1: query agent_score_stats sorted by compositeScore (or totalTasks) → get ordered agentIDs.
//   Phase 2: bulk-fetch agent docs by agentID ∈ list, preserve stats-side order.
//
// For SortRecent the sort is on the agent document's createdAt field (no stats needed).
func (r *Repository) FindLeaderboard(ctx context.Context, f LeaderboardFilter, sortOrder LeaderboardSort, skip, limit int64) ([]AgentDocument, int64, error) {
	query := buildLeaderboardQuery(f)

	total, err := r.Count(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("agent repo: leaderboard count: %w", err)
	}

	switch sortOrder {
	case SortRecent:
		opts := options.Find().
			SetSort(bson.D{{Key: "createdAt", Value: -1}}).
			SetSkip(skip).SetLimit(limit)
		docs, err := r.Find(ctx, query, opts)
		if err != nil {
			return nil, 0, fmt.Errorf("agent repo: leaderboard find: %w", err)
		}
		return docs, total, nil

	default:
		// Score / tasks sorts: delegate ordering to agent_score_stats.
		if r.StatsColl == nil {
			// Fallback: unordered scan (no stats collection available).
			opts := options.Find().SetSkip(skip).SetLimit(limit)
			docs, err := r.Find(ctx, query, opts)
			if err != nil {
				return nil, 0, fmt.Errorf("agent repo: leaderboard find (no stats): %w", err)
			}
			return docs, total, nil
		}

		var statsSortDoc bson.D
		switch sortOrder {
		case SortScoreAsc:
			statsSortDoc = bson.D{{Key: "compositeScore", Value: 1}}
		case SortTasksDesc:
			statsSortDoc = bson.D{{Key: "totalTasks", Value: -1}}
		default: // SortScoreDesc
			statsSortDoc = bson.D{{Key: "compositeScore", Value: -1}}
		}

		// Build stats-side chain filter from the agent query.
		statsFilter := bson.M{}
		if v, ok := query["chainId"]; ok {
			statsFilter["chainId"] = v
		}

		statsOpts := options.Find().
			SetSort(statsSortDoc).
			SetSkip(skip).
			SetLimit(limit).
			SetProjection(bson.M{"chainId": 1, "agentId": 1})
		cur, err := r.StatsColl.Find(ctx, statsFilter, statsOpts)
		if err != nil {
			return nil, 0, fmt.Errorf("agent repo: leaderboard stats sort: %w", err)
		}
		defer cur.Close(ctx)

		type statsRow struct {
			ChainID int64  `bson:"chainId"`
			AgentID string `bson:"agentId"`
		}
		var rows []statsRow
		if err := cur.All(ctx, &rows); err != nil {
			return nil, 0, fmt.Errorf("agent repo: leaderboard stats decode: %w", err)
		}
		if len(rows) == 0 {
			return nil, total, nil
		}

		// Determine effective chainID (all rows share the same chain in normal usage).
		chainID := rows[0].ChainID
		ids := make([]string, 0, len(rows))
		posMap := make(map[string]int, len(rows))
		for i, row := range rows {
			ids = append(ids, row.AgentID)
			posMap[row.AgentID] = i
		}

		agentDocs, err := r.Find(ctx, bson.M{"chainId": chainID, "agentId": bson.M{"$in": ids}})
		if err != nil {
			return nil, 0, fmt.Errorf("agent repo: leaderboard agent fetch: %w", err)
		}

		// Restore stats-side order.
		ordered := make([]AgentDocument, len(rows))
		for _, d := range agentDocs {
			if pos, ok := posMap[d.AgentID]; ok {
				ordered[pos] = d
			}
		}
		// Compact out zero-value slots (agents missing from collection).
		out := ordered[:0]
		for _, d := range ordered {
			if d.AgentID != "" {
				out = append(out, d)
			}
		}
		return out, total, nil
	}
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
		rx := bson.M{"$regex": pattern, "$options": "i"}
		// Match name/description, substring of agentId, or any OASF skill/domain path segment.
		q["$or"] = bson.A{
			bson.M{"name": rx},
			bson.M{"description": rx},
			bson.M{"agentId": rx},
			bson.M{"oasfSkills": rx},
			bson.M{"oasfDomains": rx},
		}
	}
	if o := strings.TrimSpace(f.Owner); o != "" {
		q["owner"] = o
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
		SetSort(bson.D{{Key: "compositeScore", Value: -1}}).
		SetLimit(limit)
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("agent repo: search by name: %w", err)
	}
	return docs, nil
}

// Stats bundles the metrics required by GET /leaderboard/stats (§2.4).
type Stats struct {
	TotalAgents      int64
	ActiveAgents     int64
	AvgAccScore      float64
	MedianAccScore   float64
	Top10AccScoreAvg float64
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

// FindRelated returns agents that share any OASF skill or domain with the given agent,
// ordered by reputationScore desc, excluding the agent itself.
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
		SetSort(bson.D{{Key: "reputationScore", Value: -1}}).
		SetLimit(limit)
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("agent repo: find related: %w", err)
	}
	return docs, nil
}
