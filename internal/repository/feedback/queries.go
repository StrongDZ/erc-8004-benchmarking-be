package feedback

// queries.go — Read-side aggregations used by the REST API.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/domain/serviceendpoint"
	"erc-8004-benchmarking-be/internal/utils"
)

// ListFilter controls pagination & filtering on /agents/:id/feedbacks (§3.3).
type ListFilter struct {
	ChainID          int64
	AgentID          string
	Category         string // "quality" | "quantity" | "junk" | "others"
	Status           string // "accepted" | "revoked" | "junk" | "anomalous"
	ServiceEndpoint  string // registered service endpoint; matches related feedback endpoints
	From             *time.Time
	To               *time.Time
	SortDesc         bool // false = ASC by blockNumber/logIndex
}

// ListFiltered returns filtered feedback records and the total count matching the filter.
func (r *Repository) ListFiltered(ctx context.Context, f ListFilter, skip, limit int64) ([]FeedbackRecord, int64, error) {
	query := buildFeedbackQuery(f)

	total, err := r.Count(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: filter count: %w", err)
	}

	sortDir := 1
	if f.SortDesc {
		sortDir = -1
	}
	opts := options.Find().
		SetSort(bson.D{
			{Key: "blockNumber", Value: sortDir},
			{Key: "logIndex", Value: sortDir},
		}).
		SetSkip(skip).
		SetLimit(limit)

	docs, err := r.Find(ctx, query, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: filter find: %w", err)
	}
	return docs, total, nil
}

func buildFeedbackQuery(f ListFilter) bson.M {
	q := bson.M{"chainId": f.ChainID, "agentId": f.AgentID}

	// A category constraint can come from two independent sources — the explicit
	// f.Category filter and the status branch (e.g. "accepted" → category ≠ junk).
	// Collect both and combine via $and so neither clobbers the other's "category"
	// map key. Contradictory combinations (e.g. category=quality + status=junk)
	// intentionally yield an empty result.
	var catConds []bson.M
	if f.Category != "" {
		catConds = append(catConds, bson.M{"category": f.Category})
	}
	switch f.Status {
	case "revoked":
		q["revokeTxHash"] = bson.M{"$exists": true, "$ne": ""}
	case "accepted":
		q["$or"] = bson.A{
			bson.M{"revokeTxHash": bson.M{"$exists": false}},
			bson.M{"revokeTxHash": ""},
		}
		catConds = append(catConds, bson.M{"category": bson.M{"$ne": "junk"}})
	case "junk":
		catConds = append(catConds, bson.M{"category": "junk"})
	case "anomalous":
		q["vi"] = 0
		catConds = append(catConds, bson.M{"category": bson.M{"$ne": "junk"}})
	}
	switch len(catConds) {
	case 1:
		// Single source — merge directly to keep the simple, index-friendly shape.
		for k, v := range catConds[0] {
			q[k] = v
		}
	case 2:
		q["$and"] = bson.A{catConds[0], catConds[1]}
	}
	if f.From != nil || f.To != nil {
		rng := bson.M{}
		if f.From != nil {
			rng["$gte"] = f.From.Unix()
		}
		if f.To != nil {
			rng["$lte"] = f.To.Unix()
		}
		q["timestamp"] = rng
	}
	if strings.TrimSpace(f.ServiceEndpoint) != "" {
		if clause := serviceendpoint.EndpointRelatedFilter(f.ServiceEndpoint); clause != nil {
			q = bson.M{"$and": bson.A{q, clause}}
		}
	}
	return q
}

// ListPenalties returns feedback rows that count as "red flags": low vi OR revoked.
// `mode` ∈ {"fail", "revoked", "both"} — any other value is treated as "both".
func (r *Repository) ListPenalties(ctx context.Context, chainID int64, agentID, mode string, skip, limit int64) ([]FeedbackRecord, int64, error) {
	var ored bson.A
	switch mode {
	case "fail":
		ored = bson.A{bson.M{"vi": bson.M{"$lt": 0.5}}}
	case "revoked":
		ored = bson.A{bson.M{"revokeTxHash": bson.M{"$exists": true, "$ne": ""}}}
	default:
		ored = bson.A{
			bson.M{"vi": bson.M{"$lt": 0.5}},
			bson.M{"revokeTxHash": bson.M{"$exists": true, "$ne": ""}},
		}
	}
	query := bson.M{
		"chainId": chainID,
		"agentId": agentID,
		"$or":     ored,
	}
	total, err := r.Count(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: penalties count: %w", err)
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "blockNumber", Value: -1}, {Key: "logIndex", Value: -1}}).
		SetSkip(skip).
		SetLimit(limit)
	docs, err := r.Find(ctx, query, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: penalties find: %w", err)
	}
	return docs, total, nil
}

// ClassDistribution counts feedback grouped by classification.category for one agent.
func (r *Repository) ClassDistribution(ctx context.Context, chainID int64, agentID string) (map[string]int64, error) {
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{"chainId": chainID, "agentId": agentID}}},
		{{Key: "$group", Value: bson.M{
			"_id":   "$category",
			"count": bson.M{"$sum": 1},
		}}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("feedback repo: class distribution aggregate: %w", err)
	}
	defer cur.Close(ctx)

	out := map[string]int64{}
	var rows []bson.M
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("feedback repo: class distribution decode: %w", err)
	}
	for _, row := range rows {
		key, _ := row["_id"].(string)
		if key == "" {
			key = "unknown"
		}
		switch c := row["count"].(type) {
		case int32:
			out[key] = int64(c)
		case int64:
			out[key] = c
		case float64:
			out[key] = int64(c)
		}
	}
	return out, nil
}

// HeatmapDay is one bucket of the activity heatmap (§3.7).
type HeatmapDay struct {
	Date   string `json:"date"` // YYYY-MM-DD UTC
	Count  int64  `json:"count"`
	Passed int64  `json:"passed"`
	Failed int64  `json:"failed"`
}

// ActivityHeatmap aggregates feedback counts per UTC day for the last `days` days.
// If `days <= 0`, defaults to 365.
func (r *Repository) ActivityHeatmap(ctx context.Context, chainID int64, agentID string, days int) ([]HeatmapDay, error) {
	if days <= 0 {
		days = 365
	}
	cutoff := time.Now().AddDate(0, 0, -days).Unix()
	pipeline := mongodrv.Pipeline{
		{{Key: "$match", Value: bson.M{
			"chainId":   chainID,
			"agentId":   agentID,
			"timestamp": bson.M{"$gte": cutoff},
		}}},
		{{Key: "$project", Value: bson.M{
			"ts":     bson.M{"$toDate": bson.M{"$multiply": bson.A{"$timestamp", 1000}}},
			"passed": bson.M{"$cond": bson.A{bson.M{"$gte": bson.A{"$vi", 0.5}}, 1, 0}},
			"failed": bson.M{"$cond": bson.A{bson.M{"$lt": bson.A{"$vi", 0.5}}, 1, 0}},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{"$dateToString": bson.M{
				"format":   "%Y-%m-%d",
				"date":     "$ts",
				"timezone": "UTC",
			}},
			"count":  bson.M{"$sum": 1},
			"passed": bson.M{"$sum": "$passed"},
			"failed": bson.M{"$sum": "$failed"},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: -1}}}},
	}
	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("feedback repo: heatmap aggregate: %w", err)
	}
	defer cur.Close(ctx)

	var rows []struct {
		ID     string `bson:"_id"`
		Count  int64  `bson:"count"`
		Passed int64  `bson:"passed"`
		Failed int64  `bson:"failed"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("feedback repo: heatmap decode: %w", err)
	}
	out := make([]HeatmapDay, 0, len(rows))
	for _, r := range rows {
		out = append(out, HeatmapDay{Date: r.ID, Count: r.Count, Passed: r.Passed, Failed: r.Failed})
	}
	return out, nil
}

// TotalCount returns the total number of feedback rows across the given chain.
// Used by GET /leaderboard/stats.
func (r *Repository) TotalCount(ctx context.Context, chainID int64) (int64, error) {
	return r.Count(ctx, bson.M{"chainId": chainID})
}

// TotalCountMulti counts feedback rows across multiple chains (OR). Empty slice
// means "all chains".
func (r *Repository) TotalCountMulti(ctx context.Context, chainIDs []int64) (int64, error) {
	q := bson.M{}
	if len(chainIDs) == 1 {
		q["chainId"] = chainIDs[0]
	} else if len(chainIDs) > 1 {
		q["chainId"] = bson.M{"$in": chainIDs}
	}
	return r.Count(ctx, q)
}

// Count24h returns the number of feedback rows ingested in the last 24h (any chain
// when chainID <= 0; per chain otherwise).
func (r *Repository) Count24h(ctx context.Context, chainID int64) (int64, error) {
	cutoff := time.Now().Add(-24 * time.Hour).Unix()
	q := bson.M{"timestamp": bson.M{"$gte": cutoff}}
	if chainID > 0 {
		q["chainId"] = chainID
	}
	return r.Count(ctx, q)
}

// ListForReputationHistory returns non-revoked, non-self feedback for the agent,
// sorted oldest-first (blockNumber asc, logIndex asc). Caller iterates to re-derive
// the reputation timeline. Capped at 50000 docs to bound work.
func (r *Repository) ListForReputationHistory(ctx context.Context, chainID int64, agentID string) ([]FeedbackRecord, error) {
	q := bson.M{
		"chainId":        chainID,
		"agentId":        agentID,
		"isRevoked":      bson.M{"$ne": true},
		"isSelfFeedback": bson.M{"$ne": true},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "blockNumber", Value: 1}, {Key: "logIndex", Value: 1}}).
		SetLimit(50000)
	docs, err := r.Find(ctx, q, opts)
	if err != nil {
		return nil, fmt.Errorf("feedback repo: list for reputation history: %w", err)
	}
	return docs, nil
}

// ListByClientAddress returns paginated feedback submitted by a specific wallet address,
// sorted newest-first (blockNumber desc). Used by GET /wallet/:address/feedbacks.
func (r *Repository) ListByClientAddress(ctx context.Context, address string, skip, limit int64) ([]FeedbackRecord, int64, error) {
	q := bson.M{"clientAddress": utils.NormalizeAddress(address)}

	total, err := r.Count(ctx, q)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: wallet feedbacks count: %w", err)
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "blockNumber", Value: -1}, {Key: "logIndex", Value: -1}}).
		SetSkip(skip).
		SetLimit(limit)

	docs, err := r.Find(ctx, q, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("feedback repo: wallet feedbacks find: %w", err)
	}
	return docs, total, nil
}
