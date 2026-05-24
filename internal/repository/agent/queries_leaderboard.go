package agent

// queries_leaderboard.go — Leaderboard search, filtering, and related-agents queries.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
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
//
//	Phase 1: query agent_score_stats sorted by compositeScore (or totalTasks) → get ordered agentIDs.
//	Phase 2: bulk-fetch agent docs by agentID ∈ list, preserve stats-side order.
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
		//
		// IMPORTANT: When a free-text query is active, the stats collection only knows
		// chainId — it has no name/description/oasfSkills fields to filter against.
		// Running Phase-1 on stats would return the full sorted list ignoring the text
		// filter, then Phase-2 would blindly fetch those unfiltered agents, producing
		// correct totals (0) but wrong rows (all agents shown).
		// To avoid this inconsistency, fall back to a direct agent-collection sort
		// whenever a text query is present. The reputationScore field is a close proxy
		// for compositeScore and keeps the result set ordered correctly.
		if strings.TrimSpace(f.Query) != "" || r.StatsColl == nil {
			// Text-search path (or no stats collection): sort directly in agent collection.
			var agentSortDoc bson.D
			switch sortOrder {
			case SortScoreAsc:
				agentSortDoc = bson.D{{Key: "reputationScore", Value: 1}}
			case SortTasksDesc:
				agentSortDoc = bson.D{{Key: "totalTasks", Value: -1}}
			default: // SortScoreDesc
				agentSortDoc = bson.D{{Key: "reputationScore", Value: -1}}
			}
			opts := options.Find().SetSort(agentSortDoc).SetSkip(skip).SetLimit(limit)
			docs, err := r.Find(ctx, query, opts)
			if err != nil {
				return nil, 0, fmt.Errorf("agent repo: leaderboard find (text query): %w", err)
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

		// Phase 2: bulk-fetch agents by (chainId, agentId) pairs so multi-chain rows survive.
		pairs := make(bson.A, 0, len(rows))
		posMap := make(map[string]int, len(rows))
		for i, row := range rows {
			pairs = append(pairs, bson.M{"chainId": row.ChainID, "agentId": row.AgentID})
			posMap[orderKey(row.ChainID, row.AgentID)] = i
		}

		agentDocs, err := r.Find(ctx, bson.M{"$or": pairs})
		if err != nil {
			return nil, 0, fmt.Errorf("agent repo: leaderboard agent fetch: %w", err)
		}

		// Restore stats-side order; key by (chainId, agentId) so agents with the same
		// agentId on different chains aren't collapsed.
		ordered := make([]AgentDocument, len(rows))
		for _, d := range agentDocs {
			if pos, ok := posMap[orderKey(d.ChainID, d.AgentID)]; ok {
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

// orderKey produces a stable map key for (chainId, agentId) pairs used during
// score-sort phase-2 ordering.
func orderKey(chainID int64, agentID string) string {
	return strconv.FormatInt(chainID, 10) + ":" + agentID
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
