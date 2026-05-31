package agent

// queries_leaderboard.go — Leaderboard search, filtering, and related-agents queries.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// leaderboardSortDoc maps a LeaderboardSort to a Mongo sort document.
// compositeScore and totalTasks are denormalized on AgentDocument so no join is needed.
func leaderboardSortDoc(s LeaderboardSort) bson.D {
	switch s {
	case SortScoreAsc:
		return bson.D{{Key: "compositeScore", Value: 1}}
	case SortTasksDesc:
		return bson.D{{Key: "totalTasks", Value: -1}}
	case SortRecent:
		return bson.D{{Key: "createdAt", Value: -1}}
	default: // SortScoreDesc
		return bson.D{{Key: "compositeScore", Value: -1}}
	}
}

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
// compositeScore and totalTasks are denormalized on AgentDocument so all sorting is
// done directly on the agents collection — no join to agent_score_stats needed.
// `limit` is capped by the caller; this function does not clamp.
func (r *Repository) FindLeaderboard(ctx context.Context, f LeaderboardFilter, sortOrder LeaderboardSort, skip, limit int64) ([]AgentDocument, int64, error) {
	query := buildLeaderboardQuery(f)

	total, err := r.Count(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("agent repo: leaderboard count: %w", err)
	}

	opts := options.Find().SetSort(leaderboardSortDoc(sortOrder)).SetSkip(skip).SetLimit(limit)
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
