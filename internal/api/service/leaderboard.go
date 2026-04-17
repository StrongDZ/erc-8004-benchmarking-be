package service

// leaderboard.go — Services for /leaderboard*, /leaderboard/search, /leaderboard/stats,
// /leaderboard/rising-stars.

import (
	"context"
	"fmt"
	"sort"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"
)

// LeaderboardDeps bundles the repos used by the leaderboard service.
type LeaderboardDeps struct {
	Agents   *agentrepo.Repository
	Feedback *feedbackrepo.Repository
	Scores   *scorerepo.Repository
	Crawlers *crawlerrepo.Repository
	Formula  scoring.FormulaConfig
}

// Leaderboard encapsulates business logic for leaderboard endpoints.
type Leaderboard struct {
	deps LeaderboardDeps
}

// NewLeaderboard returns a new Leaderboard service.
func NewLeaderboard(deps LeaderboardDeps) *Leaderboard { return &Leaderboard{deps: deps} }

// ListParams are the parsed inputs for /leaderboard.
type ListParams struct {
	ChainID  int64    // deprecated — prefer ChainIDs
	ChainIDs []int64  // multi-chain filter
	Skills   []string
	Domains  []string
	Services []string
	Tags     []string
	X402     *bool
	HasOASF  *bool
	Active   *bool
	MinScore float64
	MinTasks int64
	Query    string
	Sort     agentrepo.LeaderboardSort
	Page     int
	Limit    int
	Skip     int64
}

// ListResult carries the list + total + applied pagination.
type ListResult struct {
	Rows  []dto.AgentRow
	Total int64
	Page  int
	Limit int
}

// List returns the leaderboard page. Lazy decay is applied per row (§1.4).
// When minScore > 0 rows are filtered AFTER lazy decay.
func (s *Leaderboard) List(ctx context.Context, p ListParams) (*ListResult, error) {
	filter := agentrepo.LeaderboardFilter{
		ChainID:  p.ChainID,
		ChainIDs: p.ChainIDs,
		Skills:   p.Skills,
		Domains:  p.Domains,
		Services: p.Services,
		Tags:     p.Tags,
		X402:     p.X402,
		HasOASF:  p.HasOASF,
		Active:   p.Active,
		MinTasks: p.MinTasks,
		Query:    p.Query,
	}
	docs, total, err := s.deps.Agents.FindLeaderboard(ctx, filter, p.Sort, p.Skip, int64(p.Limit))
	if err != nil {
		return nil, fmt.Errorf("leaderboard list: %w", err)
	}

	now := time.Now().Unix()
	rows := make([]dto.AgentRow, 0, len(docs))
	for _, d := range docs {
		row := toAgentRow(d, s.deps.Formula, now)
		if p.MinScore > 0 && row.TrustScore < p.MinScore {
			continue
		}
		rows = append(rows, row)
	}
	// Re-sort by decayed trustScore when caller asked for a score-based order to make
	// the displayed ranking monotonic even if lazy decay slightly reorders rows.
	if p.Sort == agentrepo.SortScoreDesc || p.Sort == "" {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].TrustScore > rows[j].TrustScore })
	} else if p.Sort == agentrepo.SortScoreAsc {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].TrustScore < rows[j].TrustScore })
	}
	// Assign ranks based on absolute position within the page (page 1 starts at 1).
	for i := range rows {
		rows[i].Rank = int(p.Skip) + i + 1
	}

	return &ListResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// Search implements /leaderboard/search (§2.2). Returns at most `limit` rows.
func (s *Leaderboard) Search(ctx context.Context, chainID int64, q string, limit int) ([]dto.AgentSearchRow, error) {
	docs, err := s.deps.Agents.SearchByNamePrefix(ctx, chainID, q, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("leaderboard search: %w", err)
	}
	now := time.Now().Unix()
	out := make([]dto.AgentSearchRow, 0, len(docs))
	for _, d := range docs {
		trust := scoring.ComputeCurrentScore(d.AccumulatedScore, d.ScoreUpdateAt, d.ConsecutiveFails, now, s.deps.Formula)
		out = append(out, dto.AgentSearchRow{
			ChainID:    d.ChainID,
			AgentID:    d.AgentID,
			Name:       d.Name,
			Image:      d.Image,
			TrustScore: round2(trust),
		})
	}
	return out, nil
}

// Stats implements /leaderboard/stats (§2.4). chainID <= 0 = all chains.
func (s *Leaderboard) Stats(ctx context.Context, chainID int64) (*dto.LeaderboardStats, error) {
	var chainIDs []int64
	if chainID > 0 {
		chainIDs = []int64{chainID}
	}
	return s.StatsMulti(ctx, chainIDs)
}

// StatsMulti aggregates stats across a set of chain IDs. Empty slice = all chains.
func (s *Leaderboard) StatsMulti(ctx context.Context, chainIDs []int64) (*dto.LeaderboardStats, error) {
	raw, err := s.deps.Agents.ComputeStatsMulti(ctx, chainIDs)
	if err != nil {
		return nil, err
	}
	fbTotal, err := s.deps.Feedback.TotalCountMulti(ctx, chainIDs)
	if err != nil {
		return nil, fmt.Errorf("stats feedback total: %w", err)
	}

	// Find max lastProcessedBlock across all crawlers matching the chain scope.
	var crawlers []crawlerrepo.CrawlerState
	if len(chainIDs) == 0 {
		crawlers, err = s.deps.Crawlers.ListAll(ctx)
	} else {
		for _, cid := range chainIDs {
			list, lerr := s.deps.Crawlers.ListByChain(ctx, cid)
			if lerr != nil {
				err = lerr
				break
			}
			crawlers = append(crawlers, list...)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("stats crawlers: %w", err)
	}
	var lastBlock uint64
	var lastUpdated int64
	for _, c := range crawlers {
		if c.LastProcessedBlock > lastBlock {
			lastBlock = c.LastProcessedBlock
		}
		if c.UpdatedAt > lastUpdated {
			lastUpdated = c.UpdatedAt
		}
	}

	var firstChainID int64
	if len(chainIDs) == 1 {
		firstChainID = chainIDs[0]
	}

	return &dto.LeaderboardStats{
		ChainID:          firstChainID,
		ChainIDs:         chainIDs,
		TotalAgents:      raw.TotalAgents,
		ActiveAgents:     raw.ActiveAgents,
		TotalFeedbacks:   fbTotal,
		AvgTrustScore:    round2(raw.AvgAccScore),
		MedianTrustScore: round2(raw.MedianAccScore),
		Top10ScoreAvg:    round2(raw.Top10AccScoreAvg),
		LastBlockIndexed: lastBlock,
		LastIndexedAt:    unixToRFC3339(lastUpdated),
	}, nil
}

// RisingStars implements /leaderboard/rising-stars (§2.3).
// `period` is one of: 24h, 7d, 30d.
func (s *Leaderboard) RisingStars(ctx context.Context, chainID int64, period string, limit int) ([]dto.RisingStarRow, error) {
	dur, err := parsePeriod(period)
	if err != nil {
		return nil, err
	}
	since := time.Now().Add(-dur).Unix()

	stars, err := s.deps.Scores.FindRisingStars(ctx, chainID, since, int64(limit))
	if err != nil {
		return nil, err
	}
	if len(stars) == 0 {
		return []dto.RisingStarRow{}, nil
	}

	// Look up agent names/images for the candidates in one batch.
	ids := make([]string, 0, len(stars))
	for _, s := range stars {
		ids = append(ids, s.AgentID)
	}
	agents, err := s.deps.Agents.FindByIDs(ctx, chainID, ids)
	if err != nil {
		return nil, fmt.Errorf("rising stars: agents lookup: %w", err)
	}
	byID := make(map[string]agentrepo.AgentDocument, len(agents))
	for _, a := range agents {
		byID[a.AgentID] = a
	}

	days := dur.Hours() / 24.0
	out := make([]dto.RisingStarRow, 0, len(stars))
	for _, rs := range stars {
		a := byID[rs.AgentID]
		velocity := 0.0
		if days > 0 {
			velocity = rs.Delta / days
		}
		out = append(out, dto.RisingStarRow{
			ChainID:     chainID,
			AgentID:     rs.AgentID,
			Name:        a.Name,
			Image:       a.Image,
			ScoreNow:    round2(rs.ScoreNow),
			ScoreBefore: round2(rs.ScoreBefore),
			Delta:       round2(rs.Delta),
			Velocity:    round2(velocity),
			Period:      period,
		})
	}
	return out, nil
}

func parsePeriod(p string) (time.Duration, error) {
	switch p {
	case "24h", "1d":
		return 24 * time.Hour, nil
	case "7d":
		return 7 * 24 * time.Hour, nil
	case "30d":
		return 30 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid period %q (want 24h | 1d | 7d | 30d)", p)
	}
}

// RisingStarsMulti aggregates rising stars across a set of chain IDs by running
// the existing per-chain query and merging results client-side. Empty slice = all chains.
func (s *Leaderboard) RisingStarsMulti(ctx context.Context, chainIDs []int64, period string, limit int) ([]dto.RisingStarRow, error) {
	if len(chainIDs) == 0 {
		return nil, fmt.Errorf("rising stars: chainIds must not be empty")
	}
	if len(chainIDs) == 1 {
		return s.RisingStars(ctx, chainIDs[0], period, limit)
	}
	all := make([]dto.RisingStarRow, 0, len(chainIDs)*limit)
	for _, cid := range chainIDs {
		rows, err := s.RisingStars(ctx, cid, period, limit)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Delta > all[j].Delta })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// TopTags returns the top tags across the given chain scope. Empty chainIDs = all chains.
func (s *Leaderboard) TopTags(ctx context.Context, chainIDs []int64, query string, limit int) ([]dto.TagCount, error) {
	rows, err := s.deps.Agents.TopTags(ctx, chainIDs, query, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("leaderboard top tags: %w", err)
	}
	out := make([]dto.TagCount, 0, len(rows))
	for _, r := range rows {
		out = append(out, dto.TagCount{Tag: r.Tag, Count: r.Count})
	}
	return out, nil
}
