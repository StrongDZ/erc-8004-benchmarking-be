package service

// agent.go — Services for /agents/:chainId/:agentId endpoints.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// ErrAgentNotFound is returned when an agent document is not in Mongo.
var ErrAgentNotFound = errors.New("agent not found")

// AgentDeps bundles repositories used by the Agent service.
type AgentDeps struct {
	Agents    *agentrepo.Repository
	Feedback  *feedbackrepo.Repository
	Scores    *scorerepo.Repository
	Identity  *identityrepo.Repository
	Events    *eventrepo.Repository
	Contracts *configrepo.ContractsRepository
	Formula   scoring.FormulaConfig
}

// Agent encapsulates business logic for /agents/* endpoints.
type Agent struct {
	deps AgentDeps
}

// NewAgent constructs an Agent service.
func NewAgent(deps AgentDeps) *Agent { return &Agent{deps: deps} }

// ── Profile ─────────────────────────────────────────────────────────────────

// Profile returns the full agent profile (§3.1) with lazy decay + class distribution.
func (s *Agent) Profile(ctx context.Context, chainID int64, agentID string) (*dto.AgentProfile, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, fmt.Errorf("agent profile find: %w", err)
	}
	dist, err := s.deps.Feedback.ClassDistribution(ctx, chainID, agentID)
	if err != nil {
		return nil, fmt.Errorf("agent profile class dist: %w", err)
	}
	p := toAgentProfile(doc, dist, s.deps.Formula, time.Now().Unix())
	return &p, nil
}

// ── Score history ───────────────────────────────────────────────────────────

// ScoreHistoryParams are the parsed inputs for /agents/:id/score-history.
type ScoreHistoryParams struct {
	ChainID    int64
	AgentID    string
	From       *time.Time
	To         *time.Time
	Resolution string // "raw" | "1h" | "6h" | "1d" (default 1d)
	Limit      int64  // cap on number of returned points (max 1000)
}

// ScoreHistoryResult wraps the downsampled series with the original total count.
type ScoreHistoryResult struct {
	Points []dto.ScorePoint
	Total  int64
}

// ScoreHistory returns the points series for a line chart (§3.2).
func (s *Agent) ScoreHistory(ctx context.Context, p ScoreHistoryParams) (*ScoreHistoryResult, error) {
	var fromUnix, toUnix int64
	if p.From != nil {
		fromUnix = p.From.Unix()
	}
	if p.To != nil {
		toUnix = p.To.Unix()
	}
	// Cap per spec (max 1000), no cap if <= 0 means "use limit=0 at DB then trim here".
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	if limit > 1000 {
		limit = 1000
	}

	page, err := s.deps.Scores.FindSnapshotsInRange(ctx, p.ChainID, p.AgentID, fromUnix, toUnix, 0)
	if err != nil {
		return nil, err
	}
	total := page.Total

	series := downsample(page.Snapshots, p.Resolution)
	if int64(len(series)) > limit {
		series = series[len(series)-int(limit):]
	}

	out := make([]dto.ScorePoint, 0, len(series))
	for _, sn := range series {
		out = append(out, toScorePoint(sn))
	}
	return &ScoreHistoryResult{Points: out, Total: total}, nil
}

// downsample picks the last sample per bucket. `resolution` determines bucket size.
// "raw" returns the input as-is.
func downsample(in []scorerepo.ScoreSnapshotItem, resolution string) []scorerepo.ScoreSnapshotItem {
	bucket := resolutionToSeconds(resolution)
	if bucket <= 0 || len(in) == 0 {
		return in
	}
	// Ensure input is sorted by timestamp ascending.
	sort.SliceStable(in, func(i, j int) bool { return in[i].Timestamp < in[j].Timestamp })

	var out []scorerepo.ScoreSnapshotItem
	var current scorerepo.ScoreSnapshotItem
	var currentBucket int64 = -1
	for _, s := range in {
		b := s.Timestamp / bucket
		if b != currentBucket && currentBucket >= 0 {
			out = append(out, current)
		}
		current = s
		currentBucket = b
	}
	if currentBucket >= 0 {
		out = append(out, current)
	}
	return out
}

func resolutionToSeconds(r string) int64 {
	switch r {
	case "1h":
		return 3600
	case "6h":
		return 6 * 3600
	case "1d":
		return 86400
	case "raw":
		return 0
	default:
		return 86400
	}
}

// ── Feedbacks ───────────────────────────────────────────────────────────────

// FeedbacksParams are the parsed inputs for /agents/:id/feedbacks (§3.3).
type FeedbacksParams struct {
	ChainID  int64
	AgentID  string
	Category string
	Status   string
	From     *time.Time
	To       *time.Time
	SortDesc bool
	Page     int
	Limit    int
	Skip     int64
}

// FeedbacksResult carries the list + pagination.
type FeedbacksResult struct {
	Rows  []dto.FeedbackRow
	Total int64
	Page  int
	Limit int
}

// Feedbacks returns a filtered, paginated feedback list.
func (s *Agent) Feedbacks(ctx context.Context, p FeedbacksParams) (*FeedbacksResult, error) {
	f := feedbackrepo.ListFilter{
		ChainID:  p.ChainID,
		AgentID:  p.AgentID,
		Category: p.Category,
		Status:   p.Status,
		From:     p.From,
		To:       p.To,
		SortDesc: p.SortDesc,
	}
	docs, total, err := s.deps.Feedback.ListFiltered(ctx, f, p.Skip, int64(p.Limit))
	if err != nil {
		return nil, err
	}
	rows := make([]dto.FeedbackRow, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, toFeedbackRow(d))
	}
	return &FeedbacksResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// FeedbackDetail returns one feedback + offchain content (§3.4).
// `feedbackID` is "{clientAddress}:{feedbackIndex}".
func (s *Agent) FeedbackDetail(ctx context.Context, chainID int64, agentID, feedbackID string) (*dto.FeedbackDetail, error) {
	client, idxStr, ok := strings.Cut(feedbackID, ":")
	if !ok {
		return nil, errors.New("invalid feedbackId; want '{clientAddress}:{feedbackIndex}'")
	}
	idx, err := strconv.ParseUint(strings.TrimSpace(idxStr), 10, 64)
	if err != nil {
		return nil, errors.New("invalid feedbackIndex in feedbackId")
	}

	rec, err := s.deps.Feedback.FindByAgentAndIndex(ctx, chainID, agentID, strings.TrimSpace(client), idx)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, err
	}

	detail := &dto.FeedbackDetail{FeedbackRow: toFeedbackRow(*rec)}
	detail.OffchainParsed = rec.FeedbackParsed
	return detail, nil
}

// ── Identity history ────────────────────────────────────────────────────────

// IdentityHistory returns on-chain identity events (§3.5).
func (s *Agent) IdentityHistory(ctx context.Context, chainID int64, agentID string) ([]dto.IdentityEvent, error) {
	docs, err := s.deps.Identity.ListByAgent(ctx, chainID, agentID)
	if err != nil {
		return nil, fmt.Errorf("identity history: %w", err)
	}
	out := make([]dto.IdentityEvent, 0, len(docs))
	for _, d := range docs {
		out = append(out, toIdentityEvent(d))
	}
	return out, nil
}

// ── Activity heatmap ────────────────────────────────────────────────────────

// ActivityHeatmap returns one bucket per UTC day (§3.7).
func (s *Agent) ActivityHeatmap(ctx context.Context, chainID int64, agentID string, days int) ([]feedbackrepo.HeatmapDay, error) {
	return s.deps.Feedback.ActivityHeatmap(ctx, chainID, agentID, days)
}

// ── Penalties ───────────────────────────────────────────────────────────────

// PenaltiesParams captures inputs for /agents/:id/penalties (§3.8).
type PenaltiesParams struct {
	ChainID int64
	AgentID string
	Type    string // "fail" | "revoked" | "both"
	Page    int
	Limit   int
	Skip    int64
}

// PenaltiesResult is the paginated penalty list.
type PenaltiesResult struct {
	Rows  []dto.PenaltyRow
	Total int64
	Page  int
	Limit int
}

// Penalties returns "red flag" feedbacks for the agent (§3.8).
func (s *Agent) Penalties(ctx context.Context, p PenaltiesParams) (*PenaltiesResult, error) {
	docs, total, err := s.deps.Feedback.ListPenalties(ctx, p.ChainID, p.AgentID, p.Type, p.Skip, int64(p.Limit))
	if err != nil {
		return nil, err
	}
	out := make([]dto.PenaltyRow, 0, len(docs))
	for _, d := range docs {
		reason := "fail"
		if strings.TrimSpace(d.RevokeTxHash) != "" {
			reason = "revoked"
		}
		out = append(out, dto.PenaltyRow{
			FeedbackIndex: d.FeedbackIndex,
			ClientAddress: d.ClientAddress,
			Timestamp:     unixToRFC3339(d.Timestamp),
			Vi:            d.Vi,
			Wi:            round2(d.Wi),
			Reason:        reason,
			TxHash:        d.TxHash,
			RevokeTxHash:  d.RevokeTxHash,
		})
	}
	return &PenaltiesResult{Rows: out, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// ── Related agents ──────────────────────────────────────────────────────────

// Related returns other agents sharing OASF skills/domains (§3.11).
// `by` ∈ {"skill", "domain", "both"}; default "both".
func (s *Agent) Related(ctx context.Context, chainID int64, agentID, by string, limit int64) ([]dto.AgentRow, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, err
	}

	var skills, domains []string
	switch strings.ToLower(strings.TrimSpace(by)) {
	case "skill":
		skills = doc.OASFSkills
	case "domain":
		domains = doc.OASFDomains
	default:
		skills = doc.OASFSkills
		domains = doc.OASFDomains
	}

	docs, err := s.deps.Agents.FindRelated(ctx, chainID, agentID, skills, domains, limit)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	out := make([]dto.AgentRow, 0, len(docs))
	for i, d := range docs {
		r := toAgentRow(d, s.deps.Formula, now)
		r.Rank = i + 1
		out = append(out, r)
	}
	return out, nil
}

// ── Proof ───────────────────────────────────────────────────────────────────

// Proof returns event metadata for verification (§3.10).
func (s *Agent) Proof(ctx context.Context, chainID int64, agentID, txHash string) (*dto.ProofResponse, error) {
	evs, err := s.deps.Events.FindByTxHash(ctx, txHash)
	if err != nil {
		return nil, err
	}
	// Select the first matching event on the requested chain + agent (agentId appears in args).
	var hit *eventrepo.DecodedEvent
	for i := range evs {
		if evs[i].ChainID != chainID {
			continue
		}
		if matchesAgentArg(evs[i].Args, agentID) {
			hit = &evs[i]
			break
		}
	}
	if hit == nil {
		return nil, ErrAgentNotFound
	}

	resp := &dto.ProofResponse{
		ChainID:      hit.ChainID,
		TxHash:       hit.TxHash,
		BlockNumber:  hit.BlockNumber,
		EventName:    hit.EventName,
		ContractType: hit.ContractType,
		Args:         hit.Args,
	}
	// Pull URIs from decoded args if the event carries them (best-effort).
	if v, ok := hit.Args["feedbackURI"]; ok {
		if str, ok := v.(string); ok {
			resp.FeedbackURI = str
		}
	}
	if v, ok := hit.Args["responseURI"]; ok {
		if str, ok := v.(string); ok {
			resp.ResponseURIs = []string{str}
		}
	}
	// Compose block-explorer URL from contracts config.
	if cfg, err := s.deps.Contracts.FindOne(ctx, map[string]any{"_id": configrepo.ContractsDocumentID(chainID)}); err == nil {
		_ = cfg
	}
	resp.BlockExplorerURL = explorerTxURL(chainID, hit.TxHash)
	return resp, nil
}

func matchesAgentArg(args map[string]any, agentID string) bool {
	if agentID == "" {
		return true
	}
	candidates := []string{"agentId", "tokenId", "validator", "agent"}
	for _, k := range candidates {
		if v, ok := args[k]; ok {
			switch x := v.(type) {
			case string:
				if strings.EqualFold(x, agentID) {
					return true
				}
			case float64:
				if strconv.FormatInt(int64(x), 10) == agentID {
					return true
				}
			case int64:
				if strconv.FormatInt(x, 10) == agentID {
					return true
				}
			case uint64:
				if strconv.FormatUint(x, 10) == agentID {
					return true
				}
			}
		}
	}
	return false
}

// explorerTxURL returns a best-effort block explorer URL for known chains.
func explorerTxURL(chainID int64, txHash string) string {
	base := map[int64]string{
		1:        "https://etherscan.io",
		8453:     "https://basescan.org",
		42161:    "https://arbiscan.io",
		10:       "https://optimistic.etherscan.io",
		137:      "https://polygonscan.com",
		11155111: "https://sepolia.etherscan.io",
	}[chainID]
	if base == "" {
		return ""
	}
	return fmt.Sprintf("%s/tx/%s", base, txHash)
}

// ── Radar ───────────────────────────────────────────────────────────────────

// Radar computes the 6-axis radar payload (§3.6).
// All values are clamped to [0, 1].
func (s *Agent) Radar(ctx context.Context, chainID int64, agentID string) (map[string]float64, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, err
	}

	successRate := 0.0
	if doc.TotalTasks > 0 {
		successRate = float64(doc.TotalPassed) / float64(doc.TotalTasks)
	}

	taskVolume := clamp01(math.Log1p(float64(doc.TotalTasks)) / math.Log1p(200.0))

	avgDifficulty := 0.0
	if len(doc.OASFSkills)+len(doc.OASFDomains) > 0 {
		avgDifficulty = 0.5
	}

	// scoreVelocity: compare S_now vs S_{now-7d}, normalized to [0, 1].
	velocity := 0.5
	since := time.Now().Add(-7 * 24 * time.Hour).Unix()
	if stars, err := s.deps.Scores.FindSnapshotsInRange(ctx, chainID, agentID, since, 0, 0); err == nil && len(stars.Snapshots) >= 2 {
		first := stars.Snapshots[0].AgentScore
		last := stars.Snapshots[len(stars.Snapshots)-1].AgentScore
		delta := last - first
		velocity = clamp01((delta + 500) / 1000) // map ±500 → [0, 1]
	}

	domainDepth := clamp01(float64(len(doc.OASFDomains)) / 5.0)

	// consistency: 1 - stddev(recent 30 event_scores) / max_reasonable_stddev(200).
	consistency := 0.5
	if hist, err := s.deps.Scores.ListByAgent(ctx, chainID, agentID, 30); err == nil && hist != nil && len(hist.ScoreSnapshots) > 1 {
		var sum, sumSq float64
		n := 0.0
		for _, sn := range hist.ScoreSnapshots {
			if sn.Type != "event" {
				continue
			}
			sum += sn.EventScore
			sumSq += sn.EventScore * sn.EventScore
			n++
		}
		if n > 1 {
			mean := sum / n
			variance := (sumSq / n) - mean*mean
			if variance < 0 {
				variance = 0
			}
			stddev := math.Sqrt(variance)
			consistency = clamp01(1 - stddev/200.0)
		}
	}

	return map[string]float64{
		"successRate":   round4(successRate),
		"taskVolume":    round4(taskVolume),
		"avgDifficulty": round4(avgDifficulty),
		"scoreVelocity": round4(velocity),
		"domainDepth":   round4(domainDepth),
		"consistency":   round4(consistency),
	}, nil
}

func clamp01(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
