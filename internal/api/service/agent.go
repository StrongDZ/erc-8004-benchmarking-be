package service

// agent.go — /agents/* service: types, deps, profile, overview, reputation, feedback.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// Sentinel errors returned by service methods — handlers map these to HTTP codes.
var (
	ErrAgentNotFound    = errors.New("agent not found")
	ErrFeedbackNotFound = errors.New("feedback not found")
	ErrInvalidInput     = errors.New("invalid input")
)

// ── Interfaces (defined at consumer site per DIP) ────────────────────────────

type agentAgentRepo interface {
	FindByAgentID(ctx context.Context, chainID int64, agentID string) (*agentrepo.AgentDocument, error)
	FindByIDs(ctx context.Context, chainID int64, agentIDs []string) ([]agentrepo.AgentDocument, error)
	FindRelated(ctx context.Context, chainID int64, excludeAgentID string, skills, domains []string, limit int64) ([]agentrepo.AgentDocument, error)
}

type agentFeedbackRepo interface {
	ClassDistribution(ctx context.Context, chainID int64, agentID string) (map[string]int64, error)
	ListFiltered(ctx context.Context, f feedbackrepo.ListFilter, skip, limit int64) ([]feedbackrepo.FeedbackRecord, int64, error)
	ListPenalties(ctx context.Context, chainID int64, agentID, mode string, skip, limit int64) ([]feedbackrepo.FeedbackRecord, int64, error)
	FindByAgentAndIndex(ctx context.Context, chainID int64, agentID, clientAddress string, feedbackIndex uint64) (*feedbackrepo.FeedbackRecord, error)
	ActivityHeatmap(ctx context.Context, chainID int64, agentID string, days int) ([]feedbackrepo.HeatmapDay, error)
	ListForReputationHistory(ctx context.Context, chainID int64, agentID string) ([]feedbackrepo.FeedbackRecord, error)
}

type agentScoreStatsRepo interface {
	FindByAgentID(ctx context.Context, chainID int64, agentID string) (*scorestats.AgentScoreStats, error)
}

type agentIdentityRepo interface {
	ListByAgent(ctx context.Context, chainID int64, agentID string) ([]identityrepo.IdentityChange, error)
}

type agentEventRepo interface {
	FindByTxHash(ctx context.Context, txHash string) ([]eventrepo.DecodedEvent, error)
}

type agentOffchainRepo interface {
	HasSuccessfulFetch(ctx context.Context, uri string) (bool, error)
	GetContent(ctx context.Context, uri string) (string, bool, error)
	FindByURIs(ctx context.Context, uris []string) ([]offchainrepo.OffchainData, error)
}

type agentContractRepo interface {
	FindActive(ctx context.Context) ([]contractsrepo.ContractsConfig, error)
}

// AgentDeps bundles repositories used by the Agent service.
type AgentDeps struct {
	Agents     agentAgentRepo
	Feedback   agentFeedbackRepo
	ScoreStats agentScoreStatsRepo
	Identity   agentIdentityRepo
	Events     agentEventRepo
	Offchain   agentOffchainRepo
	Contracts  agentContractRepo
	Formula    scoring.FormulaConfig
}

// Agent encapsulates business logic for /agents/* endpoints.
type Agent struct {
	deps AgentDeps
}

// NewAgent constructs an Agent service.
func NewAgent(deps AgentDeps) *Agent { return &Agent{deps: deps} }

// ── Profile & identity ───────────────────────────────────────────────────────

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
	stats, _ := s.deps.ScoreStats.FindByAgentID(ctx, chainID, agentID)
	p := toAgentProfile(doc, stats, dist, s.deps.Formula, time.Now().Unix())
	return &p, nil
}

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
	statsMap := bulkFetchStats(ctx, s.deps.ScoreStats, docs)
	out := make([]dto.AgentRow, 0, len(docs))
	for i, d := range docs {
		r := toAgentRow(d, statsMap[statsKey(d.ChainID, d.AgentID)], s.deps.Formula, now)
		r.Rank = i + 1
		out = append(out, r)
	}
	return out, nil
}

// Proof returns event metadata for verification (§3.10).
func (s *Agent) Proof(ctx context.Context, chainID int64, agentID, txHash string) (*dto.ProofResponse, error) {
	evs, err := s.deps.Events.FindByTxHash(ctx, txHash)
	if err != nil {
		return nil, err
	}
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

// Radar computes the 6-axis radar payload (§3.6).
func (s *Agent) Radar(ctx context.Context, chainID int64, agentID string) (map[string]float64, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, err
	}

	stats, _ := s.deps.ScoreStats.FindByAgentID(ctx, chainID, agentID)
	var totalTasks, totalPassed int64
	if stats != nil {
		totalTasks = stats.TotalTasks
		totalPassed = stats.TotalPassed
	}
	successRate := 0.0
	if totalTasks > 0 {
		successRate = float64(totalPassed) / float64(totalTasks)
	}

	taskVolume := clamp01(math.Log1p(float64(totalTasks)) / math.Log1p(200.0))

	avgDifficulty := 0.0
	if len(doc.OASFSkills)+len(doc.OASFDomains) > 0 {
		avgDifficulty = 0.5
	}

	domainDepth := clamp01(float64(len(doc.OASFDomains)) / 5.0)

	velocity := 0.5
	consistency := 0.5
	if stats != nil {
		velocity = clamp01((stats.Delta7d/7 + 500) / 1000)
		consistency = stats.Consistency
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

// ── Overview ─────────────────────────────────────────────────────────────────

// Overview returns the payload for the "Overview" tab.
func (s *Agent) Overview(ctx context.Context, chainID int64, agentID string) (*dto.AgentOverview, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, fmt.Errorf("agent overview find: %w", err)
	}

	createdTx := ""
	if hist, herr := s.deps.Identity.ListByAgent(ctx, chainID, agentID); herr == nil {
		for _, h := range hist {
			if strings.EqualFold(h.EventName, "Registered") {
				createdTx = h.TxHash
				break
			}
		}
		if createdTx == "" && len(hist) > 0 {
			createdTx = hist[0].TxHash
		}
	}

	agentWallet := extractAgentWallet(doc.OnchainMetadata)

	svcs := make([]dto.ServiceOverview, 0, len(doc.Services))
	for _, sv := range doc.Services {
		health, info := probeEndpointHealth(ctx, s.deps.Offchain, sv.Name, sv.Endpoint)
		svcs = append(svcs, dto.ServiceOverview{
			Name:       sv.Name,
			Endpoint:   sv.Endpoint,
			Version:    sv.Version,
			Skills:     sv.Skills,
			Domains:    sv.Domains,
			Health:     health,
			HealthInfo: info,
		})
	}

	onchain := make(map[string]dto.OnchainMetadataValue, len(doc.OnchainMetadata))
	for k, v := range doc.OnchainMetadata {
		onchain[k] = dto.OnchainMetadataValue{
			RawHex:       v.RawHex,
			Decoded:      v.Decoded,
			DetectedType: v.DetectedType,
			Confidence:   v.Confidence,
		}
	}

	return &dto.AgentOverview{
		ChainID:          doc.ChainID,
		AgentID:          doc.AgentID,
		Owner:            doc.Owner,
		AgentURI:         doc.AgentURI,
		Name:             doc.Name,
		Description:      doc.Description,
		Image:            doc.Image,
		Active:           doc.Active,
		CreatedAt:        unixToRFC3339(doc.CreatedAt),
		CreatedTx:        createdTx,
		AgentWallet:      agentWallet,
		Services:         svcs,
		OnchainMetadata:  onchain,
		OffchainMetadata: doc.OffchainMetadata,
	}, nil
}

func onchainDecodedToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func extractAgentWallet(meta map[string]agentrepo.OnchainMetadataValue) string {
	if len(meta) == 0 {
		return ""
	}
	candidates := []string{"agentWallet", "agent_wallet", "wallet", "walletAddress", "wallet_address"}
	for _, k := range candidates {
		for mk, mv := range meta {
			if strings.EqualFold(mk, k) {
				if s := onchainDecodedToString(mv.Decoded); s != "" {
					return s
				}
				if mv.RawHex != "" {
					return mv.RawHex
				}
			}
		}
	}
	return ""
}

func probeEndpointHealth(ctx context.Context, repo agentOffchainRepo, name, endpoint string) (string, string) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || repo == nil {
		return "unknown", ""
	}
	rows, rerr := repo.FindByURIs(ctx, []string{endpoint})
	if rerr != nil || len(rows) == 0 {
		return "unknown", ""
	}
	row := rows[0]
	switch row.Status {
	case offchainrepo.StatusFetchedJSON:
		return "ok", ""
	case offchainrepo.StatusFetchedNotJSON:
		if scoring.IsJSONRequired(name) {
			return "warning", "fetched but not valid JSON; expected JSON for this endpoint type"
		}
		return "ok", ""
	case offchainrepo.StatusFetchFailed:
		if strings.TrimSpace(row.FetchError) != "" {
			return "fail", row.FetchError
		}
		return "fail", ""
	}
	return "unknown", ""
}

// ── Reputation history ───────────────────────────────────────────────────────

// ReputationScoreHistory re-derives the reputation timeline from feedback_history.
func (s *Agent) ReputationScoreHistory(ctx context.Context, chainID int64, agentID string) (*dto.ReputationScoreHistoryResult, error) {
	feedbacks, err := s.deps.Feedback.ListForReputationHistory(ctx, chainID, agentID)
	if err != nil {
		return nil, fmt.Errorf("reputation history list: %w", err)
	}

	cfg := s.deps.Formula
	points := make([]dto.ReputationScorePoint, 0, len(feedbacks)*2+1)
	var rep float64
	var consecFails int64
	var lastTs int64

	for _, f := range feedbacks {
		for _, midnight := range midnightsBetween(lastTs, f.Timestamp) {
			points = append(points, dto.ReputationScorePoint{
				Timestamp: unixToRFC3339(midnight),
				Score:     round2(decayForward(rep, lastTs, midnight, cfg)),
				Type:      "decay",
			})
		}

		vi := computeVi(f)
		wi := scoring.ComputeWi(f.PriceUSDC, cfg.Alpha, cfg.Beta, cfg.K)
		rep = scoring.ApplyTaskScore(rep, lastTs, wi, vi, f.Timestamp, cfg)
		if vi < 0.40 {
			consecFails++
			rep -= scoring.ComputePenalty(consecFails, cfg.Gamma, cfg.Theta)
		} else {
			consecFails = 0
		}
		lastTs = f.Timestamp

		points = append(points, dto.ReputationScorePoint{
			Timestamp: unixToRFC3339(f.Timestamp),
			Score:     round2(rep),
			Type:      "event",
			TxHash:    f.TxHash,
		})
	}

	if lastTs > 0 {
		now := time.Now().Unix()
		for _, midnight := range midnightsBetween(lastTs, now) {
			points = append(points, dto.ReputationScorePoint{
				Timestamp: unixToRFC3339(midnight),
				Score:     round2(decayForward(rep, lastTs, midnight, cfg)),
				Type:      "decay",
			})
		}
		points = append(points, dto.ReputationScorePoint{
			Timestamp: unixToRFC3339(now),
			Score:     round2(scoring.ComputeCurrentScore(rep, lastTs, now, cfg)),
			Type:      "decay",
		})
	}

	return &dto.ReputationScoreHistoryResult{Points: points}, nil
}

func midnightsBetween(from, to int64) []int64 {
	if from <= 0 || to <= from {
		return nil
	}
	fromT := time.Unix(from, 0).UTC()
	next := time.Date(fromT.Year(), fromT.Month(), fromT.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	out := []int64{}
	for ts := next.Unix(); ts <= to; ts += 86400 {
		out = append(out, ts)
	}
	return out
}

func decayForward(rep float64, lastTs, atTs int64, cfg scoring.FormulaConfig) float64 {
	if lastTs <= 0 || atTs <= lastTs {
		return rep
	}
	lambda := scoring.ComputeDecayRate(cfg.Alpha, cfg.TBaseDays)
	deltaDays := float64(atTs-lastTs) / 86400.0
	return rep * scoring.ComputeDecayFactor(lambda, deltaDays)
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

const (
	offchainURIMaxLen       = 8192
	offchainRawPreviewRunes = 12000
)

func truncateRunesPreview(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// OffchainDataByURI returns one offchain_data row for the given logical URI.
func (s *Agent) OffchainDataByURI(ctx context.Context, uri string) (*dto.OffchainURIDataView, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil, fmt.Errorf("%w: uri is required", ErrInvalidInput)
	}
	if len(uri) > offchainURIMaxLen {
		return nil, fmt.Errorf("%w: uri exceeds max length", ErrInvalidInput)
	}
	docs, err := s.deps.Offchain.FindByURIs(ctx, []string{uri})
	if err != nil {
		return nil, err
	}
	out := &dto.OffchainURIDataView{Found: len(docs) > 0}
	if len(docs) == 0 {
		return out, nil
	}
	d := docs[0]
	out.Status = d.Status
	out.SourceType = d.SourceType
	out.EventType = d.EventType
	out.ContractType = d.ContractType
	out.FetchError = d.FetchError
	out.ContentSize = d.ContentSize
	switch d.Status {
	case offchainrepo.StatusFetchedJSON:
		body := strings.TrimSpace(d.Content)
		if body == "" {
			break
		}
		var parsed any
		if err := json.Unmarshal([]byte(body), &parsed); err == nil {
			out.Parsed = parsed
		}
	case offchainrepo.StatusFetchedNotJSON:
		if d.Content != "" {
			out.RawPreview = truncateRunesPreview(d.Content, offchainRawPreviewRunes)
		}
	}
	return out, nil
}

// ActivityHeatmap returns one bucket per UTC day (§3.7).
func (s *Agent) ActivityHeatmap(ctx context.Context, chainID int64, agentID string, days int) ([]feedbackrepo.HeatmapDay, error) {
	return s.deps.Feedback.ActivityHeatmap(ctx, chainID, agentID, days)
}

// PenaltiesParams captures inputs for /agents/:id/penalties (§3.8).
type PenaltiesParams struct {
	ChainID int64
	AgentID string
	Type    string
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
			Vi:            computeVi(d),
			Wi:            round2(d.Wi),
			Reason:        reason,
			TxHash:        d.TxHash,
			RevokeTxHash:  d.RevokeTxHash,
		})
	}
	return &PenaltiesResult{Rows: out, Total: total, Page: p.Page, Limit: p.Limit}, nil
}
