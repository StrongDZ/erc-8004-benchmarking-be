package service

// agent.go — Services for /agents/:chainId/:agentId endpoints.

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

// ── Overview ────────────────────────────────────────────────────────────────

// Overview returns the payload for the "Overview" tab: basic identity fields,
// on/off-chain metadata, and each service's health (derived from offchain_data).
func (s *Agent) Overview(ctx context.Context, chainID int64, agentID string) (*dto.AgentOverview, error) {
	doc, err := s.deps.Agents.FindByAgentID(ctx, chainID, agentID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return nil, ErrAgentNotFound
		}
		return nil, fmt.Errorf("agent overview find: %w", err)
	}

	// Find the "Registered" tx (first identity event for this agent).
	createdTx := ""
	if hist, herr := s.deps.Identity.ListByAgent(ctx, chainID, agentID); herr == nil {
		for _, h := range hist {
			if strings.EqualFold(h.EventName, "Registered") {
				createdTx = h.TxHash
				break
			}
		}
		// Fallback: oldest event regardless of name.
		if createdTx == "" && len(hist) > 0 {
			createdTx = hist[0].TxHash
		}
	}

	// agentWallet is conventionally stored as an on-chain metadata entry.
	agentWallet := extractAgentWallet(doc.OnchainMetadata)

	// Probe service health via the offchain_data cache.
	svcs := make([]dto.ServiceOverview, 0, len(doc.Services))
	for _, sv := range doc.Services {
		health, info := probeEndpointHealth(ctx, s.deps.Offchain, sv.Endpoint)
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

// onchainDecodedToString stringifies on-chain decoded metadata for display/lookup (addresses, scalars).
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

// extractAgentWallet pulls the best candidate for the agent's wallet address
// out of on-chain metadata, trying a few common key variants.
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

// probeEndpointHealth derives a service's health from the offchain_data cache.
// Returns one of: "ok" | "fail" | "unknown".
func probeEndpointHealth(ctx context.Context, repo agentOffchainRepo, endpoint string) (string, string) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || repo == nil {
		return "unknown", ""
	}
	ok, err := repo.HasSuccessfulFetch(ctx, endpoint)
	if err != nil {
		return "unknown", err.Error()
	}
	if ok {
		return "ok", ""
	}
	// Not a success — inspect the stored error (if any) to decide fail vs unknown.
	if _, found, gerr := repo.GetContent(ctx, endpoint); gerr == nil && !found {
		// Fetch the raw row to see if a FetchError was recorded.
		rows, rerr := repo.FindByURIs(ctx, []string{endpoint})
		if rerr == nil && len(rows) > 0 && strings.TrimSpace(rows[0].FetchError) != "" {
			return "fail", rows[0].FetchError
		}
	}
	return "unknown", ""
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

// OffchainDataByURI returns one offchain_data row for the given logical URI (fetch status + parsed JSON when available).
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
			Vi:            computeVi(d),
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

	domainDepth := clamp01(float64(len(doc.OASFDomains)) / 5.0)

	// scoreVelocity and consistency from pre-computed agent_score_stats (updated every ~30 min).
	velocity := 0.5
	consistency := 0.5
	if stats, err := s.deps.ScoreStats.FindByAgentID(ctx, chainID, agentID); err == nil && stats != nil {
		velocity = clamp01((stats.Delta7d/7 + 500) / 1000) // map ±500 pts/day → [0, 1]
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
