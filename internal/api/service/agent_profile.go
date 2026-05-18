package service

// agent_profile.go — Methods for agent identity, overview, radar, proof, and related-agents.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

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
// Returns one of: "ok" | "warning" | "fail" | "unknown".
//   - "warning": endpoint fetched successfully but is not JSON, and the service is
//     JSON-required (oasf, a2a, mcp). Other services that fetched OK are treated as
//     healthy regardless of body type.
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
	statsMap := bulkFetchStats(ctx, s.deps.ScoreStats, chainID, docs)
	out := make([]dto.AgentRow, 0, len(docs))
	for i, d := range docs {
		r := toAgentRow(d, statsMap[d.AgentID], s.deps.Formula, now)
		r.Rank = i + 1
		out = append(out, r)
	}
	return out, nil
}

// bulkFetchStats fetches stats per agent. Returns map[agentID]*stats with nils for missing.
func bulkFetchStats(ctx context.Context, repo agentScoreStatsRepo, chainID int64, docs []agentrepo.AgentDocument) map[string]*scorestats.AgentScoreStats {
	out := make(map[string]*scorestats.AgentScoreStats, len(docs))
	for _, d := range docs {
		if s, _ := repo.FindByAgentID(ctx, chainID, d.AgentID); s != nil {
			out[d.AgentID] = s
		}
	}
	return out
}

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

	// Source per-agent stats once; task counts now live on agent_score_stats.
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

	// scoreVelocity and consistency from pre-computed agent_score_stats (updated every ~30 min).
	velocity := 0.5
	consistency := 0.5
	if stats != nil {
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

// ReputationScoreHistory re-derives the reputation timeline from feedback_history.
// Output: one "event" point per scored feedback, a "decay" point at every 00:00 UTC
// midnight between events, and a final "decay" point at "now". The math mirrors
// the trustrank write path: ApplyTaskScore for the event update, ComputePenalty
// when vi < 0.40, and α-based decay (same lambda as the lazy read path) for the
// between-event samples.
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

// midnightsBetween returns every 00:00 UTC strictly after `from` and up to (and including)
// any midnight <= `to`. When from <= 0 (no prior event), returns nil because there's no
// rep to decay yet.
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

// decayForward returns rep decayed from lastTs forward to atTs using the α-based lambda
// (same as the lazy read path; never write back).
func decayForward(rep float64, lastTs, atTs int64, cfg scoring.FormulaConfig) float64 {
	if lastTs <= 0 || atTs <= lastTs {
		return rep
	}
	lambda := scoring.ComputeDecayRate(cfg.Alpha, cfg.TBaseDays)
	deltaDays := float64(atTs-lastTs) / 86400.0
	return rep * scoring.ComputeDecayFactor(lambda, deltaDays)
}
