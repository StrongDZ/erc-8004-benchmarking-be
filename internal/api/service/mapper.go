package service

// mapper.go — Shared repo → DTO mappers and score-stats helpers.

import (
	"context"
	"math"
	"strconv"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/agent"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
)

// statsOrZero dereferences stats, returning a zero-value struct when nil so the
// mapper code paths can stay branch-free.
func statsOrZero(s *scorestats.AgentScoreStats) scorestats.AgentScoreStats {
	if s == nil {
		return scorestats.AgentScoreStats{}
	}
	return *s
}

// buildScoreBreakdown builds a ScoreBreakdown DTO from a stats record.
// Reputation surfaces the raw accumulator value (unbounded), not the [0,100] component used in composite math.
func buildScoreBreakdown(s scorestats.AgentScoreStats) dto.ScoreBreakdown {
	return dto.ScoreBreakdown{
		Reputation: round2(s.ReputationScore),
		Adoption:   round2(s.AdoptionScore),
		Services:   round2(s.ServicesScore),
		Publisher:  round2(s.PublisherScore),
		Compliance: round2(s.ComplianceScore),
	}
}

// buildServicesSlice converts repo service records to DTO slice.
func buildServicesSlice(svcs []agent.RegistrationService) []dto.AgentService {
	out := make([]dto.AgentService, 0, len(svcs))
	for _, sv := range svcs {
		out = append(out, dto.AgentService{
			Name:     sv.Name,
			Endpoint: sv.Endpoint,
			Version:  sv.Version,
			Skills:   sv.Skills,
			Domains:  sv.Domains,
		})
	}
	return out
}

// mapOnchainMeta converts repo onchain metadata map to DTO map.
func mapOnchainMeta(meta map[string]agent.OnchainMetadataValue) map[string]dto.OnchainMetadataValue {
	out := make(map[string]dto.OnchainMetadataValue, len(meta))
	for k, v := range meta {
		out[k] = dto.OnchainMetadataValue{
			RawHex:       v.RawHex,
			Decoded:      v.Decoded,
			DetectedType: v.DetectedType,
			Confidence:   v.Confidence,
		}
	}
	return out
}

// toAgentRow projects the document + stats into AgentRow.
// stats may be nil — fields will default to zero in that case.
func toAgentRow(d agent.AgentDocument, stats *scorestats.AgentScoreStats, _ scoring.FormulaConfig, _ int64) dto.AgentRow {
	s := statsOrZero(stats)
	breakdown := buildScoreBreakdown(s)
	services := buildServicesSlice(d.Services)
	return dto.AgentRow{
		ChainID:          d.ChainID,
		AgentID:          d.AgentID,
		Name:             d.Name,
		Image:            d.Image,
		Owner:            d.Owner,
		TrustScore:       round2(s.CompositeScore),
		ReputationScore:  round2(s.ReputationScore),
		ScoreBreakdown:   breakdown,
		ScoreUpdateAt:    s.ScoreUpdateAt,
		ConsecutiveFails: s.ConsecutiveFails,
		TotalTasks:       s.TotalTasks,
		TotalFeedbacks:   feedbackTotal(d.TotalFeedbacks, nil),
		TotalPassed:      s.TotalPassed,
		TotalFailed:      s.TotalFailed,
		SuccessRate:      safeRate(s.TotalPassed, s.TotalTasks),
		Active:           d.Active,
		X402Support:      d.X402Support,
		HasOASF:          d.HasOASF,
		Domains:          d.Domains,
		OASFSkills:       d.OASFSkills,
		OASFDomains:      d.OASFDomains,
		Services:         services,
		Tags:             d.Tags,
		CreatedAt:        d.CreatedAt,
	}
}

// toAgentProfile builds the full profile payload from the agent doc and stats.
// stats may be nil — defaults apply.
func toAgentProfile(d *agent.AgentDocument, stats *scorestats.AgentScoreStats, dist map[string]int64, cfg scoring.FormulaConfig, _ int64) dto.AgentProfile {
	s := statsOrZero(stats)
	// v2 "penalty" = reliability reduction in [0,100]: how much the current fail streak docks reputation.
	penalty := (1.0 - scoring.ComputeReliability(s.ConsecutiveFails, cfg.Gamma, cfg.Theta)) * 100.0
	breakdown := buildScoreBreakdown(s)
	services := buildServicesSlice(d.Services)
	onchain := mapOnchainMeta(d.OnchainMetadata)

	return dto.AgentProfile{
		ChainID:          d.ChainID,
		AgentID:          d.AgentID,
		Owner:            d.Owner,
		AgentURI:         d.AgentURI,
		Name:             d.Name,
		Description:      d.Description,
		Image:            d.Image,
		Active:           d.Active,
		X402Support:      d.X402Support,
		SupportedTrust:   d.SupportedTrust,
		Domains:          d.Domains,
		HasOASF:          d.HasOASF,
		OASFSkills:       d.OASFSkills,
		OASFDomains:      d.OASFDomains,
		Services:         services,
		OnchainMetadata:  onchain,
		OffchainMetadata: d.OffchainMetadata,
		CreatedAt:        unixToRFC3339(d.CreatedAt),
		Scoring: dto.AgentScoring{
			TrustScore:        round2(s.CompositeScore),
			ReputationScore:   round2(s.ReputationScore),
			AdoptionScore:     round2(s.AdoptionScore),
			ScoreBreakdown:    breakdown,
			ScoreUpdateAt:     s.ScoreUpdateAt,
			ConsecutiveFails:  s.ConsecutiveFails,
			Penalty:           round2(penalty),
			TotalTasks:        s.TotalTasks,
			TotalFeedbacks:    feedbackTotal(d.TotalFeedbacks, dist),
			TotalPassed:       s.TotalPassed,
			TotalFailed:       s.TotalFailed,
			SuccessRate:       safeRate(s.TotalPassed, s.TotalTasks),
			ClassDistribution: dist,
		},
	}
}

// toFeedbackRow projects a FeedbackRecord into the public FeedbackRow shape.
func toFeedbackRow(r feedback.FeedbackRecord) dto.FeedbackRow {
	resp := make([]dto.FeedbackResponse, 0, len(r.Responses))
	for _, x := range r.Responses {
		resp = append(resp, dto.FeedbackResponse{
			Responder:      x.Responder,
			ResponseURI:    x.ResponseURI,
			ResponseHash:   x.ResponseHash,
			TxHash:         x.TxHash,
			ResponseParsed: x.ResponseParsed,
		})
	}
	return dto.FeedbackRow{
		ID:             r.ID,
		FeedbackIndex:  r.FeedbackIndex,
		ClientAddress:  r.ClientAddress,
		Value:          r.Value,
		ValueDecimals:  r.ValueDecimals,
		ValueScale:     r.ValueScale,
		Vi:             computeVi(r),
		Wi:             round2(r.Wi),
		PriceUSDC:      r.PriceUSDC,
		Tag1:           r.Tag1,
		Tag2:           r.Tag2,
		Endpoint:       r.Endpoint,
		Unit:           r.Unit,
		FeedbackURI:    r.FeedbackURI,
		FeedbackHash:   r.FeedbackHash,
		FeedbackParsed: r.FeedbackParsed,
		Classification: dto.FeedbackClassification{
			Rule: dto.RuleClassification{Category: r.Classification.Rule.Category},
			Fallback: func() *dto.FallbackClassification {
				if r.Classification.Fallback == nil {
					return nil
				}
				return &dto.FallbackClassification{
					Category:   r.Classification.Fallback.Category,
					Reason:     r.Classification.Fallback.Reason,
					Confidence: r.Classification.Fallback.Confidence,
				}
			}(),
		},
		TxHash:        r.TxHash,
		BlockNumber:   r.BlockNumber,
		Timestamp:     unixToRFC3339(r.Timestamp),
		TimestampUnix: r.Timestamp,
		RevokeTxHash:  r.RevokeTxHash,
		Responses:     resp,
	}
}

// computeVi recomputes the validation score at runtime from the stored raw value and scale.
// vi is not persisted — ValueScale is the source of truth.
func computeVi(r feedback.FeedbackRecord) float64 {
	real, ok := classifier.RawValueToReal(r.Value, int(r.ValueDecimals))
	if !ok {
		return 0.0
	}
	return classifier.NormalizeValueWithScale(real, r.ValueScale)
}

// toIdentityEvent projects one identity change.
func toIdentityEvent(c identity.IdentityChange) dto.IdentityEvent {
	return dto.IdentityEvent{
		EventName:   c.EventName,
		AgentURI:    c.AgentURI,
		Owner:       c.Owner,
		EventArgs:   c.EventArgs,
		URIParsed:   c.URIParsed,
		BlockNumber: c.BlockNumber,
		TxHash:      c.TxHash,
		Timestamp:   unixToRFC3339(c.Timestamp),
	}
}

// ── small helpers ───────────────────────────────────────────────────────────

func safeRate(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return round4(float64(num) / float64(den))
}

// feedbackTotal prefers the denormalized agents.totalFeedbacks; falls back to summing
// class distribution for agents not yet backfilled by score-refresh.
func feedbackTotal(denorm int64, dist map[string]int64) int64 {
	if denorm > 0 {
		return denorm
	}
	var sum int64
	for _, v := range dist {
		sum += v
	}
	return sum
}

func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}

func round4(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*10000) / 10000
}

func unixToRFC3339(sec int64) string {
	if sec <= 0 {
		return ""
	}
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}

// bulkFetchStats fetches stats for each (chainID, agentID) pair from docs. The map is
// keyed by statsKey so multi-chain pages don't collapse rows with duplicate agentIDs.
func bulkFetchStats(ctx context.Context, repo agentScoreStatsRepo, docs []agent.AgentDocument) map[string]*scorestats.AgentScoreStats {
	out := make(map[string]*scorestats.AgentScoreStats, len(docs))
	for _, d := range docs {
		if s, _ := repo.FindByAgentID(ctx, d.ChainID, d.AgentID); s != nil {
			out[statsKey(d.ChainID, d.AgentID)] = s
		}
	}
	return out
}

func statsKey(chainID int64, agentID string) string {
	return strconv.FormatInt(chainID, 10) + ":" + agentID
}
