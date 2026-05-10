package service

// mapper.go — Shared mappers repo -> DTO. Pure functions; no I/O.

import (
	"math"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/agent"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/score"
)

// toAgentRow applies lazy decay (§1.4) and projects the document into AgentRow.
func toAgentRow(d agent.AgentDocument, cfg scoring.FormulaConfig, nowUnix int64) dto.AgentRow {
	trust := scoring.ComputeCurrentScore(d.AccumulatedScore, d.ScoreUpdateAt, d.ConsecutiveFails, nowUnix, cfg)
	services := make([]dto.AgentService, 0, len(d.Services))
	for _, s := range d.Services {
		services = append(services, dto.AgentService{
			Name:     s.Name,
			Endpoint: s.Endpoint,
			Version:  s.Version,
			Skills:   s.Skills,
			Domains:  s.Domains,
		})
	}
	return dto.AgentRow{
		ChainID:          d.ChainID,
		AgentID:          d.AgentID,
		Name:             d.Name,
		Image:            d.Image,
		Owner:            d.Owner,
		TrustScore:       round2(trust),
		AccumulatedScore: round2(d.AccumulatedScore),
		ScoreUpdateAt:    d.ScoreUpdateAt,
		ConsecutiveFails: d.ConsecutiveFails,
		TotalTasks:       d.TotalTasks,
		TotalPassed:      d.TotalPassed,
		TotalFailed:      d.TotalFailed,
		SuccessRate:      safeRate(d.TotalPassed, d.TotalTasks),
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

// toAgentProfile builds the full profile payload, including lazy-decayed scoring.
func toAgentProfile(d *agent.AgentDocument, dist map[string]int64, cfg scoring.FormulaConfig, nowUnix int64) dto.AgentProfile {
	trust := scoring.ComputeCurrentScore(d.AccumulatedScore, d.ScoreUpdateAt, d.ConsecutiveFails, nowUnix, cfg)
	penalty := scoring.ComputePenalty(d.ConsecutiveFails, cfg.Gamma, cfg.Theta)

	services := make([]dto.AgentService, 0, len(d.Services))
	for _, s := range d.Services {
		services = append(services, dto.AgentService{
			Name:     s.Name,
			Endpoint: s.Endpoint,
			Version:  s.Version,
			Skills:   s.Skills,
			Domains:  s.Domains,
		})
	}

	onchain := make(map[string]dto.OnchainMetadataValue, len(d.OnchainMetadata))
	for k, v := range d.OnchainMetadata {
		onchain[k] = dto.OnchainMetadataValue{
			RawHex:       v.RawHex,
			Decoded:      v.Decoded,
			DetectedType: v.DetectedType,
			Confidence:   v.Confidence,
		}
	}

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
			TrustScore:        round2(trust),
			AccumulatedScore:  round2(d.AccumulatedScore),
			ScoreUpdateAt:     d.ScoreUpdateAt,
			ConsecutiveFails:  d.ConsecutiveFails,
			Penalty:           round2(penalty),
			TotalTasks:        d.TotalTasks,
			TotalPassed:       d.TotalPassed,
			TotalFailed:       d.TotalFailed,
			SuccessRate:       safeRate(d.TotalPassed, d.TotalTasks),
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
		Vi:             r.Vi,
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
			Category:      r.Classification.Category,
			Confidence:    r.Classification.Confidence,
			Source:        r.Classification.Source,
			NormalizedTag: r.Classification.NormalizedTag,
		},
		TxHash:       r.TxHash,
		BlockNumber:  r.BlockNumber,
		Timestamp:    unixToRFC3339(r.Timestamp),
		RevokeTxHash: r.RevokeTxHash,
		Responses:    resp,
	}
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

// toScorePoint projects one score_snapshot item.
func toScorePoint(s score.ScoreSnapshotItem) dto.ScorePoint {
	return dto.ScorePoint{
		Timestamp:  unixToRFC3339(s.Timestamp),
		Score:      round2(s.AgentScore),
		Type:       s.Type,
		TxHash:     s.TxHash,
		EventScore: round2(s.EventScore),
	}
}

// ── small helpers ───────────────────────────────────────────────────────────

func safeRate(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return round4(float64(num) / float64(den))
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
