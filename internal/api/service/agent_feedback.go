package service

// agent_feedback.go — Methods for agent feedbacks, penalties, offchain data, and activity heatmap.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

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
