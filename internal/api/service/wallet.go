package service

// wallet.go — Services for /wallet/:address/* endpoints.

import (
	"context"
	"fmt"
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// ── Interfaces (defined at consumer site per DIP) ────────────────────────────

type walletAgentRepo interface {
	FindByIDs(ctx context.Context, chainID int64, agentIDs []string) ([]agentrepo.AgentDocument, error)
}

type walletFeedbackRepo interface {
	ListByClientAddress(ctx context.Context, address string, skip, limit int64) ([]feedbackrepo.FeedbackRecord, int64, error)
}

// WalletDeps groups the repos used by the wallet service.
type WalletDeps struct {
	Agents   walletAgentRepo
	Feedback walletFeedbackRepo
}

// Wallet encapsulates business logic for /wallet/* endpoints.
type Wallet struct {
	deps WalletDeps
}

// NewWallet returns a new Wallet service.
func NewWallet(deps WalletDeps) *Wallet { return &Wallet{deps: deps} }

// WalletFeedbacksParams carries inputs for /wallet/:address/feedbacks.
type WalletFeedbacksParams struct {
	Address string
	Page    int
	Limit   int
	Skip    int64
}

// WalletFeedbacksResult carries the paginated feedback result.
type WalletFeedbacksResult struct {
	Rows  []dto.WalletFeedbackRow
	Total int64
	Page  int
	Limit int
}

// FeedbackGiven returns paginated feedbacks submitted by a wallet, enriched with agent names.
func (s *Wallet) FeedbackGiven(ctx context.Context, p WalletFeedbacksParams) (*WalletFeedbacksResult, error) {
	address := strings.TrimSpace(p.Address)
	if address == "" {
		return nil, fmt.Errorf("wallet: address is required")
	}

	docs, total, err := s.deps.Feedback.ListByClientAddress(ctx, address, p.Skip, int64(p.Limit))
	if err != nil {
		return nil, fmt.Errorf("wallet feedbacks: %w", err)
	}

	if len(docs) == 0 {
		return &WalletFeedbacksResult{Rows: []dto.WalletFeedbackRow{}, Total: total, Page: p.Page, Limit: p.Limit}, nil
	}

	// Group agent IDs by chain for batched name lookups.
	chainAgents := make(map[int64][]string)
	for _, d := range docs {
		chainAgents[d.ChainID] = append(chainAgents[d.ChainID], d.AgentID)
	}

	// key: "{chainId}:{agentId}" → agent name
	nameMap := make(map[string]string, len(docs))
	for chainID, agentIDs := range chainAgents {
		agents, err := s.deps.Agents.FindByIDs(ctx, chainID, dedupStrings(agentIDs))
		if err != nil {
			continue // best-effort enrichment; missing names degrade gracefully
		}
		for _, a := range agents {
			nameMap[fmt.Sprintf("%d:%s", chainID, a.AgentID)] = a.Name
		}
	}

	rows := make([]dto.WalletFeedbackRow, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, dto.WalletFeedbackRow{
			FeedbackRow: toFeedbackRow(d),
			AgentID:     d.AgentID,
			ChainID:     d.ChainID,
			AgentName:   nameMap[fmt.Sprintf("%d:%s", d.ChainID, d.AgentID)],
		})
	}

	return &WalletFeedbacksResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// dedupStrings returns a deduplicated copy of ss preserving first-seen order.
func dedupStrings(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
