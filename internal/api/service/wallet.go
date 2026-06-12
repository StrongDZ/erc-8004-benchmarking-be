package service

// wallet.go — Services for /wallet/:address/* endpoints.

import (
	"context"
	"fmt"
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

// ── Interfaces (defined at consumer site per DIP) ────────────────────────────

type walletAgentRepo interface {
	FindByIDs(ctx context.Context, chainID int64, agentIDs []string) ([]agentrepo.AgentDocument, error)
}

type walletFeedbackRepo interface {
	ListByClientAddress(ctx context.Context, address string, skip, limit int64) ([]feedbackrepo.FeedbackRecord, int64, error)
	CountDistinctAgentsByClient(ctx context.Context, address string) (int64, error)
	ListDistinctAgentsByClient(ctx context.Context, address string, skip, limit int64) ([]feedbackrepo.DistinctAgentRow, error)
}

type walletWalletRepo interface {
	FindAllByAddress(ctx context.Context, address string) ([]walletrepo.WalletDocument, error)
}

// WalletDeps groups the repos used by the wallet service.
type WalletDeps struct {
	Agents   walletAgentRepo
	Feedback walletFeedbackRepo
	Wallet   walletWalletRepo // may be nil; when nil, Profile returns not-found
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
			nameMap[statsKey(chainID, a.AgentID)] = a.Name
		}
	}

	rows := make([]dto.WalletFeedbackRow, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, dto.WalletFeedbackRow{
			FeedbackRow: toFeedbackRow(d),
			AgentID:     d.AgentID,
			ChainID:     d.ChainID,
			AgentName:   nameMap[statsKey(d.ChainID, d.AgentID)],
		})
	}

	return &WalletFeedbacksResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// WalletFeedbackAgentsParams carries inputs for /wallet/:address/feedback-agents.
type WalletFeedbackAgentsParams struct {
	Address string
	Page    int
	Limit   int
	Skip    int64
}

// WalletFeedbackAgentsResult carries distinct agents that received feedback from a wallet.
type WalletFeedbackAgentsResult struct {
	Rows  []dto.FeedbackAgentRow
	Total int64
	Page  int
	Limit int
}

// FeedbackAgents returns paginated distinct agents that received feedback from a wallet.
func (s *Wallet) FeedbackAgents(ctx context.Context, p WalletFeedbackAgentsParams) (*WalletFeedbackAgentsResult, error) {
	address := strings.TrimSpace(p.Address)
	if address == "" {
		return nil, fmt.Errorf("wallet: address is required")
	}

	total, err := s.deps.Feedback.CountDistinctAgentsByClient(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("wallet feedback agents: %w", err)
	}

	docs, err := s.deps.Feedback.ListDistinctAgentsByClient(ctx, address, p.Skip, int64(p.Limit))
	if err != nil {
		return nil, fmt.Errorf("wallet feedback agents: %w", err)
	}
	if len(docs) == 0 {
		return &WalletFeedbackAgentsResult{Rows: []dto.FeedbackAgentRow{}, Total: total, Page: p.Page, Limit: p.Limit}, nil
	}

	chainAgents := make(map[int64][]string)
	for _, d := range docs {
		chainAgents[d.ChainID] = append(chainAgents[d.ChainID], d.AgentID)
	}

	nameMap := make(map[string]string, len(docs))
	scoreMap := make(map[string]float64, len(docs))
	for chainID, agentIDs := range chainAgents {
		agents, err := s.deps.Agents.FindByIDs(ctx, chainID, dedupStrings(agentIDs))
		if err != nil {
			continue
		}
		for _, a := range agents {
			key := statsKey(chainID, a.AgentID)
			nameMap[key] = a.Name
			scoreMap[key] = a.CompositeScore
		}
	}

	rows := make([]dto.FeedbackAgentRow, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, dto.FeedbackAgentRow{
			AgentID:       d.AgentID,
			ChainID:       d.ChainID,
			AgentName:     nameMap[statsKey(d.ChainID, d.AgentID)],
			FeedbackCount: d.FeedbackCount,
			TrustScore:    round2(scoreMap[statsKey(d.ChainID, d.AgentID)]),
		})
	}

	return &WalletFeedbackAgentsResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

// WalletProfileResult is the response shape for GET /wallet/{address}.
type WalletProfileResult struct {
	Address            string   `json:"address"`
	ChainID            int64    `json:"chainId"`
	Kind               string   `json:"kind"`
	TrustScore         *float64 `json:"trustScore"`
	Rated              bool     `json:"rated"`
	FeedbackTotalCount int64    `json:"feedbackTotalCount"`
	FeedbackValidCount int64    `json:"feedbackValidCount"`
	FeedbackJunkCount  int64    `json:"feedbackJunkCount"`
	JunkRatio          float64  `json:"junkRatio"`
	OwnedAgentIDs      []string `json:"ownedAgentIds,omitempty"`
	ExternalScore      *float64 `json:"externalScore"`
	ExternalComplete   bool     `json:"externalComplete"`
}

// Profile returns the trust profile for a wallet address.
// When chainID > 0 it returns the record for that specific chain; otherwise it
// returns the record with the highest trustScore across all chains.
// Returns nil, nil when the wallet has not been seen yet (not an error).
func (s *Wallet) Profile(ctx context.Context, address string, chainID int64) (*WalletProfileResult, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("wallet profile: address is required")
	}
	if s.deps.Wallet == nil {
		return nil, nil
	}

	docs, err := s.deps.Wallet.FindAllByAddress(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("wallet profile: %w", err)
	}
	if len(docs) == 0 {
		return nil, nil
	}

	// Pick by chainID if requested; otherwise the first doc is highest-score (sorted desc).
	doc := &docs[0]
	if chainID > 0 {
		for i := range docs {
			if docs[i].ChainID == chainID {
				doc = &docs[i]
				break
			}
		}
	}

	rated := doc.TrustRated
	var trust *float64
	if rated {
		v := doc.TrustScore
		trust = &v
	}

	var externalScore *float64
	var externalComplete bool
	if doc.External.Present {
		v := doc.External.Score
		externalScore = &v
		externalComplete = doc.External.Complete
	}

	return &WalletProfileResult{
		Address:            doc.Address,
		ChainID:            doc.ChainID,
		Kind:               doc.Kind,
		TrustScore:         trust,
		Rated:              rated,
		FeedbackTotalCount: doc.FeedbackTotalCount,
		FeedbackValidCount: doc.FeedbackValidCount,
		FeedbackJunkCount:  doc.FeedbackJunkCount,
		JunkRatio:          doc.JunkRatio,
		OwnedAgentIDs:      doc.OwnedAgentIDs,
		ExternalScore:      externalScore,
		ExternalComplete:   externalComplete,
	}, nil
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
