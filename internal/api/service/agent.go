package service

// agent.go — Types, interfaces, and constructor for /agents/* services.
// Method implementations live in agent_profile.go and agent_feedback.go.

import (
	"context"
	"errors"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
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
