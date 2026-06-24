package service

// admin.go — Admin / observability services (§6.1).

import (
	"context"
	"fmt"
	"sort"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/simulator"
	"erc-8004-benchmarking-be/internal/mq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"

	"go.mongodb.org/mongo-driver/bson"
)

// ── Interfaces (defined at consumer site per DIP) ────────────────────────────

type adminCrawlerRepo interface {
	ListAll(ctx context.Context) ([]crawlerrepo.CrawlerState, error)
}

type adminEventRepo interface {
	Count(ctx context.Context, filter any) (int64, error)
}

type adminFeedbackRepo interface {
	TotalCount(ctx context.Context, chainID int64) (int64, error)
	Count24h(ctx context.Context, chainID int64) (int64, error)
}

type adminAgentRepo interface {
	Count(ctx context.Context, filter any) (int64, error)
}

type adminContractRepo interface {
	FindByChainID(ctx context.Context, chainID int64) (contractsrepo.ContractsConfig, error)
}

// AdminDeps bundles the repos used by the admin service.
type AdminDeps struct {
	Crawlers  adminCrawlerRepo
	Events    adminEventRepo
	Feedback  adminFeedbackRepo
	Agents    adminAgentRepo
	Contracts adminContractRepo

	// Publisher sends the on-demand recompute trigger to score-refresh.
	Publisher mq.Publisher

	// SimAgents/SimFeedbacks/SimWallets back the sandbox-chain feedback simulator.
	SimAgents    *agentrepo.Repository
	SimFeedbacks *feedbackrepo.Repository
	SimWallets   *walletrepo.Repository
}

// Admin exposes observability endpoints gated behind ADMIN_API_KEY.
type Admin struct {
	deps AdminDeps
}

// NewAdmin returns a new Admin service.
func NewAdmin(deps AdminDeps) *Admin { return &Admin{deps: deps} }

// IndexerStatus implements GET /admin/indexer-status (§6.1).
// The queue metrics are left out intentionally; they require the RabbitMQ management
// API, which is a future deliverable. Workers cursor is empty until the workers expose one.
func (s *Admin) IndexerStatus(ctx context.Context) (*dto.IndexerStatus, error) {
	crawlers, err := s.deps.Crawlers.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("admin: list crawlers: %w", err)
	}
	// Group by chain; keep the most recent updatedAt and max lastProcessedBlock.
	byChain := make(map[int64]*dto.IndexerChainStatus, 8)
	for _, c := range crawlers {
		entry, ok := byChain[c.ChainID]
		if !ok {
			entry = &dto.IndexerChainStatus{ChainID: c.ChainID}
			byChain[c.ChainID] = entry
		}
		if c.LastProcessedBlock > entry.LastProcessedBlock {
			entry.LastProcessedBlock = c.LastProcessedBlock
		}
		if c.UpdatedAt > 0 {
			entry.LastIndexedAt = unixToRFC3339(c.UpdatedAt)
		}
	}
	for chainID, entry := range byChain {
		agentCount, err := s.deps.Agents.Count(ctx, bson.M{"chainId": chainID})
		if err != nil {
			return nil, fmt.Errorf("admin: agent count chain=%d: %w", chainID, err)
		}
		entry.AgentCount = agentCount

		feedbackCount, err := s.deps.Feedback.TotalCount(ctx, chainID)
		if err != nil {
			return nil, fmt.Errorf("admin: feedback count chain=%d: %w", chainID, err)
		}
		entry.FeedbackCount = feedbackCount

		cfg, err := s.deps.Contracts.FindByChainID(ctx, chainID)
		if err == nil {
			for _, ct := range cfg.Contracts {
				switch ct.Type {
				case contractsrepo.ContractTypeIdentity:
					entry.IdentityRegistry = ct.Address
				case contractsrepo.ContractTypeReputation:
					entry.ReputationRegistry = ct.Address
				}
			}
		}
	}
	chains := make([]dto.IndexerChainStatus, 0, len(byChain))
	for _, v := range byChain {
		chains = append(chains, *v)
	}
	sort.Slice(chains, func(i, j int) bool {
		return chains[i].ChainID < chains[j].ChainID
	})

	events24h, err := s.deps.Events.Count(ctx, bson.M{
		"timestamp": bson.M{"$gte": nowMinus24h()},
	})
	if err != nil {
		return nil, fmt.Errorf("admin: events24h: %w", err)
	}
	feedbacks24h, err := s.deps.Feedback.Count24h(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("admin: feedbacks24h: %w", err)
	}

	return &dto.IndexerStatus{
		Chains: chains,
		Workers: map[string]dto.WorkerStatus{
			"indexer":      {Running: len(crawlers) > 0},
			"uriBootstrap": {Running: false},
			"trustrank":    {Running: false},
			"decay":        {Running: false},
		},
		Events24H:    events24h,
		Feedbacks24H: feedbacks24h,
	}, nil
}

func nowMinus24h() int64 {
	return time.Now().Add(-24 * time.Hour).Unix()
}

// TriggerRecompute publishes an on-demand recompute request to score-refresh.
// chainID == 0 means replay every chain. Returns immediately; the replay itself
// runs asynchronously and may take several minutes (Table~tab:latency).
func (s *Admin) TriggerRecompute(ctx context.Context, chainID int64) (string, error) {
	if s.deps.Publisher == nil {
		return "", fmt.Errorf("admin: publisher not configured (RabbitMQ unavailable)")
	}
	requestID := fmt.Sprintf("recompute-%d", time.Now().UnixNano())
	msg := mq.RecomputeTriggerMessage{ChainID: chainID, RequestID: requestID}
	if err := s.deps.Publisher.Publish(ctx, mq.QueueScoreRefreshTrigger, msg); err != nil {
		return "", fmt.Errorf("admin: publish recompute trigger: %w", err)
	}
	return requestID, nil
}

// StartSimulator generates count synthetic feedback records on the sandbox
// chain for agentID (or a freshly generated one if empty), then publishes a
// recompute trigger scoped to the sandbox chain so the synthetic feedback
// flows into agent_score_stats/composite score.
func (s *Admin) StartSimulator(ctx context.Context, agentID string, count int) (simulator.Result, string, error) {
	if s.deps.Publisher == nil {
		return simulator.Result{}, "", fmt.Errorf("admin: publisher not configured (RabbitMQ unavailable)")
	}
	res, err := simulator.Generate(ctx, simulator.Deps{
		Agents:    s.deps.SimAgents,
		Feedbacks: s.deps.SimFeedbacks,
		Wallets:   s.deps.SimWallets,
	}, agentID, count)
	if err != nil {
		return simulator.Result{}, "", fmt.Errorf("admin: simulator generate: %w", err)
	}

	requestID := fmt.Sprintf("simulator-%d", time.Now().UnixNano())
	msg := mq.RecomputeTriggerMessage{ChainID: simulator.SandboxChainID, RequestID: requestID}
	if err := s.deps.Publisher.Publish(ctx, mq.QueueScoreRefreshTrigger, msg); err != nil {
		return res, "", fmt.Errorf("admin: publish scoped recompute: %w", err)
	}
	return res, requestID, nil
}
