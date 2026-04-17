package service

// admin.go — Admin / observability services (§6.1).

import (
	"context"
	"fmt"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"

	"go.mongodb.org/mongo-driver/bson"
)

// AdminDeps bundles the repos used by the admin service.
type AdminDeps struct {
	Crawlers *crawlerrepo.Repository
	Events   *eventrepo.Repository
	Feedback *feedbackrepo.Repository
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
		if c.Status != "" {
			entry.CrawlerStatus = c.Status
		}
		if c.LastError != "" {
			entry.LastError = c.LastError
		}
		if c.ActiveRPC != "" {
			entry.ActiveRPC = c.ActiveRPC
		}
		if entry.LastUpdatedAt == "" || c.UpdatedAt > 0 {
			entry.LastUpdatedAt = unixToRFC3339(c.UpdatedAt)
		}
	}
	chains := make([]dto.IndexerChainStatus, 0, len(byChain))
	for _, v := range byChain {
		chains = append(chains, *v)
	}

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
