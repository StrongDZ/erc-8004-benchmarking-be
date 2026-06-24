package simulator

// simulator.go — Sandbox feedback generator for POST /admin/simulator/start.
//
// All synthetic data lands on SandboxChainID (999999001), which is never
// marked active: true in the contracts collection, so it cannot contaminate
// live chain crawl cursors or the real agent graph.

import (
	"context"
	"fmt"
	"time"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

// SandboxChainID is the reserved chain ID for all simulator-generated data.
// It is intentionally far outside the range of any real EVM chain and is
// never inserted into the contracts collection, so the indexer and TrustRank
// workers will never crawl or process it.
const SandboxChainID int64 = 999999001

// SandboxReviewer is the synthetic wallet address used as clientAddress for
// every simulator-generated feedback record.
const SandboxReviewer = "0x000000000000000000000000000000000000dead"

// Deps holds the repositories the simulator writes to.
type Deps struct {
	Agents    *agentrepo.Repository
	Feedbacks *feedbackrepo.Repository
	Wallets   *walletrepo.Repository
}

// Result summarises the simulator run.
type Result struct {
	AgentID string `json:"agentId"`
	Count   int    `json:"count"`
}

// Generate creates count synthetic feedback records on SandboxChainID for
// agentID. If agentID is empty a stable synthetic agent is upserted and its
// ID is returned in Result. Each record is classified by the real rule
// classifier so the subsequent score-refresh replay exercises the full
// production weighting path.
func Generate(ctx context.Context, deps Deps, agentID string, count int) (Result, error) {
	if count <= 0 || count > 100 {
		return Result{}, fmt.Errorf("simulator: count must be 1–100, got %d", count)
	}

	agentID, err := ensureSandboxAgent(ctx, deps.Agents, agentID)
	if err != nil {
		return Result{}, fmt.Errorf("simulator: ensure sandbox agent: %w", err)
	}

	// Ensure reviewer wallet exists in the wallets collection.
	if _, _, err := deps.Wallets.UpsertCold(ctx, SandboxChainID, SandboxReviewer, 0); err != nil {
		return Result{}, fmt.Errorf("simulator: upsert reviewer wallet: %w", err)
	}

	now := time.Now().Unix()
	for i := 0; i < count; i++ {
		if err := scoreOne(ctx, deps, agentID, uint64(i), now, i); err != nil {
			return Result{}, fmt.Errorf("simulator: record %d: %w", i, err)
		}
	}

	return Result{AgentID: agentID, Count: count}, nil
}

// scoreOne writes one synthetic feedback record.
func scoreOne(ctx context.Context, deps Deps, agentID string, feedbackIndex uint64, now int64, slot int) error {
	fix := fixturePool[slot%len(fixturePool)]

	cls := classifier.Classify(fix.tag1, fix.tag2, fix.scale)

	rec := feedbackrepo.FeedbackRecord{
		AgentID:       agentID,
		ChainID:       SandboxChainID,
		ClientAddress: SandboxReviewer,
		FeedbackIndex: feedbackIndex,
		Value:         fix.value,
		ValueDecimals: 0,
		Tag1:          fix.tag1,
		Tag2:          fix.tag2,
		ValueScale:    fix.scale,
		BlockNumber:   0,
		TxHash:        fmt.Sprintf("0xsim%020d", feedbackIndex),
		LogIndex:      0,
		Timestamp:     now,
		Type:          "reputation_feedback",
		Category:      string(cls.Category),
		Feature:       string(cls.Feature),
		Classification: feedbackrepo.FeedbackClassification{
			Rule: feedbackrepo.RuleClassification{
				Category: string(cls.Category),
				Feature:  string(cls.Feature),
			},
		},
		Wi:           1.0,
		WiComputedAt: now,
		QualityScore: cls.Confidence,
	}

	if err := deps.Feedbacks.Upsert(ctx, rec); err != nil {
		return fmt.Errorf("upsert feedback: %w", err)
	}

	isValid := cls.Category == classifier.CategoryQuality || cls.Category == classifier.CategoryQuantity
	if err := deps.Wallets.IncrementFeedbackCounters(ctx, SandboxChainID, SandboxReviewer, isValid); err != nil {
		return fmt.Errorf("increment wallet counters: %w", err)
	}

	return nil
}

// ensureSandboxAgent returns agentID if non-empty; otherwise upserts a
// placeholder agent document on SandboxChainID and returns "sim-agent-0".
func ensureSandboxAgent(ctx context.Context, repo *agentrepo.Repository, agentID string) (string, error) {
	if agentID != "" {
		return agentID, nil
	}
	agentID = "sim-agent-0"
	doc := agentrepo.AgentDocument{
		AgentID: agentID,
		ChainID: SandboxChainID,
		Owner:   SandboxReviewer,
		Active:  false,
	}
	if err := repo.Upsert(ctx, doc); err != nil {
		return "", err
	}
	return agentID, nil
}
