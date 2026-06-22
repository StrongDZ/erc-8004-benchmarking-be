package scorerefresh

// app.go — Periodic score-refresh worker.
// Every cronExpr cycle: replay feedback_history per agent → upsert agent_score_stats
// + sync agent.reputationScore and composite breakdown with the exact replay result.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

// App runs the periodic score-refresh cycle.
type App struct {
	agents            *agentrepo.Repository
	feedbacks         *feedbackrepo.Repository
	scoreStats        *scorestats.Repository
	offchain          *offchainrepo.Repository
	wallets           *walletrepo.Repository
	formulaCfg        scoring.FormulaConfig
	compositeWeights  scoring.CompositeWeights
	complianceWeights scoring.ComplianceWeights
	cronExpr          string
}

// NewApp creates a new scorerefresh App.
func NewApp(
	agents *agentrepo.Repository,
	feedbacks *feedbackrepo.Repository,
	scoreStats *scorestats.Repository,
	offchain *offchainrepo.Repository,
	wallets *walletrepo.Repository,
	formulaCfg scoring.FormulaConfig,
	compositeWeights scoring.CompositeWeights,
	complianceWeights scoring.ComplianceWeights,
	cronExpr string,
) *App {
	return &App{
		agents:            agents,
		feedbacks:         feedbacks,
		scoreStats:        scoreStats,
		offchain:          offchain,
		wallets:           wallets,
		formulaCfg:        formulaCfg,
		compositeWeights:  compositeWeights,
		complianceWeights: complianceWeights,
		cronExpr:          cronExpr,
	}
}

// Run starts the cron scheduler and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	c := cron.New()
	if _, err := c.AddFunc(a.cronExpr, func() { a.runCycle(ctx) }); err != nil {
		return fmt.Errorf("score-refresh: add cron func: %w", err)
	}
	c.Start()
	<-ctx.Done()
	<-c.Stop().Done()
	return ctx.Err()
}

// ownerAgentEntry is a minimal per-agent snapshot used to compute the O component of WalletTrust.
type ownerAgentEntry struct {
	directRep  float64 // AgentDirectReputation (composite without publisher)
	weightMass float64 // evidence mass B = Σ wᵢ·dᵢ
}

func (a *App) runCycle(ctx context.Context) {
	now := time.Now().Unix()
	log.Printf("score-refresh: cycle start ts=%d", now)

	const batchSize = 200
	skip := int64(0)
	total := 0

	cw := scoring.DefaultCompositeWeights()
	ownerToAgents := make(map[string][]ownerAgentEntry) // wallet doc ID → agent entries

	for {
		agents, err := a.agents.FindAll(ctx, skip, batchSize)
		if err != nil {
			log.Printf("score-refresh: list agents skip=%d: %v", skip, err)
			return
		}
		if len(agents) == 0 {
			break
		}

		// Collect all service endpoints across the batch for a single bulk fetch.
		var allEndpoints []string
		for _, ag := range agents {
			for _, s := range ag.Services {
				if s.Endpoint != "" {
					allEndpoints = append(allEndpoints, s.Endpoint)
				}
			}
		}
		offchainStatus, err := BulkFetchOffchainStatus(ctx, a.offchain, allEndpoints)
		if err != nil {
			log.Printf("score-refresh: bulk offchain fetch skip=%d: %v", skip, err)
			// Non-fatal: proceed with empty map (services score will be 0).
			offchainStatus = map[string]int{}
		}

		statsBatch := make([]scorestats.AgentScoreStats, 0, len(agents))
		fbCounts := make([]int64, 0, len(agents))
		feedbacksByAgent := make([][]feedbackrepo.FeedbackRecord, 0, len(agents))
		for i := range agents {
			ag := &agents[i]
			fbs, err := a.feedbacks.ListByAgent(ctx, ag.ChainID, ag.AgentID)
			if err != nil {
				log.Printf("score-refresh: feedbacks chain=%d agent=%s: %v", ag.ChainID, ag.AgentID, err)
				feedbacksByAgent = append(feedbacksByAgent, nil)
				continue
			}
			feedbacksByAgent = append(feedbacksByAgent, fbs)
		}

		walletIDs := CollectWalletIDs(agents, feedbacksByAgent)
		trustBatch, err := NewWalletTrustBatch(ctx, a.wallets, walletIDs)
		if err != nil {
			log.Printf("score-refresh: wallet trust batch skip=%d: %v", skip, err)
			trustBatch = NewEmptyWalletTrustBatch()
		}
		publisherProvider := trustBatch.PublisherProvider()
		log.Printf("score-refresh: batch skip=%d agents=%d wallets=%d loaded=%d",
			skip, len(agents), len(walletIDs), trustBatch.LoadedCount())

		for i := range agents {
			ag := &agents[i]
			if i >= len(feedbacksByAgent) {
				continue
			}
			fbs := feedbacksByAgent[i]
			stats := replayAgent(
				ctx, ag, fbs, now,
				a.formulaCfg, offchainStatus,
				trustBatch, publisherProvider, a.compositeWeights, a.complianceWeights,
			)
			statsBatch = append(statsBatch, stats)
			fbCounts = append(fbCounts, int64(len(fbs)))

			// Accumulate data for WalletTrust O-component computation after all batches.
			if ag.Owner != "" {
				walletID := walletrepo.WalletDocumentID(ag.ChainID, utils.NormalizeAddress(ag.Owner))
				dr := scoring.AgentDirectReputation(stats.ReputationScore, stats.AdoptionScore, stats.ServicesScore, stats.ComplianceScore, stats.WeightMass > 0, cw)
				ownerToAgents[walletID] = append(ownerToAgents[walletID], ownerAgentEntry{
					directRep:  dr,
					weightMass: stats.WeightMass,
				})
			}
		}

		// BulkUpsert is the single write — agent_score_stats is the source of truth for scoring.
		if err := a.scoreStats.BulkUpsert(ctx, statsBatch); err != nil {
			log.Printf("score-refresh: bulk upsert stats: %v", err)
		}

		// Sync compositeScore + totalTasks + totalFeedbacks to agents collection so leaderboard
		// queries work without a join to agent_score_stats or feedback_history.
		scoreUpdates := make([]agentrepo.ScoreUpdate, 0, len(statsBatch))
		for i, s := range statsBatch {
			var fbCount int64
			if i < len(fbCounts) {
				fbCount = fbCounts[i]
			}
			scoreUpdates = append(scoreUpdates, agentrepo.ScoreUpdate{
				ID:             agentrepo.AgentDocumentID(s.ChainID, s.AgentID),
				CompositeScore: s.CompositeScore,
				TotalTasks:     s.TotalTasks,
				TotalFeedbacks: fbCount,
			})
		}
		if err := a.agents.BulkUpdateScores(ctx, scoreUpdates); err != nil {
			log.Printf("score-refresh: bulk update scores: %v", err)
		}

		total += len(agents)
		skip += int64(len(agents))
		if len(agents) < batchSize {
			break
		}
	}

	log.Printf("score-refresh: cycle done agents=%d", total)

	// Compute and persist WalletTrust for every wallet using the freshly replayed agent stats.
	a.computeAndWriteWalletTrust(ctx, ownerToAgents, now)
}

// computeAndWriteWalletTrust derives WalletTrust for every wallet in the collection and
// writes trustScore via BulkSetTrustScore. Called once per score-refresh cycle after
// all agent scores have been replayed so the O-component (owned-agent quality) is fresh.
func (a *App) computeAndWriteWalletTrust(ctx context.Context, ownerToAgents map[string][]ownerAgentEntry, now int64) {
	allWallets, err := a.wallets.ScanAll(ctx, 0)
	if err != nil {
		log.Printf("score-refresh: wallet trust: scan wallets: %v", err)
		return
	}

	wtWeights := scoring.DefaultWalletTrustWeights()
	scores := make([]agentrepo.WalletScore, 0, len(allWallets))

	for _, wd := range allWallets {
		R := scoring.ReviewerReliability(wd.FeedbackValidCount, wd.FeedbackJunkCount)
		E := wd.External.Score
		ePresent := wd.External.Present

		// O = Σ(AgentDirectReputation_i · weightMass_i) / Σ(weightMass_i)
		var O float64
		var oPresent bool
		if entries := ownerToAgents[wd.ID]; len(entries) > 0 {
			var sumN, sumD float64
			for _, e := range entries {
				sumN += e.directRep * e.weightMass
				sumD += e.weightMass
			}
			if sumD > 0 {
				O = sumN / sumD
				oPresent = true
			}
		}

		wt := scoring.ComputeWalletTrust(R, E, O, ePresent, oPresent, wtWeights)
		scores = append(scores, agentrepo.WalletScore{ID: wd.ID, Score: wt, At: now})
	}

	if err := a.wallets.BulkSetTrustScore(ctx, scores); err != nil {
		log.Printf("score-refresh: wallet trust: bulk write: %v", err)
		return
	}
	log.Printf("score-refresh: wallet trust updated count=%d", len(scores))
}
