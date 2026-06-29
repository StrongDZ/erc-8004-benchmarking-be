package scorerefresh

// app.go — Periodic score-refresh worker.
// Every cronExpr cycle: replay feedback_history per agent → upsert agent_score_stats
// + sync agent.reputationScore and composite breakdown with the exact replay result.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/robfig/cron/v3"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/mq"
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
	conn              *amqp.Connection
}

// NewApp creates a new scorerefresh App. conn may be nil (no on-demand
// trigger consumer started, e.g. in tests); the API process passes a real
// connection so it can publish on-demand recompute requests.
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
	conn *amqp.Connection,
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
		conn:              conn,
	}
}

// Run starts the cron scheduler (and, when a RabbitMQ connection was supplied,
// the on-demand trigger consumer) and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	c := cron.New()
	if _, err := c.AddFunc(a.cronExpr, func() { a.runCycle(ctx, 0) }); err != nil {
		return fmt.Errorf("score-refresh: add cron func: %w", err)
	}
	c.Start()
	defer func() { <-c.Stop().Done() }()

	if a.conn != nil {
		go a.consumeTriggers(ctx)
	}

	<-ctx.Done()
	return ctx.Err()
}

// consumeTriggers reads RecomputeTriggerMessage off mq.QueueScoreRefreshTrigger
// and runs a scoped (or full, if ChainID==0) replay for each one. Runs until
// ctx is cancelled; logs and continues on a single message's decode/ack error
// so one bad message cannot wedge the worker.
func (a *App) consumeTriggers(ctx context.Context) {
	ch, err := a.conn.Channel()
	if err != nil {
		log.Printf("score-refresh: trigger consumer: open channel: %v", err)
		return
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(mq.QueueScoreRefreshTrigger, true, false, false, false, nil); err != nil {
		log.Printf("score-refresh: trigger consumer: declare queue: %v", err)
		return
	}

	deliveries, err := ch.Consume(mq.QueueScoreRefreshTrigger, "score-refresh-trigger", false, false, false, false, nil)
	if err != nil {
		log.Printf("score-refresh: trigger consumer: consume: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			var msg mq.RecomputeTriggerMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("score-refresh: trigger consumer: decode: %v", err)
				_ = d.Nack(false, false)
				continue
			}
			log.Printf("score-refresh: on-demand recompute triggered chainId=%d requestId=%s", msg.ChainID, msg.RequestID)
			a.runCycle(ctx, msg.ChainID)
			_ = d.Ack(false)
		}
	}
}

// ownerAgentEntry is a minimal per-agent snapshot used to compute the O component of WalletTrust.
type ownerAgentEntry struct {
	directRep  float64 // AgentDirectReputation (composite without publisher)
	weightMass float64 // evidence mass B = Σ wᵢ·dᵢ
}

func (a *App) runCycle(ctx context.Context, chainID int64) {
	now := time.Now().Unix()
	log.Printf("score-refresh: cycle start ts=%d", now)

	const batchSize = 200
	skip := int64(0)
	total := 0

	cw := scoring.DefaultCompositeWeights()
	ownerToAgents := make(map[string][]ownerAgentEntry) // wallet doc ID → agent entries

	for {
		agents, err := a.agents.FindAllFiltered(ctx, chainID, skip, batchSize)
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
	a.computeAndWriteWalletTrust(ctx, chainID, ownerToAgents, now)
}

// computeAndWriteWalletTrust derives WalletTrust for the wallets in scope and writes
// trustScore via BulkSetTrustScore. Called once per score-refresh cycle after all agent
// scores have been replayed so the O-component (owned-agent quality) is fresh. chainID
// scopes the wallet set to match the replay scope (0 = all chains, the full cron cycle);
// a scoped recompute must not touch other chains' wallets, whose owned-agent stats were
// not replayed this cycle.
func (a *App) computeAndWriteWalletTrust(ctx context.Context, chainID int64, ownerToAgents map[string][]ownerAgentEntry, now int64) {
	allWallets, err := a.wallets.ScanAll(ctx, chainID)
	if err != nil {
		log.Printf("score-refresh: wallet trust: scan wallets: %v", err)
		return
	}

	wtWeights := scoring.DefaultWalletTrustWeights()
	scores := make([]agentrepo.WalletScore, 0, len(allWallets))
	var unratedIDs []string

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

		// A wallet is rated only with real evidence: feedback history, external enrichment,
		// or owned-agent quality. Evidence-less wallets (e.g. a one-off feedback sender with
		// no valid/junk counts) would otherwise tie at the neutral ~50 and flood the rated
		// ranking, so they are marked unrated instead.
		hasEvidence := wd.FeedbackValidCount+wd.FeedbackJunkCount > 0 || ePresent || oPresent
		if !hasEvidence {
			unratedIDs = append(unratedIDs, wd.ID)
			continue
		}

		wt := scoring.ComputeWalletTrust(R, E, O, ePresent, oPresent, wtWeights)
		scores = append(scores, agentrepo.WalletScore{ID: wd.ID, Score: wt, At: now})
	}

	if err := a.wallets.BulkSetTrustScore(ctx, scores); err != nil {
		log.Printf("score-refresh: wallet trust: bulk write: %v", err)
		return
	}
	if len(unratedIDs) > 0 {
		if err := a.wallets.BulkSetUnrated(ctx, unratedIDs, now); err != nil {
			log.Printf("score-refresh: wallet trust: bulk set unrated: %v", err)
		}
	}
	log.Printf("score-refresh: wallet trust updated rated=%d unrated=%d", len(scores), len(unratedIDs))
}
