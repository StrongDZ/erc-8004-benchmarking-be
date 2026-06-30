package feedbackgrade

// handler.go — handle() grades one classified feedback and live-updates the agent score.
//
// Flow (verdict-LAST):
//  1. Fetch FeedbackRecord; ErrNoDocuments → ErrTransient (nack+requeue, row not visible yet).
//  2. Already-graded guard: IsGraded(verdict) → return nil (idempotent redelivery no-op).
//  3. Grade exactly once: scoring.GradeFeedback (same call the replay makes).
//  4. Live reputation — quality only (skip revoked/self): under the per-agent mutex,
//     read agent_score_stats, apply one ApplyFeedbackToMass increment, recompute
//     reputation + composite, UpsertFromWritePath, then sync composite to the agent doc.
//  5. Verdict-last: UpdateWeighting (wi / qualityScore / validationVerdict / reason) — the
//     IsGraded marker — written strictly AFTER the mass update.
//
// The per-feedback math mirrors one iteration of scorerefresh.replayAgent (replay.go:140-215)
// exactly: same GradeFeedback, same EffectiveWi(g.Wi, reviewerTrust) where reviewerTrust is
// the wallet's persisted trustScore (the SAME value WalletTrustBatch.TrustScore returns),
// same vi normalization, same ApplyFeedbackToMass, same reputation/composite formula + config.
//
// Idempotency note: the mass increment is not transactional with the verdict write, so a
// failure strictly between the agent_score_stats upsert and the verdict write can, on
// requeue, double-apply one increment. This is intentionally tolerated — the authoritative
// score-refresh replay recomputes A/B from scratch each cycle, healing any drift.

import (
	"context"
	"errors"
	"log"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

// ErrTransient signals a recoverable failure — caller nacks+requeues.
var ErrTransient = errors.New("transient failure")

// defaultWalletTrustScore mirrors scorerefresh.WalletTrustBatch's neutral default (50) for
// wallets with no stored trustScore, so absent-reviewer weighting matches the replay.
const defaultWalletTrustScore = 50.0

func (a *App) handle(ctx context.Context, feedbackID string) error {
	// 1. Fetch.
	fb, err := a.deps.FeedbackRepo.FindByID(ctx, feedbackID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			// Row not yet committed (publish raced the write) — requeue.
			return ErrTransient
		}
		// Any other fetch error is a transient infra fault (a _id lookup has no
		// permanent-failure mode); requeue rather than drop the live update.
		log.Printf("feedbackgrade: fetch feedback %s: %v", feedbackID, err)
		return ErrTransient
	}

	// 2. Already-graded guard: the verdict is the IsGraded marker; a set verdict means a
	//    prior delivery already processed this row (or the replay backfilled it).
	if feedbackrepo.IsGraded(fb.ValidationVerdict) {
		return nil
	}

	// IsRevoked short-circuit — matches the replay which skips revoked feedback before
	// grading (replay.go:130). No verdict, no mass, no composite update.
	if fb.IsRevoked {
		return nil
	}

	// 3. Grade once — same call, same config the replay uses.
	g := scoring.GradeFeedback(*fb, a.qwCfg)

	// 4. Live reputation: only quality feedback contributes to the reputation mass, and
	//    only when not revoked and not self-feedback (the replay's quality-mass guard).
	if feedbackrepo.EffectiveCategory(*fb) == string(classifier.CategoryQuality) && !fb.IsRevoked && !fb.IsSelfFeedback {
		if err := a.applyQuality(ctx, fb, g); err != nil {
			return err
		}
	}

	// 5. Verdict-last: write the grade (the IsGraded marker) strictly after the mass update.
	if err := a.deps.FeedbackRepo.UpdateWeighting(ctx, fb.ID, feedbackrepo.WeightingUpdate{
		Wi:           g.Wi,
		QualityScore: g.QualityScore,
		Verdict:      g.Verdict,
		Reason:       g.Reason,
		ComputedAt:   time.Now().Unix(),
	}); err != nil {
		log.Printf("feedbackgrade: update weighting %s: %v", fb.ID, err)
		return ErrTransient
	}
	return nil
}

// applyQuality applies one weighted-mean mass increment for a quality feedback to
// agent_score_stats and syncs the composite to the agent document. The agent_score_stats
// read-modify-write is serialized by the per-agent mutex.
func (a *App) applyQuality(ctx context.Context, fb *feedbackrepo.FeedbackRecord, g scoring.GradeResult) error {
	// vi: recompute from raw value + stored scale exactly as the replay does. An
	// unparseable value contributes no mass (the replay's `if !realOK { continue }`).
	real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals))
	if !ok {
		return nil
	}
	vi := classifier.NormalizeValueWithScale(real, fb.ValueScale)

	// Reviewer trust — read BEFORE the lock (independent of the agent's stats). Same source
	// the replay's trustBatch.TrustScore reads: the reviewer wallet's persisted trustScore.
	trust, err := a.reviewerTrust(ctx, fb.ChainID, fb.ClientAddress)
	if err != nil {
		log.Printf("feedbackgrade: reviewer trust %d:%s: %v", fb.ChainID, fb.ClientAddress, err)
		return ErrTransient
	}

	// totalFeedbacks is owned by score-refresh and not derivable here; read the current
	// value so BulkUpdateScores below preserves it (a plain $set would otherwise zero it).
	var totalFeedbacks int64
	if a.deps.AgentRepo != nil {
		if ag, aerr := a.deps.AgentRepo.FindByAgentID(ctx, fb.ChainID, fb.AgentID); aerr == nil && ag != nil {
			totalFeedbacks = ag.TotalFeedbacks
		}
	}

	unlock := a.lockFor(fb.ChainID, fb.AgentID)
	defer unlock()

	// Read prior accumulators (zero-value when the agent has no stats yet).
	prev, err := a.deps.ScoreStatsRepo.FindByID(ctx, fb.ChainID, fb.AgentID)
	if err != nil {
		log.Printf("feedbackgrade: load stats %d:%s: %v", fb.ChainID, fb.AgentID, err)
		return ErrTransient
	}
	// coldStart: the agent has no prior stats doc — adoption/services/publisher/compliance
	// are all zero because the score-refresh cron has not yet run for this agent.
	coldStart := prev == nil
	var cur scorestats.AgentScoreStats
	if prev != nil {
		cur = *prev
	}

	// One mass increment — identical to one iteration of replayAgent's loop.
	wi := scoring.EffectiveWi(g.Wi, trust)
	A, B := scoring.ApplyFeedbackToMass(cur.WeightedScoreSum, cur.WeightMass, cur.ScoreUpdateAt, wi, vi, fb.Timestamp, a.formulaCfg)
	rep := scoring.ComputeReputationScore(A, B, cur.ConsecutiveFails, a.formulaCfg.C, a.formulaCfg.Gamma, a.formulaCfg.Theta)
	// Composite via the SAME assembly score-refresh uses: only reputation changes live; the
	// other components (adoption/services/publisher/compliance) are carried from prev.
	comp := scoring.ComputeCompositeFromStats(
		rep, cur.AdoptionScore, cur.ServicesScore, cur.PublisherScore, cur.ComplianceScore,
		B > 0, cur.PublisherPresent, a.compositeWeights,
	)

	// Persist the updated accumulators; every non-recomputed field is carried from prev so
	// the live write touches only A/B/reputation/composite/scoreUpdateAt.
	if err := a.deps.ScoreStatsRepo.UpsertFromWritePath(ctx,
		fb.ChainID, fb.AgentID,
		rep, A, B, fb.Timestamp,
		cur.ConsecutiveFails, cur.TotalTasks, cur.TotalPassed, cur.TotalFailed, cur.MonthUniqueUsers,
		comp, rep, cur.AdoptionScore, cur.ServicesScore, cur.PublisherScore, cur.PublisherPresent, cur.ComplianceScore,
		cur.ServiceWarnings, cur.ServiceScores,
	); err != nil {
		log.Printf("feedbackgrade: upsert stats %d:%s: %v", fb.ChainID, fb.AgentID, err)
		return ErrTransient
	}

	// Sync the live composite to the agents collection (leaderboard denormalization).
	// Skip on cold-start: adoption/services/publisher/compliance are all zero (the cron has
	// not yet run for this agent), so ComputeCompositeFromStats would produce a value far
	// below the true composite. Leave the first composite to the score-refresh cron, which
	// computes all components correctly. Established agents (prev != nil) keep current behaviour.
	// Failure here is non-fatal: it is a pure denormalization the next cycle reconciles, and
	// the stats above are already committed (re-applying would double-count on requeue).
	if a.deps.AgentRepo != nil && !coldStart {
		if err := a.deps.AgentRepo.BulkUpdateScores(ctx, []agentrepo.ScoreUpdate{{
			ID:             agentrepo.AgentDocumentID(fb.ChainID, fb.AgentID),
			CompositeScore: comp,
			TotalTasks:     cur.TotalTasks,
			TotalFeedbacks: totalFeedbacks,
		}}); err != nil {
			log.Printf("feedbackgrade: sync agent score %d:%s: %v", fb.ChainID, fb.AgentID, err)
		}
	}
	return nil
}

// reviewerTrust returns the reviewer wallet's persisted trust for (chainID, address),
// defaulting to 50 when absent. This is byte-for-byte the same derivation as
// scorerefresh.WalletTrustBatch.TrustScore: WalletDocumentID(chainID, lower(address)) →
// BulkGetTrustScores (which projects the wallet's trustScore field) → 50 when not found.
func (a *App) reviewerTrust(ctx context.Context, chainID int64, address string) (float64, error) {
	if address == "" {
		return defaultWalletTrustScore, nil
	}
	id := walletrepo.WalletDocumentID(chainID, utils.NormalizeAddress(address))
	scores, err := a.deps.WalletRepo.BulkGetTrustScores(ctx, []string{id})
	if err != nil {
		return 0, err
	}
	if s, ok := scores[id]; ok {
		return s, nil
	}
	return defaultWalletTrustScore, nil
}
