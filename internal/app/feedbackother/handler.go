package feedbackother

// handler.go — handle() processes one "others" feedback through LLM resolve → grade → persist.
//
// Flow:
//  1. Fetch FeedbackRecord from Mongo; ErrNoDocuments → transient (nack+requeue).
//  2. Idempotency: skip if already graded.
//  3. Resolve LLM: if Classifier is nil → persist pending + ack.
//     Build HybridInput (+ agent context from AgentRepo), call Classifier.Classify.
//     Source=="fallback" (LLM down) → persist pending + ack.
//     On success → UpdateFallback + mutate fb.Category/Feature in place.
//  4. scoring.GradeFeedback(*fb, cfg) — category is now resolved; never "others".
//  5. UpsertCold sender wallet (+ WalletEnrich publish on new wallet), under per-wallet mutex.
//  6. UpdateWeighting on the feedback record.
//  7. IncrementFeedbackCounters on the sender wallet.
//
// Reputation is NOT updated here — the next score-refresh replay incorporates the
// now-resolved row (same behaviour as the old trustgraph handler for the others arm).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/mq"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// ErrTransient signals a recoverable failure — caller nacks+requeues.
var ErrTransient = errors.New("transient failure")

// ErrLLMUnavailable is returned by resolveLLM when the LLM service is down.
var ErrLLMUnavailable = errors.New("llm service unavailable")

func (a *App) handle(ctx context.Context, feedbackID string, chainID int64) error {
	// 1. Fetch.
	fb, err := a.deps.FeedbackRepo.FindByID(ctx, feedbackID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return ErrTransient
		}
		return fmt.Errorf("feedbackother: fetch feedback %s: %w", feedbackID, err)
	}

	// 2. Idempotency: skip if already graded (at-least-once redelivery guard).
	if feedbackrepo.IsGraded(fb.ValidationVerdict) {
		return nil
	}

	// 3. Resolve LLM.
	if a.deps.Classifier == nil {
		if err := a.persistPendingLLMUnavailable(ctx, feedbackID); err != nil {
			return ErrTransient
		}
		return nil
	}
	if llmErr := a.resolveLLM(ctx, fb); llmErr != nil {
		if errors.Is(llmErr, ErrLLMUnavailable) {
			if err := a.persistPendingLLMUnavailable(ctx, feedbackID); err != nil {
				return ErrTransient
			}
			return nil
		}
		return ErrTransient
	}

	// 4. Grade — category is now resolved (never "others").
	// GradeFeedback handles self / missing-fields / junk / valid internally.
	g := scoring.GradeFeedback(*fb, a.deps.PropCfg)

	// 5. Ensure sender wallet (under per-wallet mutex).
	mu := a.walletMutex(fb.ClientAddress)
	mu.Lock()
	defer mu.Unlock()

	_, wasNew, err := a.deps.WalletRepo.UpsertCold(ctx, chainID, fb.ClientAddress, a.deps.Cfg.ColdStartT0)
	if err != nil {
		return ErrTransient
	}
	if err := mq.PublishWalletEnrich(ctx, a.deps.Publisher, wasNew, chainID, fb.ClientAddress); err != nil {
		log.Printf("feedbackother: publish wallet enrich (chain=%d addr=%s): %v", chainID, fb.ClientAddress, err)
	}

	// 6. Write feedback weighting.
	now := time.Now().Unix()
	if err := a.deps.FeedbackRepo.UpdateWeighting(ctx, feedbackID, feedbackrepo.WeightingUpdate{
		Wi:           g.Wi,
		QualityScore: g.QualityScore,
		Verdict:      g.Verdict,
		Reason:       g.Reason,
		ComputedAt:   now,
	}); err != nil {
		return ErrTransient
	}

	// 7. Increment reviewer counters (valid/junk). trustScore is set by trustrank-pass.
	if err := a.deps.WalletRepo.IncrementFeedbackCounters(ctx, chainID, fb.ClientAddress, !g.Gated); err != nil {
		return ErrTransient
	}
	return nil
}

// resolveLLM calls the HybridClassifier for an "others" record, persists the LLM verdict
// to MongoDB via UpdateFallback, and mutates fb.Category/Feature in place so the caller
// can immediately call GradeFeedback on the updated record.
// Returns ErrLLMUnavailable when Source=="fallback" (service down); caller persists pending + acks.
func (a *App) resolveLLM(ctx context.Context, fb *feedbackrepo.FeedbackRecord) error {
	content := ""
	if len(fb.FeedbackParsed) > 0 {
		if b, err := json.Marshal(fb.FeedbackParsed); err == nil {
			content = string(b)
		}
	}

	in := classifier.HybridInput{
		Tag1:            fb.Tag1,
		Tag2:            fb.Tag2,
		ValueRaw:        fb.Value,
		ValueDecimals:   int(fb.ValueDecimals),
		OffchainContent: content,
		Endpoint:        fb.Endpoint,
	}

	// Load agent context so the classifier's domain stage (3-tier cosine / LLM
	// agent_domain axis) can judge whether the tag fits this agent's business domain.
	// Best-effort: any error or absent doc falls through with empty agent context.
	if a.deps.AgentRepo != nil {
		if ag, err := a.deps.AgentRepo.FindByAgentID(ctx, fb.ChainID, fb.AgentID); err == nil && ag != nil {
			desc := ag.SummarizedDescription
			if desc == "" {
				desc = ag.Description
			}
			in.AgentDescription = desc
			in.AgentOASFDomains = ag.OASFDomains
			in.AgentOASFSkills = ag.OASFSkills
			in.AgentTags = ag.Tags
			svcs := make([]classifier.AgentServicePayload, 0, len(ag.Services))
			for _, s := range ag.Services {
				svcs = append(svcs, classifier.AgentServicePayload{
					Name:     s.Name,
					Endpoint: s.Endpoint,
					Version:  s.Version,
					Skills:   s.Skills,
					Domains:  s.Domains,
				})
			}
			in.AgentServices = svcs
		}
	}

	res, _ := a.deps.Classifier.Classify(ctx, in)

	// Source=="fallback" means the LLM service itself is down (rule gave "others",
	// HybridClassifier fell through to its own "others" rule result).
	if res.Source == "fallback" {
		return ErrLLMUnavailable
	}

	fallback := feedbackrepo.FallbackClassification{
		Category:   string(res.Category),
		Feature:    string(res.Feature),
		Confidence: res.Confidence,
	}
	if err := a.deps.FeedbackRepo.UpdateFallback(ctx, fb.ID, fallback); err != nil {
		return err
	}
	// Mutate in place so the caller's GradeFeedback call sees the resolved category.
	fb.Classification.Fallback = &fallback
	fb.Category = fallback.Category
	if fallback.Feature != "" {
		fb.Feature = fallback.Feature
	}
	return nil
}

func (a *App) persistPendingLLMUnavailable(ctx context.Context, feedbackID string) error {
	return a.deps.FeedbackRepo.UpdateWeighting(ctx, feedbackID, feedbackrepo.WeightingUpdate{
		Verdict:    "pending",
		Reason:     "llm unavailable",
		ComputedAt: time.Now().Unix(),
	})
}
