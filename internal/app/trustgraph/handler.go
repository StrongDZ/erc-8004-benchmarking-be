package trustgraph

// handler.go — handle() processes one classified feedback through gate + grade + persist.
//
// Flow:
//  1. Fetch FeedbackRecord from Mongo.
//  2. Skip if already processed (idempotency guard).
//  3. Validate (gate).
//  4. UpsertCold sender wallet.
//  5. Compute weight (no delta; propagation owns trustScore).
//  6. Write weighting fields to feedback_history.
//  7. Increment reviewer counters (valid/junk).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/propagation"
	"erc-8004-benchmarking-be/internal/mq"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// ErrTransient signals a recoverable failure — caller nacks+requeues.
var ErrTransient = errors.New("transient failure")

// ErrLLMUnavailable is returned by resolveLLMFallback when the LLM service is down.
var ErrLLMUnavailable = errors.New("llm service unavailable")

func (a *App) handle(ctx context.Context, feedbackID string, chainID int64) error {
	// 1. Fetch.
	fb, err := a.deps.FeedbackRepo.FindByID(ctx, feedbackID)
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return ErrTransient
		}
		return fmt.Errorf("trustgraph: fetch feedback %s: %w", feedbackID, err)
	}

	// 2. Idempotency: skip if already graded.
	if feedbackrepo.IsGraded(fb.ValidationVerdict) {
		return nil
	}

	// 3. Gate.
	verdict, err := Validate(*fb)
	if errors.Is(err, ErrPendingLLM) {
		if a.deps.Classifier == nil {
			if err := a.persistPendingLLMUnavailable(ctx, feedbackID); err != nil {
				return ErrTransient
			}
			return nil
		}
		if llmErr := a.resolveLLMFallback(ctx, fb); llmErr != nil {
			if errors.Is(llmErr, ErrLLMUnavailable) {
				if err := a.persistPendingLLMUnavailable(ctx, feedbackID); err != nil {
					return ErrTransient
				}
				return nil
			}
			return ErrTransient
		}
		verdict, err = Validate(*fb)
	}
	if err != nil {
		return fmt.Errorf("trustgraph: validate %s: %w", feedbackID, err)
	}

	// 4. Ensure sender wallet.
	mu := a.walletMutex(fb.ClientAddress)
	mu.Lock()
	defer mu.Unlock()

	_, wasNew, err := a.deps.WalletRepo.UpsertCold(ctx, chainID, fb.ClientAddress, a.deps.Cfg.ColdStartT0)
	if err != nil {
		return ErrTransient
	}
	if err := mq.PublishWalletEnrich(ctx, a.deps.Publisher, wasNew, chainID, fb.ClientAddress); err != nil {
		log.Printf("trustgraph: publish wallet enrich (chain=%d addr=%s): %v", chainID, fb.ClientAddress, err)
	}

	// 5. Compute weight (for reputation) — no trust delta; propagation owns trustScore.
	now := time.Now().Unix()
	var wi, qualityScore float64
	if !verdict.IsGated() {
		qi := extractQualityInput(*fb, verdict.Confidence)
		qualityScore = propagation.ComputeQualityScore(a.deps.PropCfg, qi)
		wi = propagation.ComputeWeight(a.deps.PropCfg, qualityScore)
	}

	// 6. Write feedback weighting.
	if err := a.deps.FeedbackRepo.UpdateWeighting(ctx, feedbackID, feedbackrepo.WeightingUpdate{
		Wi: wi, QualityScore: qualityScore, Verdict: verdict.Code, Reason: verdict.Reason, ComputedAt: now,
	}); err != nil {
		return ErrTransient
	}

	// 7. Increment reviewer counters (valid/junk). trustScore is set by trustrank-pass.
	if err := a.deps.WalletRepo.IncrementFeedbackCounters(ctx, chainID, fb.ClientAddress, !verdict.IsGated()); err != nil {
		return ErrTransient
	}
	return nil
}

// resolveLLMFallback calls the HybridClassifier for a record stuck at ErrPendingLLM,
// persists the result to MongoDB, and mutates fb.Classification.Fallback in place.
// Returns non-nil when the LLM service was unavailable (caller should nack+requeue).
func (a *App) resolveLLMFallback(ctx context.Context, fb *feedbackrepo.FeedbackRecord) error {
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

	// Load agent context so the classifier's domain stage (3tier cosine / LLM
	// agent_domain axis) can judge whether the tag is in this agent's business.
	// Best-effort: on any miss we fall through with empty agent context.
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
					Name: s.Name, Endpoint: s.Endpoint, Version: s.Version,
					Skills: s.Skills, Domains: s.Domains,
				})
			}
			in.AgentServices = svcs
		}
	}

	res, _ := a.deps.Classifier.Classify(ctx, in)

	// Source=="fallback" means the LLM service is down; caller persists pending and acks.
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
	fb.Classification.Fallback = &fallback
	fb.Category = fallback.Category
	if fallback.Feature != "" {
		fb.Feature = fallback.Feature
	}
	return nil
}

func pendingWeightingForUnavailableLLM(now int64) feedbackrepo.WeightingUpdate {
	return feedbackrepo.WeightingUpdate{
		Verdict:    "pending",
		Reason:     "llm unavailable",
		ComputedAt: now,
	}
}

func (a *App) persistPendingLLMUnavailable(ctx context.Context, feedbackID string) error {
	return a.deps.FeedbackRepo.UpdateWeighting(ctx, feedbackID, pendingWeightingForUnavailableLLM(time.Now().Unix()))
}
