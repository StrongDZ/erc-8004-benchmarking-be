package feedbackother

// handler.go — handle() processes one "others" feedback: LLM-classify + persist category only.
//
// Flow:
//  1. Fetch FeedbackRecord from Mongo; ErrNoDocuments → ErrTransient (nack+requeue).
//  2. Already-resolved guard: skip (return nil) if fb.Classification.Fallback != nil (LLM already ran).
//  3. Nil classifier → ErrTransient (retry when LLM is available).
//  4. Resolve LLM: build HybridInput (+ agent context from AgentRepo), call Classifier.Classify.
//     Source=="fallback" (AI down) → ErrTransient.
//     On success → UpdateFallback(category) and return nil.
//
// Grading, wallet counters, weighting, and reputation are the score-refresh replay's job.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/classifier"
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

	// 2. Already-resolved guard: skip if the LLM already ran.
	if fb.Classification.Fallback != nil {
		return nil
	}

	// 3. Nil classifier → transient (retry when LLM is available).
	if a.deps.Classifier == nil {
		return ErrTransient
	}

	// 4. Resolve LLM → UpdateFallback; grading/counters are the replay's job.
	if llmErr := a.resolveLLM(ctx, fb); llmErr != nil {
		return ErrTransient
	}
	return nil
}

// resolveLLM calls the HybridClassifier for an "others" record and persists the LLM verdict
// to MongoDB via UpdateFallback.
// Returns ErrLLMUnavailable when Source=="fallback" (service down).
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

	// Load agent context so the classifier's domain stage can judge whether the tag
	// fits this agent's business domain. Best-effort: any error or absent doc falls
	// through with empty agent context.
	if a.deps.AgentRepo != nil {
		if ag, err := a.deps.AgentRepo.FindByAgentID(ctx, fb.ChainID, fb.AgentID); err == nil && ag != nil {
			in.AgentDescription = ag.Description
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

	// Source=="fallback" means the LLM service itself is down.
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
	return nil
}
