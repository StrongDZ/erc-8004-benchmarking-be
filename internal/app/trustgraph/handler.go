package trustgraph

// handler.go — handle() processes one classified feedback through gate + grade + persist.
//
// Flow:
//  1. Fetch FeedbackRecord from Mongo.
//  2. Skip if already processed (idempotency guard).
//  3. Validate (gate).
//  4. UpsertCold sender wallet.
//  5a. Gated → penalty Δ-, wi=0.
//  5b. Valid → extract quality, compute wi, reward Δ+.
//  6. Write weighting fields to feedback_history.
//  7. Apply trust delta to wallet.

import (
	"context"
	"errors"
	"fmt"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/propagation"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

// ErrTransient signals a recoverable failure — caller nacks+requeues.
var ErrTransient = errors.New("transient failure")

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
	if fb.ValidationVerdict != "" && fb.ValidationVerdict != "legacy" && fb.ValidationVerdict != "pending" {
		return nil
	}

	// 3. Gate.
	verdict, err := Validate(*fb)
	if errors.Is(err, ErrPendingLLM) {
		return ErrTransient
	}
	if err != nil {
		return fmt.Errorf("trustgraph: validate %s: %w", feedbackID, err)
	}

	// 4. Ensure sender wallet.
	mu := a.walletMutex(fb.ClientAddress)
	mu.Lock()
	defer mu.Unlock()

	sender, err := a.deps.WalletRepo.UpsertCold(ctx, chainID, fb.ClientAddress, a.deps.Cfg.ColdStartT0)
	if err != nil {
		return ErrTransient
	}

	// 5. Compute weight + delta.
	now := time.Now().Unix()
	var wi, qualityScore, delta float64

	if verdict.IsGated() {
		delta = propagation.ComputePenaltyDelta(a.deps.PropCfg, verdict.Confidence)
	} else {
		qi := extractQualityInput(*fb, verdict.Confidence)
		qualityScore = propagation.ComputeQualityScore(a.deps.PropCfg, qi)
		wi = propagation.ComputeWeight(a.deps.PropCfg, sender.TrustScore, qualityScore)
		delta = propagation.ComputeRewardDelta(a.deps.PropCfg, wi)
	}

	// 6. Write feedback weighting.
	if err := a.deps.FeedbackRepo.UpdateWeighting(ctx, feedbackID, feedbackrepo.WeightingUpdate{
		Wi:           wi,
		QualityScore: qualityScore,
		Verdict:      verdict.Code,
		Reason:       verdict.Reason,
		ComputedAt:   now,
	}); err != nil {
		return ErrTransient
	}

	// 7. Apply trust delta.
	newTrust := walletClip(sender.TrustScore + delta)
	if err := a.deps.WalletRepo.ApplyTrustDelta(ctx, walletrepo.DeltaInput{
		ChainID:  chainID,
		Address:  fb.ClientAddress,
		NewTrust: newTrust,
		IsValid:  !verdict.IsGated(),
	}); err != nil {
		return ErrTransient
	}

	return nil
}

func walletClip(score float64) float64 {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}
