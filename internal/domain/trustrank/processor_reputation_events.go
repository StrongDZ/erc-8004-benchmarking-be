package trustrank

import (
	"context"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/domain/serviceendpoint"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
	"erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

func (p *Processor) processReputationEvent(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	switch ev.EventName {
	case "NewFeedback":
		p.handleNewFeedback(bs, agentID, ev)
	case "FeedbackRevoked":
		p.handleFeedbackRevoked(bs, agentID, ev)
	case "ResponseAppended":
		p.handleResponseAppended(bs, agentID, ev)
	}
}

// resolvePublisherScore returns the owner wallet's publisher reputation and presence,
// matching the score-refresh WalletTrustPublisherProvider (present is always true; an
// unrated or unknown owner gets the neutral default). Seeding a brand-new agent's prev
// with this keeps the O(1) write-path composite consistent with the next full refresh.
func (p *Processor) resolvePublisherScore(ctx context.Context, owner string, chainID int64) (float64, bool) {
	const neutral = 50.0
	if p.walletRepo == nil || owner == "" {
		return neutral, true
	}
	id := wallet.WalletDocumentID(chainID, utils.NormalizeAddress(owner))
	scores, err := p.walletRepo.BulkGetTrustScores(ctx, []string{id})
	if err != nil {
		return neutral, true
	}
	if s, ok := scores[id]; ok {
		return s, true
	}
	return neutral, true
}

func (p *Processor) handleNewFeedback(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	rawClient, ok := utils.GetStringArg(ev.Args, "clientAddress")
	clientAddress := utils.NormalizeAddress(rawClient)
	if !ok {
		log.Printf("processor: NewFeedback missing clientAddress: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}
	value, _ := utils.GetStringArg(ev.Args, "value")
	valueDecimals := utils.GetUint8Arg(ev.Args, "valueDecimals")
	tag1, _ := utils.GetStringArg(ev.Args, "tag1")
	tag2, _ := utils.GetStringArg(ev.Args, "tag2")
	endpoint, _ := utils.GetStringArg(ev.Args, "endpoint")
	feedbackURI, _ := utils.GetStringArg(ev.Args, "feedbackURI")
	feedbackHash, _ := utils.GetStringArg(ev.Args, "feedbackHash")
	feedbackParsed := parseJSONObject(bs.uriMap[feedbackURI])

	// Self-feedback check: clientAddress matches owner or the agent's registered wallet.
	agentDoc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)
	isSelf := strings.EqualFold(clientAddress, agentDoc.Owner) ||
		(agentDoc.AgentWallet != "" && strings.EqualFold(clientAddress, agentDoc.AgentWallet))
	if isSelf {
		log.Printf("processor: self-feedback excluded from scoring: chain=%d agent=%s client=%s", bs.chainID, agentID, clientAddress)
	}

	isAnomalous := classifier.IsAnomalousValue(value, int(valueDecimals))
	if isAnomalous {
		log.Printf("processor: anomalous value detected (persisting, no scoring): chain=%d agent=%s value=%q", bs.chainID, agentID, value)
	}

	// Compute real value and normalize using dynamically detected scale for this (tag1, tag2) pair.
	// When scale is not yet detected, infer it from the value itself via AssignTier instead of
	// defaulting to pct100, which would severely underweight non-percentage feedback (e.g. star5=5 → 0.05).
	real, realOK := classifier.RawValueToReal(value, int(valueDecimals))
	scale := p.lookupScale(tag1, tag2)
	if scale == "" && realOK {
		scale = classifier.AssignTier(real)
	}
	// A negative value has no positive bounded scale. Force unbounded even when a
	// bounded scale was detected from positive feedback on this (tag1, tag2) pair,
	// so negatives never normalize into a quality score or classify as quality.
	if realOK && real < 0 {
		scale = "unbounded"
	}

	cls := classifier.Classify(tag1, tag2, scale)
	if isSelf {
		cls = classifier.SelfFeedbackResult()
	}

	var vi float64
	if !realOK || isAnomalous {
		vi = 0.0
	} else {
		vi = classifier.NormalizeValueWithScale(real, scale)
	}

	// Record a tier vote for Phase 3 stats flush (only for the quality category).
	if cls.Category == classifier.CategoryQuality {
		tu := tagstats.TierUpdate{Tag1: tag1, Tag2: tag2, IsEmpty: !realOK}
		if realOK {
			tu.Tier = classifier.AssignTier(real)
		}
		bs.pendingTierUpdates = append(bs.pendingTierUpdates, tu)
	}

	fbRecord := feedback.FeedbackRecord{
		AgentID:        agentID,
		ChainID:        bs.chainID,
		ClientAddress:  clientAddress,
		FeedbackIndex:  feedbackIndex,
		Value:          value,
		ValueDecimals:  valueDecimals,
		Tag1:           tag1,
		Tag2:           tag2,
		Endpoint:       endpoint,
		FeedbackURI:    feedbackURI,
		FeedbackHash:   feedbackHash,
		FeedbackParsed: feedbackParsed,
		ValueScale:     scale,
		IsSelfFeedback:      isSelf,
		Type:           "reputation_feedback",
		BlockNumber:    ev.BlockNumber,
		TxHash:         ev.TxHash,
		LogIndex:       ev.LogIndex,
		Timestamp:      ev.Timestamp,
		Category:       string(cls.Category),
		Feature:        string(cls.Feature),
		Classification: feedback.FeedbackClassification{
			Rule: feedback.RuleClassification{Category: string(cls.Category), Feature: string(cls.Feature)},
		},
	}

	// Grade inline before persisting so the row carries verdict/Wi/qualityScore.
	// "others" (rule undecided) is left ungraded and escalated to the async queue;
	// every rule-decided category — including self and junk — is graded here and
	// gets a reviewer-counter + sender-wallet intent applied during flush.
	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)
	p.gradeFeedbackInline(bs, &fbRecord, fbID, ev.Timestamp)

	bs.pendingFeedbacks = append(bs.pendingFeedbacks, fbRecord)
	agentDoc.TotalFeedbacks++
	bs.dirtyAgents[agentID] = true

	bs.fbMap[fbID] = &fbRecord

	// Skip scoring for self-feedback, junk, anomalous values, or non-quality categories.
	// Only `quality` feeds the trust score (quantity/others do not).
	if isSelf {
		return
	}
	if cls.Category == classifier.CategoryJunk {
		return
	}
	if isAnomalous {
		return
	}
	if cls.Category != classifier.CategoryQuality {
		return
	}
	matchedSvc, hasMatchedService := serviceendpoint.MatchService(agentDoc.Services, endpoint)

	// Fetch current scoring state from agent_score_stats (single source of truth).
	// If absent (first feedback ever), default to zero-value with neutral publisher.
	prev, _ := p.statsRepo.FindByID(context.Background(), bs.chainID, agentID)
	if prev == nil {
		// Brand-new agent: seed the publisher term from the owner wallet so the O(1)
		// write-path composite matches the score-refresh provider (always present=true)
		// instead of dropping the 20% publisher weight until the first refresh.
		pubScore, pubPresent := p.resolvePublisherScore(context.Background(), agentDoc.Owner, bs.chainID)
		prev = &scorestats.AgentScoreStats{ChainID: bs.chainID, AgentID: agentID, PublisherScore: pubScore, PublisherPresent: pubPresent}
	}

	// v2 weighted-mean state: decay the two mass accumulators forward and add this
	// feedback's contribution. A = Σ wᵢ·dᵢ·vᵢ, B = Σ wᵢ·dᵢ.
	newA, newB := scoring.ApplyFeedbackToMass(prev.WeightedScoreSum, prev.WeightMass, prev.ScoreUpdateAt, fbRecord.Wi, vi, ev.Timestamp, bs.formulaCfg)
	newTotalTasks := prev.TotalTasks + 1

	var newPassed, newFailed, newConsecFails int64
	if vi >= 0.40 {
		newPassed = prev.TotalPassed + 1
		newFailed = prev.TotalFailed
		newConsecFails = scoring.DecayConsecutiveFails(prev.ConsecutiveFails)
	} else {
		newPassed = prev.TotalPassed
		newFailed = prev.TotalFailed + 1
		newConsecFails = prev.ConsecutiveFails + 1
	}

	bs.dirtyAgents[agentID] = true

	// Reputation = Quality·Confidence·Reliability·100 from the freshly updated mass.
	// The consecutive-fail penalty is the Reliability factor (no separate subtraction).
	newRep := scoring.ComputeReputationScore(newA, newB, newConsecFails, bs.formulaCfg.C, bs.formulaCfg.Gamma, bs.formulaCfg.Theta)
	newServiceScores := prev.ServiceScores
	if hasMatchedService {
		newServiceScores = upsertServiceReputation(
			prev.ServiceScores,
			matchedSvc,
			fbRecord.Wi,
			vi,
			ev.Timestamp,
			bs.formulaCfg,
		)
	}

	// Adoption / S / P / C are carried forward from the last refresh cycle; the
	// score-refresh worker recomputes them authoritatively (incl. distinct clients).
	adoption := prev.AdoptionScore
	monthUniqueUsers := prev.MonthUniqueUsers
	services := prev.ServicesScore
	publisher := prev.PublisherScore
	publisherPresent := prev.PublisherPresent
	compliance := prev.ComplianceScore
	// qualityPresent is true: we just recorded a scored service feedback (B > 0).
	composite := scoring.ComputeCompositeFromStats(newRep, adoption, services, publisher, compliance, true, publisherPresent, p.compositeWeights)

	if err := p.statsRepo.UpsertFromWritePath(
		context.Background(),
		bs.chainID, agentID,
		newRep, newA, newB, ev.Timestamp,
		newConsecFails, newTotalTasks, newPassed, newFailed, monthUniqueUsers,
		composite, newRep, adoption, services, publisher, publisherPresent, compliance,
		prev.ServiceWarnings,
		newServiceScores,
	); err != nil {
		log.Printf("trustrank: upsert write-path stats chain=%d agent=%s: %v", bs.chainID, agentID, err)
		// non-fatal — refresh worker will eventually reconcile via replay.
		return
	}

	// Sync denormalized fields to agents collection immediately so leaderboard sort
	// reflects the new score without waiting for the next score-refresh cycle.
	if err := p.agentRepo.BulkUpdateScores(context.Background(), []agentrepo.ScoreUpdate{{
		ID:             agentrepo.AgentDocumentID(bs.chainID, agentID),
		CompositeScore: composite,
		TotalTasks:     newTotalTasks,
	}}); err != nil {
		log.Printf("trustrank: sync agent score chain=%d agent=%s: %v", bs.chainID, agentID, err)
	}
}

// gradeFeedbackInline grades a freshly built feedback record at ingest.
//
//   - "others" (rule could not decide): left ungraded — no verdict, no Wi, no
//     counters, no wallet — and its ID is queued for async LLM grading.
//   - every rule-decided category (quality, quantity, junk, and self — which
//     classifies as junk): graded via scoring.GradeFeedback. The verdict/Wi/
//     qualityScore are stamped onto the record, and a reviewer-counter +
//     sender-wallet intent is recorded for flush. Gated verdicts (self / junk /
//     missing_fields) yield Wi 0 and increment the junk counter; "valid" yields
//     Wi ≥ WiBase and increments the valid counter. This mirrors the old async
//     grader, which processed EVERY non-"others" feedback (self and junk included).
func (p *Processor) gradeFeedbackInline(bs *batchState, fbRecord *feedback.FeedbackRecord, fbID string, computedAt int64) {
	if fbRecord.Category == string(classifier.CategoryOthers) {
		bs.pendingOthers = append(bs.pendingOthers, fbID)
		return
	}
	g := scoring.GradeFeedback(*fbRecord, p.qualityWeightCfg)
	fbRecord.Wi = g.Wi
	fbRecord.QualityScore = g.QualityScore
	fbRecord.ValidationVerdict = g.Verdict
	fbRecord.ValidationReason = g.Reason
	fbRecord.WiComputedAt = computedAt
	bs.pendingCounters = append(bs.pendingCounters, counterIntent{chainID: bs.chainID, addr: fbRecord.ClientAddress, valid: !g.Gated})
	bs.pendingSenderWallets = append(bs.pendingSenderWallets, walletIntent{chainID: bs.chainID, addr: fbRecord.ClientAddress})
}

func upsertServiceReputation(
	current []scorestats.ServiceReputationStats,
	service serviceendpoint.ServiceMeta,
	wi, vi float64,
	ts int64,
	cfg scoring.FormulaConfig,
) []scorestats.ServiceReputationStats {
	if strings.TrimSpace(service.Endpoint) == "" {
		return current
	}
	next := append([]scorestats.ServiceReputationStats(nil), current...)
	idx := -1
	for i := range next {
		if serviceendpoint.Related(next[i].Endpoint, service.Endpoint) {
			idx = i
			break
		}
	}
	var st scorestats.ServiceReputationStats
	if idx >= 0 {
		st = next[idx]
	} else {
		st = scorestats.ServiceReputationStats{
			Name:     service.Name,
			Endpoint: service.Endpoint,
		}
	}
	if strings.TrimSpace(st.Name) == "" {
		st.Name = service.Name
	}
	if strings.TrimSpace(st.Endpoint) == "" {
		st.Endpoint = service.Endpoint
	}
	newA, newB := scoring.ApplyFeedbackToMass(st.WeightedScoreSum, st.WeightMass, st.ScoreUpdateAt, wi, vi, ts, cfg)
	st.WeightedScoreSum = newA
	st.WeightMass = newB
	st.ScoreUpdateAt = ts
	st.TotalTasks++
	if vi >= 0.40 {
		st.TotalPassed++
		st.ConsecutiveFails = scoring.DecayConsecutiveFails(st.ConsecutiveFails)
	} else {
		st.TotalFailed++
		st.ConsecutiveFails++
	}
	st.ReputationScore = scoring.ComputeReputationScore(newA, newB, st.ConsecutiveFails, cfg.C, cfg.Gamma, cfg.Theta)
	if idx >= 0 {
		next[idx] = st
		return next
	}
	return append(next, st)
}

func (p *Processor) handleFeedbackRevoked(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	rawClient, ok := utils.GetStringArg(ev.Args, "clientAddress")
	clientAddress := utils.NormalizeAddress(rawClient)
	if !ok {
		log.Printf("processor: FeedbackRevoked missing clientAddress: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}
	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)

	// Mark the feedback as revoked. Score correction is handled by the next
	// score-refresh cycle which replays feedback_history excluding revoked records.
	bs.pendingFBUpdates = append(bs.pendingFBUpdates, feedback.FeedbackUpdate{
		ID: fbID,
		Update: bson.M{
			"$set": bson.M{
				"revokeTxHash": ev.TxHash,
				"isRevoked":    true,
				"revokedAt":    ev.Timestamp,
			},
		},
	})
}

func (p *Processor) handleResponseAppended(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	rawClient, ok := utils.GetStringArg(ev.Args, "clientAddress")
	clientAddress := utils.NormalizeAddress(rawClient)
	if !ok {
		log.Printf("processor: ResponseAppended missing clientAddress: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}
	responseURI, _ := utils.GetStringArg(ev.Args, "responseURI")
	responseHash, _ := utils.GetStringArg(ev.Args, "responseHash")
	rawResponder, _ := utils.GetStringArg(ev.Args, "responder")
	responder := utils.NormalizeAddress(rawResponder)
	responseParsed := parseJSONObject(bs.uriMap[responseURI])

	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)
	bs.pendingFBUpdates = append(bs.pendingFBUpdates, feedback.FeedbackUpdate{
		ID: fbID,
		Update: bson.M{
			"$push": bson.M{
				"responses": feedback.FeedbackResponse{
					Responder:      responder,
					ResponseURI:    responseURI,
					ResponseHash:   responseHash,
					TxHash:         ev.TxHash,
					ResponseParsed: responseParsed,
				},
			},
		},
	})
}

// lookupScale returns the detected scale for a (tag1, tag2) pair from the in-memory cache.
// On cache miss it queries tag_value_stats; returns "" when not yet detected (caller should infer via AssignTier).
func (p *Processor) lookupScale(tag1, tag2 string) string {
	const cacheTTLSecs = 300 // 5 minutes

	cacheKey := tagstats.TagPairKey(tag1, tag2)
	now := time.Now().Unix()

	if v, ok := p.tagScaleCache.Load(cacheKey); ok {
		cs := v.(cachedScale)
		if cs.ExpiresAt > now {
			return cs.Scale
		}
		p.tagScaleCache.Delete(cacheKey)
	}

	if p.tagStatsRepo == nil {
		return ""
	}

	// Background context: cache miss query should not block the processing pipeline long.
	doc, err := p.tagStatsRepo.GetByTagPair(context.Background(), tag1, tag2)
	if err != nil || doc == nil {
		return ""
	}

	scale := doc.DetectedScale
	p.tagScaleCache.Store(cacheKey, cachedScale{Scale: scale, ExpiresAt: now + cacheTTLSecs})
	return scale
}
