package trustrank

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
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

func (p *Processor) handleNewFeedback(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	clientAddress, ok := utils.GetStringArg(ev.Args, "clientAddress")
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

	cls := classifier.Classify(tag1, tag2)

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
	var vi float64
	if !realOK || isAnomalous {
		vi = 0.0
	} else {
		vi = classifier.NormalizeValueWithScale(real, scale)
	}

	// Record a tier vote for Phase 3 stats flush (only for service_feedback category).
	if cls.Category == classifier.CategoryService {
		tu := tagstats.TierUpdate{Tag1: tag1, Tag2: tag2, IsEmpty: !realOK}
		if realOK {
			tu.Tier = classifier.AssignTier(real)
		}
		bs.pendingTierUpdates = append(bs.pendingTierUpdates, tu)
	}

	priceUSDC := readPriceFromContent(bs.uriMap[feedbackURI])
	wi := scoring.ComputeWi(priceUSDC, bs.formulaCfg.Alpha, bs.formulaCfg.Beta, bs.formulaCfg.K)

	var adjustments []scoring.WiAdjustment
	if adj, ok := scoring.LowConfidenceAdjustment(cls.Confidence); ok {
		adjustments = append(adjustments, adj)
	}
	if cls.Category == classifier.CategoryService {
		adjustments = append(adjustments, scoring.ServiceFeedbackBonus(feedbackURI, bs.uriMap[feedbackURI]))
	}
	wi = scoring.ApplyAdjustments(wi, adjustments)

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
		PriceUSDC:      priceUSDC,
		Wi:             wi,
		ValueScale:     scale,
		IsSelfFeedback: isSelf,
		Type:           "reputation_feedback",
		BlockNumber:    ev.BlockNumber,
		TxHash:         ev.TxHash,
		LogIndex:       ev.LogIndex,
		Timestamp:      ev.Timestamp,
		Classification: feedback.FeedbackClassification{
			Rule: feedback.RuleClassification{Category: string(cls.Category)},
		},
	}
	bs.pendingFeedbacks = append(bs.pendingFeedbacks, fbRecord)

	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)
	bs.fbMap[fbID] = &fbRecord

	// Skip scoring for self-feedback, spam, noise, anomalous values, or non-service categories.
	if isSelf {
		return
	}
	if cls.Category == classifier.CategorySpam || cls.Category == classifier.CategoryNoise {
		return
	}
	if isAnomalous {
		return
	}
	if cls.Category != classifier.CategoryService {
		return
	}

	newRep := scoring.ApplyTaskScore(agentDoc.ReputationScore, agentDoc.ScoreUpdateAt, wi, vi, ev.Timestamp, bs.formulaCfg)
	newTotalTasks := agentDoc.TotalTasks + 1

	var newPassed, newFailed, newConsecFails int64
	if vi >= 0.40 {
		newPassed = agentDoc.TotalPassed + 1
		newFailed = agentDoc.TotalFailed
		newConsecFails = 0
	} else {
		newPassed = agentDoc.TotalPassed
		newFailed = agentDoc.TotalFailed + 1
		newConsecFails = agentDoc.ConsecutiveFails + 1
		// Bake the progressive penalty directly into reputationScore (unified write path).
		newRep -= scoring.ComputePenalty(newConsecFails, bs.formulaCfg.Gamma, bs.formulaCfg.Theta)
	}

	agentDoc.ReputationScore = newRep
	agentDoc.ScoreUpdateAt = ev.Timestamp
	agentDoc.ConsecutiveFails = newConsecFails
	agentDoc.TotalTasks = newTotalTasks
	agentDoc.TotalPassed = newPassed
	agentDoc.TotalFailed = newFailed
	bs.dirtyAgents[agentID] = true

	// Recompute composite using newly-updated rep + cached S/P/C from last refresh cycle.
	// On first feedback (before any refresh has run), cached components default to 0/50/0.
	repNorm := scoring.NormalizeReputation(newRep)
	services := agentDoc.ServicesScore     // cached from last score-refresh
	publisher := agentDoc.PublisherScore   // cached from last score-refresh
	compliance := agentDoc.ComplianceScore // cached from last score-refresh
	if publisher == 0 {
		// Initialize neutral if not yet populated by refresh cycle.
		publisher = 50.0
	}
	composite := scoring.ComputeCompositeScore(repNorm, services, publisher, compliance, p.compositeWeights)
	if err := p.agentRepo.UpdateCompositeBreakdown(context.Background(), bs.chainID, agentID, composite, repNorm, services, publisher, compliance); err != nil {
		log.Printf("trustrank: update composite breakdown chain=%d agent=%s: %v", bs.chainID, agentID, err)
		// non-fatal — refresh worker will eventually fix
	}
}

func (p *Processor) handleFeedbackRevoked(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	clientAddress, ok := utils.GetStringArg(ev.Args, "clientAddress")
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
	clientAddress, ok := utils.GetStringArg(ev.Args, "clientAddress")
	if !ok {
		log.Printf("processor: ResponseAppended missing clientAddress: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}
	responseURI, _ := utils.GetStringArg(ev.Args, "responseURI")
	responseHash, _ := utils.GetStringArg(ev.Args, "responseHash")
	responder, _ := utils.GetStringArg(ev.Args, "responder")
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

func readPriceFromContent(content string) float64 {
	if content == "" {
		return 0.0
	}
	var parsed struct {
		PriceUSDC float64 `json:"priceUSDC"`
	}
	if err := json.Unmarshal([]byte(content), &parsed); err != nil {
		return 0.0
	}
	return parsed.PriceUSDC
}
