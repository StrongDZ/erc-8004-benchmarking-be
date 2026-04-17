package trustrank

// processor.go — Batch event processor for TrustRank worker (Stream 2).
// Processes a batch of decoded events in 3 phases:
//   Phase 1 (Prefetch): scan batch, collect IDs/URIs, query DB once.
//   Phase 2 (Process):  sequential in-memory computation, no I/O.
//   Phase 3 (Flush):    bulk-write all results to DB.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	decoder "erc-8004-benchmarking-be/internal/decoder/evm"
	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/agent"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/score"
	"erc-8004-benchmarking-be/internal/utils"

	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
)

// NewProcessor constructs a Processor with all required dependencies.
func NewProcessor(
	agentRepo *agent.Repository,
	identityRepo *identityrepo.Repository,
	feedbackRepo *feedback.Repository,
	scoreRepo *score.Repository,
	offchainRepo *offchain.Repository,
	formulaCfg scoring.FormulaConfig,
) *Processor {
	return &Processor{
		agentRepo:    agentRepo,
		identityRepo: identityRepo,
		feedbackRepo: feedbackRepo,
		scoreRepo:    scoreRepo,
		offchainRepo: offchainRepo,
		formulaCfg:   formulaCfg,
	}
}

func newBatchState(chainID int64, cfg scoring.FormulaConfig) *batchState {
	return &batchState{
		chainID:     chainID,
		formulaCfg:  cfg,
		agentMap:    make(map[string]*agent.AgentDocument),
		uriMap:      make(map[string]string),
		fbMap:       make(map[string]*feedback.FeedbackRecord),
		dirtyAgents: make(map[string]bool),
	}
}

// ── ProcessBatch: orchestrator ─────────────────────────────────────────────────

// ProcessBatch handles a batch of decoded events: prefetch -> process -> flush.
func (p *Processor) ProcessBatch(ctx context.Context, chainID int64, events []eventrepo.DecodedEvent) error {
	if len(events) == 0 {
		return nil
	}

	bs := newBatchState(chainID, p.formulaCfg)

	if err := p.prefetch(ctx, chainID, events, bs); err != nil {
		return fmt.Errorf("processor: prefetch: %w", err)
	}

	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.processEvent(bs, ev)
	}

	if err := p.flush(ctx, bs); err != nil {
		return fmt.Errorf("processor: flush: %w", err)
	}

	return nil
}

// ── Phase 1: Prefetch ──────────────────────────────────────────────────────────

func (p *Processor) prefetch(ctx context.Context, chainID int64, events []eventrepo.DecodedEvent, bs *batchState) error {
	agentIDSet := make(map[string]bool)
	uriSet := make(map[string]bool)
	var revokedFBIDs []string

	for _, ev := range events {
		agentID, ok := utils.GetStringArg(ev.Args, "agentId")
		if ok {
			agentIDSet[agentID] = true
		}

		switch {
		case ev.ContractType == "identity" && ev.EventName == "Registered":
			if uri, ok := utils.GetStringArg(ev.Args, "agentURI"); ok {
				uriSet[uri] = true
			}
		case ev.ContractType == "identity" && ev.EventName == "URIUpdated":
			if uri, ok := utils.GetStringArg(ev.Args, "newURI"); ok {
				uriSet[uri] = true
			}
		case ev.ContractType == "reputation" && ev.EventName == "NewFeedback":
			if uri, ok := utils.GetStringArg(ev.Args, "feedbackURI"); ok {
				uriSet[uri] = true
			}
		case ev.ContractType == "reputation" && ev.EventName == "ResponseAppended":
			if uri, ok := utils.GetStringArg(ev.Args, "responseURI"); ok {
				uriSet[uri] = true
			}
		case ev.ContractType == "reputation" && ev.EventName == "FeedbackRevoked":
			agentID, _ := utils.GetStringArg(ev.Args, "agentId")
			clientAddr, ok := utils.GetStringArg(ev.Args, "clientAddress")
			fbIdx := utils.GetUint64Arg(ev.Args, "feedbackIndex")
			if agentID != "" && ok {
				revokedFBIDs = append(revokedFBIDs, feedback.FeedbackDocumentID(chainID, agentID, clientAddr, fbIdx))
			}
		}
	}

	agentIDs := make([]string, 0, len(agentIDSet))
	for id := range agentIDSet {
		agentIDs = append(agentIDs, id)
	}
	uris := make([]string, 0, len(uriSet))
	for uri := range uriSet {
		uris = append(uris, uri)
	}

	agentDocs, err := p.agentRepo.FindByIDs(ctx, chainID, agentIDs)
	if err != nil {
		return fmt.Errorf("prefetch agents: %w", err)
	}
	bs.agentMap = indexAgentsByAgentID(agentDocs)

	offchainDocs, err := p.offchainRepo.FindByURIs(ctx, uris)
	if err != nil {
		return fmt.Errorf("prefetch offchain: %w", err)
	}
	bs.uriMap = indexURIToContent(uris, offchainDocs)

	fbDocs, err := p.feedbackRepo.FindByIDs(ctx, revokedFBIDs)
	if err != nil {
		return fmt.Errorf("prefetch feedback: %w", err)
	}
	bs.fbMap = indexFeedbackByID(fbDocs)

	return nil
}

// indexAgentsByAgentID builds the agentID -> *doc map used for in-batch mutation.
func indexAgentsByAgentID(docs []agent.AgentDocument) map[string]*agent.AgentDocument {
	if len(docs) == 0 {
		return make(map[string]*agent.AgentDocument)
	}
	m := make(map[string]*agent.AgentDocument, len(docs))
	for i := range docs {
		m[docs[i].AgentID] = &docs[i]
	}
	return m
}

func indexFeedbackByID(docs []feedback.FeedbackRecord) map[string]*feedback.FeedbackRecord {
	if len(docs) == 0 {
		return make(map[string]*feedback.FeedbackRecord)
	}
	m := make(map[string]*feedback.FeedbackRecord, len(docs))
	for i := range docs {
		m[docs[i].ID] = &docs[i]
	}
	return m
}

// indexURIToContent maps requested URI -> cached JSON for rows with successful content.
func indexURIToContent(requestedURIs []string, docs []offchain.OffchainData) map[string]string {
	idToURI := make(map[string]string, len(requestedURIs))
	for _, u := range requestedURIs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		idToURI[offchain.OffchainDocumentID(u)] = u
	}
	out := make(map[string]string)
	for i := range docs {
		doc := &docs[i]
		if doc.Content == "" || doc.FetchError != "" {
			continue
		}
		uri := idToURI[doc.ID]
		if uri == "" {
			uri = strings.TrimSpace(doc.URI)
		}
		if uri != "" {
			out[uri] = doc.Content
		}
	}
	return out
}

// ── Phase 2: Process in-memory ─────────────────────────────────────────────────

func (p *Processor) processEvent(bs *batchState, ev eventrepo.DecodedEvent) {
	agentID, ok := utils.GetStringArg(ev.Args, "agentId")
	if !ok {
		return
	}

	switch ev.ContractType {
	case "identity":
		p.processIdentityEvent(bs, agentID, ev)
	case "reputation":
		p.processReputationEvent(bs, agentID, ev)
	}
}

// ── Identity Events ────────────────────────────────────────────────────────────

func (p *Processor) processIdentityEvent(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	switch ev.EventName {
	case "Registered":
		p.handleRegistered(bs, agentID, ev)
	case "URIUpdated":
		p.handleURIUpdated(bs, agentID, ev)
	case "MetadataSet":
		p.handleMetadataSet(bs, agentID, ev)
	}
}

func (p *Processor) handleRegistered(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	owner, _ := utils.GetStringArg(ev.Args, "owner")
	agentURI, _ := utils.GetStringArg(ev.Args, "agentURI")

	doc := bs.agentMap[agentID]
	if doc == nil {
		doc = &agent.AgentDocument{
			AgentID:          agentID,
			ChainID:          bs.chainID,
			AccumulatedScore: 0,
		}
		bs.agentMap[agentID] = doc
	}
	doc.Owner = owner
	doc.AgentURI = agentURI
	doc.CreatedAt = ev.Timestamp
	doc.ScoreUpdateAt = ev.Timestamp
	bs.dirtyAgents[agentID] = true

	p.appendIdentityHistory(bs, agentID, ev, agentURI, owner)

	if agentURI != "" {
		p.applyIdentityFromURI(bs, agentID, agentURI)
	}
}

func (p *Processor) handleURIUpdated(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	newURI, _ := utils.GetStringArg(ev.Args, "newURI")

	p.appendIdentityHistory(bs, agentID, ev, newURI, "")

	if newURI != "" {
		doc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)
		doc.AgentURI = newURI
		bs.dirtyAgents[agentID] = true
		p.applyIdentityFromURI(bs, agentID, newURI)
	}
}

func (p *Processor) handleMetadataSet(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	metadataKey, _ := utils.GetStringArg(ev.Args, "metadataKey")
	metadataValue, _ := utils.GetStringArg(ev.Args, "metadataValue")

	p.appendIdentityHistory(bs, agentID, ev, "", "")

	if metadataKey == "" {
		return
	}

	doc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)

	// Initialize map if needed
	if doc.OnchainMetadata == nil {
		doc.OnchainMetadata = make(map[string]agent.OnchainMetadataValue)
	}

	decoded := decoder.DecodeMetadataValue(metadataKey, metadataValue)

	// Empty value means unset/delete the key
	if decoded.DetectedType == "empty" {
		delete(doc.OnchainMetadata, metadataKey)
		if strings.EqualFold(metadataKey, "tags") {
			doc.Tags = nil
		}
	} else {
		doc.OnchainMetadata[metadataKey] = agent.OnchainMetadataValue{
			RawHex:       decoded.RawHex,
			Decoded:      decoded.Decoded,
			DetectedType: decoded.DetectedType,
			Confidence:   decoded.Confidence,
		}
		// Denormalize onchainMetadata.tags into a top-level Tags field for cheap filtering.
		if strings.EqualFold(metadataKey, "tags") {
			doc.Tags = parseTagsList(decoded.Decoded)
		}
	}

	bs.dirtyAgents[agentID] = true
}

// parseTagsList parses a JSON-array string like ["a","b"] into a lowercase,
// deduped slice. Non-array or malformed values return nil.
func parseTagsList(decoded string) []string {
	s := strings.TrimSpace(decoded)
	if s == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	seen := make(map[string]struct{}, len(arr))
	out := make([]string, 0, len(arr))
	for _, t := range arr {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (p *Processor) appendIdentityHistory(bs *batchState, agentID string, ev eventrepo.DecodedEvent, uri, owner string) {
	var uriParsed map[string]any
	if strings.TrimSpace(uri) != "" {
		uriParsed = parseJSONObject(bs.uriMap[uri])
	}
	bs.pendingIdentity = append(bs.pendingIdentity, identityrepo.IdentityChange{
		AgentID:     agentID,
		ChainID:     bs.chainID,
		EventName:   ev.EventName,
		AgentURI:    uri,
		Owner:       owner,
		EventArgs:   cloneArgs(ev.Args),
		URIParsed:   uriParsed,
		BlockNumber: ev.BlockNumber,
		TxHash:      ev.TxHash,
		LogIndex:    ev.LogIndex,
		Timestamp:   ev.Timestamp,
	})
}

// applyIdentityFromURI reads pre-fetched URI content from uriMap and updates agent fields in-memory.
func (p *Processor) applyIdentityFromURI(bs *batchState, agentID, uri string) {
	content, ok := bs.uriMap[uri]
	if !ok {
		return
	}

	// First unmarshal to extract known fields
	var card agentCard
	if err := json.Unmarshal([]byte(content), &card); err != nil {
		log.Printf("processor: parse agent card JSON for %s: %v", agentID, err)
		return
	}

	// Extract extra/unknown fields for offchain_metadata
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(content), &rawMap); err == nil {
		// Remove known fixed fields
		knownFields := []string{
			"name", "description", "domains", "image", "imageURI",
			"services", "active", "supportedTrust", "x402Support",
		}
		card.Extra = make(map[string]any)
		for k, v := range rawMap {
			isKnown := false
			for _, known := range knownFields {
				if strings.EqualFold(k, known) {
					isKnown = true
					break
				}
			}
			if !isKnown && v != nil {
				card.Extra[k] = v
			}
		}
	}

	doc := bs.agentMap[agentID]
	if doc == nil {
		return
	}

	img := strings.TrimSpace(card.Image)
	if img == "" {
		img = strings.TrimSpace(card.ImageURI)
	}
	doc.AgentURI = uri
	doc.Name = card.Name
	doc.Image = img
	doc.Domains = card.Domains
	doc.Description = card.Description
	doc.Services = card.ParseServices()
	doc.Active = card.Active
	doc.SupportedTrust = card.SupportedTrust
	doc.X402Support = bool(card.X402Support)

	// Extract and normalize OASF capabilities
	oasfSkills, oasfDomains, hasOASF := ExtractOASFCapabilities(doc.Services)
	doc.OASFSkills = oasfSkills
	doc.OASFDomains = oasfDomains
	doc.HasOASF = hasOASF

	// Store extra fields in offchain_metadata
	if len(card.Extra) > 0 {
		doc.OffchainMetadata = card.Extra
	}

	bs.dirtyAgents[agentID] = true
}

// ── Reputation Events ──────────────────────────────────────────────────────────

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

	// ── Stage 1: Classify tag1/tag2 (always) ──────────────────────────────────
	cls := classifier.Classify(tag1, tag2)

	// ── Stage 2: Anomalous value check (but still persist history) ───────────
	isAnomalous := classifier.IsAnomalousValue(value, int(valueDecimals))
	if isAnomalous {
		log.Printf("processor: anomalous value detected (persisting, no scoring): chain=%d agent=%s value=%q", bs.chainID, agentID, value)
	}

	// ── Stage 3: Normalize value and compute weights ───────────────────────────
	vi := classifier.NormalizeValue(value, int(valueDecimals))
	if isAnomalous {
		// Don't trust the parsed value. Keep record, skip scoring.
		vi = 0.0
	}
	priceUSDC := readPriceFromContent(bs.uriMap[feedbackURI])
	wi := scoring.ComputeWi(priceUSDC, bs.formulaCfg.Alpha, bs.formulaCfg.Beta, bs.formulaCfg.K)

	// Reduce weight for low-confidence classifications (< 0.60).
	if cls.Confidence < 0.60 {
		wi *= 0.5
	}

	// Bonus for service_feedback quality signals:
	// +0.2 when feedbackURI exists and was fetched successfully
	// +0.2 when fetched payload includes proofOfPayment or attachments
	if cls.Category == classifier.CategoryService {
		wi += serviceFeedbackWiBonus(feedbackURI, bs.uriMap[feedbackURI])
	}

	// ── Stage 4: Build feedback record (always persisted, including spam/noise) ───
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
		Vi:             vi,
		Type:           "reputation_feedback",
		BlockNumber:    ev.BlockNumber,
		TxHash:         ev.TxHash,
		LogIndex:       ev.LogIndex,
		Timestamp:      ev.Timestamp,
		Classification: feedback.FeedbackClassification{
			Category:      string(cls.Category),
			Confidence:    cls.Confidence,
			Source:        cls.Source,
			NormalizedTag: cls.NormalizedTag,
		},
	}
	bs.pendingFeedbacks = append(bs.pendingFeedbacks, fbRecord)

	// Also store in fbMap so FeedbackRevoked in same batch can find it.
	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)
	bs.fbMap[fbID] = &fbRecord

	// ── Stage 5: Scoring dispatch — only service_feedback updates TrustRank ───
	if cls.Category == classifier.CategorySpam || cls.Category == classifier.CategoryNoise {
		// Persisted to history; do not affect TrustRank.
		return
	}
	if isAnomalous {
		// Persisted to history; do not affect TrustRank.
		return
	}
	if cls.Category != classifier.CategoryService {
		// config_feedback, app_specific, others: store the record but skip scoring.
		// Future: dispatch to UpdateReliabilityMetrics / RecordDomainSignal / LLM queue.
		return
	}

	agentDoc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)

	newAcc := scoring.ApplyTaskScore(agentDoc.AccumulatedScore, agentDoc.ScoreUpdateAt, wi, vi, ev.Timestamp, bs.formulaCfg)
	newTotalTasks := agentDoc.TotalTasks + 1

	var newPassed, newFailed, newConsecFails int64
	if vi >= 0.5 {
		newPassed = agentDoc.TotalPassed + 1
		newFailed = agentDoc.TotalFailed
		newConsecFails = 0
	} else {
		newPassed = agentDoc.TotalPassed
		newFailed = agentDoc.TotalFailed + 1
		newConsecFails = agentDoc.ConsecutiveFails + 1
	}

	agentDoc.AccumulatedScore = newAcc
	agentDoc.ScoreUpdateAt = ev.Timestamp
	agentDoc.ConsecutiveFails = newConsecFails
	agentDoc.TotalTasks = newTotalTasks
	agentDoc.TotalPassed = newPassed
	agentDoc.TotalFailed = newFailed
	bs.dirtyAgents[agentID] = true

	displayScore := scoring.ComputeCurrentScore(newAcc, ev.Timestamp, newConsecFails, ev.Timestamp, bs.formulaCfg)
	bs.pendingSnapshots = append(bs.pendingSnapshots, struct {
		ChainID  int64
		AgentID  string
		Snapshot score.ScoreSnapshotItem
	}{
		ChainID: bs.chainID,
		AgentID: agentID,
		Snapshot: score.ScoreSnapshotItem{
			AgentScore: displayScore,
			Type:       "event",
			TxHash:     ev.TxHash,
			EventScore: wi * vi,
			Timestamp:  ev.Timestamp,
		},
	})
}

func (p *Processor) handleFeedbackRevoked(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	feedbackIndex := utils.GetUint64Arg(ev.Args, "feedbackIndex")
	clientAddress, ok := utils.GetStringArg(ev.Args, "clientAddress")
	if !ok {
		log.Printf("processor: FeedbackRevoked missing clientAddress: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}
	fbID := feedback.FeedbackDocumentID(bs.chainID, agentID, clientAddress, feedbackIndex)

	bs.pendingFBUpdates = append(bs.pendingFBUpdates, feedback.FeedbackUpdate{
		ID: fbID,
		Update: bson.M{
			"$set": bson.M{
				"revokeTxHash": ev.TxHash,
			},
		},
	})

	fb := bs.fbMap[fbID]
	if fb == nil {
		log.Printf("processor: feedback revoked but record not found: chain=%d agent=%s idx=%d", bs.chainID, agentID, feedbackIndex)
		return
	}

	agentDoc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)

	newAcc := scoring.RevertFeedbackScore(agentDoc.AccumulatedScore, agentDoc.ScoreUpdateAt, fb.Wi, fb.Vi, fb.Timestamp, ev.Timestamp, bs.formulaCfg)
	agentDoc.AccumulatedScore = newAcc
	agentDoc.ScoreUpdateAt = ev.Timestamp
	bs.dirtyAgents[agentID] = true

	displayScore := scoring.ComputeCurrentScore(newAcc, ev.Timestamp, agentDoc.ConsecutiveFails, ev.Timestamp, bs.formulaCfg)
	bs.pendingSnapshots = append(bs.pendingSnapshots, struct {
		ChainID  int64
		AgentID  string
		Snapshot score.ScoreSnapshotItem
	}{
		ChainID: bs.chainID,
		AgentID: agentID,
		Snapshot: score.ScoreSnapshotItem{
			AgentScore: displayScore,
			Type:       "event",
			TxHash:     ev.TxHash,
			EventScore: -(fb.Wi * fb.Vi), // negative score contribution (revocation)
			Timestamp:  ev.Timestamp,
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

// ── Phase 3: Flush ─────────────────────────────────────────────────────────────

func (p *Processor) flush(ctx context.Context, bs *batchState) error {
	dirtyDocs := make([]*agent.AgentDocument, 0, len(bs.dirtyAgents))
	for id := range bs.dirtyAgents {
		if doc := bs.agentMap[id]; doc != nil {
			dirtyDocs = append(dirtyDocs, doc)
		}
	}
	if err := p.agentRepo.BulkUpsert(ctx, dirtyDocs); err != nil {
		return fmt.Errorf("flush agents: %w", err)
	}

	if err := p.identityRepo.BulkInsert(ctx, bs.pendingIdentity); err != nil {
		return fmt.Errorf("flush identity history: %w", err)
	}

	if err := p.feedbackRepo.BulkUpsert(ctx, bs.pendingFeedbacks); err != nil {
		return fmt.Errorf("flush feedback: %w", err)
	}

	if err := p.feedbackRepo.BulkUpdate(ctx, bs.pendingFBUpdates); err != nil {
		return fmt.Errorf("flush feedback updates: %w", err)
	}

	if err := p.scoreRepo.BulkAppendSnapshots(ctx, bs.pendingSnapshots); err != nil {
		return fmt.Errorf("flush score snapshots: %w", err)
	}

	return nil
}

// ── Helpers ────────────────────────────────────────────────────────────────────

// getOrCreateAgent returns the agent from the map, creating a stub if missing.
func (p *Processor) getOrCreateAgent(bs *batchState, agentID string, timestamp int64) *agent.AgentDocument {
	doc := bs.agentMap[agentID]
	if doc != nil {
		return doc
	}
	doc = &agent.AgentDocument{
		AgentID:          agentID,
		ChainID:          bs.chainID,
		AccumulatedScore: 0,
		ScoreUpdateAt:    timestamp,
	}
	bs.agentMap[agentID] = doc
	bs.dirtyAgents[agentID] = true
	return doc
}

// readPriceFromContent extracts priceUSDC from JSON content. Returns 0 on any failure.
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

func serviceFeedbackWiBonus(feedbackURI, content string) float64 {
	bonus := 0.0
	if strings.TrimSpace(feedbackURI) == "" {
		return bonus
	}

	// URI exists and content was fetched into prefetch cache.
	if strings.TrimSpace(content) != "" {
		bonus += 0.2
	}

	// Additional bonus if payload includes proofOfPayment or attachments.
	if hasProofOfPaymentOrAttachments(content) {
		bonus += 0.2
	}

	return bonus
}

func hasProofOfPaymentOrAttachments(content string) bool {
	if strings.TrimSpace(content) == "" {
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return false
	}

	var hasProof bool
	var hasAttachments bool
	for k, v := range payload {
		lowerKey := strings.ToLower(strings.TrimSpace(k))
		switch lowerKey {
		case "proofofpayment":
			hasProof = hasNonEmptyValue(v)
		case "attachments":
			hasAttachments = hasNonEmptyValue(v)
		}
		if hasProof || hasAttachments {
			return true
		}
	}
	return false
}

func hasNonEmptyValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(t) != ""
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

func parseJSONObject(content string) map[string]any {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		return nil
	}
	return out
}

func cloneArgs(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ EventProcessor = (*Processor)(nil)
