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
	"time"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/agent"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/score"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
	"erc-8004-benchmarking-be/internal/utils"
)

// NewProcessor constructs a Processor with all required dependencies.
// uriPublisher may be nil; when set, service endpoint URIs discovered during
// identity event processing are published to the service_uri queue.
// tagStatsRepo / tagCorrsRepo may be nil to disable dynamic scale detection.
func NewProcessor(
	agentRepo *agent.Repository,
	identityRepo *identityrepo.Repository,
	feedbackRepo *feedback.Repository,
	scoreRepo *score.Repository,
	offchainRepo *offchain.Repository,
	formulaCfg scoring.FormulaConfig,
	uriPublisher URIPublisher,
	tagStatsRepo *tagstats.StatsRepository,
	tagCorrsRepo *tagstats.CorrectionRepository,
	minSamples int,
) *Processor {
	return &Processor{
		agentRepo:    agentRepo,
		identityRepo: identityRepo,
		feedbackRepo: feedbackRepo,
		scoreRepo:    scoreRepo,
		offchainRepo: offchainRepo,
		formulaCfg:   formulaCfg,
		uriPublisher: uriPublisher,
		tagStatsRepo: tagStatsRepo,
		tagCorrsRepo: tagCorrsRepo,
		minSamples:   minSamples,
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

// indexURIToContent maps requested URI -> cached JSON for rows with status=5 (StatusFetchedJSON).
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
		if doc.Status != offchain.StatusFetchedJSON {
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

	// Flush tag tier votes and detect scale changes (best-effort; errors logged, not fatal).
	if p.tagStatsRepo != nil && len(bs.pendingTierUpdates) > 0 {
		if err := p.flushTierStats(ctx, bs); err != nil {
			log.Printf("processor: flush tier stats: %v", err)
		}
	}

	// Publish service endpoint URIs for async fetching (best-effort; errors are logged, not fatal).
	if p.uriPublisher != nil {
		for _, msg := range bs.pendingServiceURIs {
			queueName := mq.ServiceURIQueueName(msg.ChainID)
			if err := p.uriPublisher.Publish(ctx, queueName, msg); err != nil {
				log.Printf("processor: publish service uri chain=%d agent=%s service=%s: %v",
					msg.ChainID, msg.AgentID, msg.ServiceName, err)
			}
		}
	}

	return nil
}

// flushTierStats increments tier counts in tag_value_stats, then checks whether
// any tag's detected scale has changed and enqueues a correction record if so.
func (p *Processor) flushTierStats(ctx context.Context, bs *batchState) error {
	if err := p.tagStatsRepo.BulkIncrementTiers(ctx, bs.pendingTierUpdates); err != nil {
		return fmt.Errorf("bulk increment tiers: %w", err)
	}

	// Collect unique tag1s that received votes in this batch.
	tag1Set := make(map[string]bool, len(bs.pendingTierUpdates))
	for _, u := range bs.pendingTierUpdates {
		if !u.IsEmpty {
			tag1Set[u.Tag1] = true
		}
	}
	if len(tag1Set) == 0 {
		return nil
	}
	tag1s := make([]string, 0, len(tag1Set))
	for t := range tag1Set {
		tag1s = append(tag1s, t)
	}

	// Re-fetch updated stats to run scale detection.
	statsDocs, err := p.tagStatsRepo.GetByTag1s(ctx, tag1s)
	if err != nil {
		return fmt.Errorf("re-fetch stats: %w", err)
	}

	nowUnix := time.Now().Unix()
	for _, doc := range statsDocs {
		if doc.Count < int64(p.minSamples) {
			continue
		}
		newScale := argmaxTier(doc.TierCounts)
		if newScale == doc.DetectedScale {
			continue
		}

		// Scale changed: persist the new scale and enqueue a correction.
		if err := p.tagStatsRepo.SetDetectedScale(ctx, doc.Tag1, newScale, nowUnix); err != nil {
			log.Printf("processor: set detected scale %s: %v", doc.Tag1, err)
			continue
		}
		// Invalidate cache so next batch picks up the new scale.
		p.tagScaleCache.Delete(doc.Tag1)

		if p.tagCorrsRepo == nil {
			continue
		}
		corrID := fmt.Sprintf("%s:v%d", doc.Tag1, doc.ScaleVersion+1)
		corr := tagstats.ScaleChangeCorrection{
			ID:         corrID,
			Tag1:       doc.Tag1,
			OldScale:   doc.DetectedScale,
			NewScale:   newScale,
			DetectedAt: nowUnix,
			Status:     tagstats.CorrectionPending,
		}
		if err := p.tagCorrsRepo.UpsertCorrection(ctx, corr); err != nil {
			log.Printf("processor: upsert correction %s: %v", corrID, err)
		}
	}
	return nil
}

// argmaxTier returns the tier name with the highest count.
// Tie-breaking order: binary > star5 > star10 > pct100 > unbounded.
func argmaxTier(tc tagstats.TierCounts) string {
	type entry struct {
		name  string
		count int64
		rank  int // lower = preferred on tie
	}
	tiers := []entry{
		{"binary", tc.Binary, 0},
		{"star5", tc.Star5, 1},
		{"star10", tc.Star10, 2},
		{"pct100", tc.Pct100, 3},
		{"unbounded", tc.Unbounded, 4},
	}
	best := tiers[0]
	for _, t := range tiers[1:] {
		if t.count > best.count || (t.count == best.count && t.rank < best.rank) {
			best = t
		}
	}
	if best.count == 0 {
		return "" // no votes yet
	}
	return best.name
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
