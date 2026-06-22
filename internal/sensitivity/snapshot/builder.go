package snapshot

// builder.go — Build SnapshotData from source MongoDB collections.
// Orchestrates: load → filter → denormalize → compute baseline.

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/feedback"
)

// SourceCollections aggregates the source-database collection handles
// needed by the builder. Caller wires these from cfg.AnalyzedDatabase.
type SourceCollections struct {
	Feedbacks       *mongodrv.Collection // feedback_history
	Agents          *mongodrv.Collection // agents
	AgentScoreStats *mongodrv.Collection // agent_score_stats
	Wallets         *mongodrv.Collection // wallets — owner external on-chain score (Cụm D E-component)
}

// BuildOptions carries everything BuildFromSource needs to know.
type BuildOptions struct {
	ChainID    int64
	Filter     FilterConfig
	Now        time.Time // injected for deterministic snapshot IDs (use time.Now() in prod)
	Composite  scoring.CompositeWeights
	Compliance scoring.ComplianceWeights
}

// agentMini is the minimal agent info denormalizeAgents needs.
type agentMini struct {
	Owner       string
	AgentWallet string
	Verified    bool
}

// BuildFromSource reads filtered feedbacks + their agents from the source DB and
// returns SnapshotData ready to be written. It does NOT touch the snapshot DB.
func BuildFromSource(ctx context.Context, src SourceCollections, opts BuildOptions) (SnapshotData, error) {
	fbs, err := loadFilteredFeedbacks(ctx, src.Feedbacks, opts.ChainID, opts.Filter)
	if err != nil {
		return SnapshotData{}, fmt.Errorf("load feedbacks: %w", err)
	}

	agentIDs := uniqueAgentIDs(fbs)
	agentDocs, err := loadAgents(ctx, src.Agents, opts.ChainID, agentIDs)
	if err != nil {
		return SnapshotData{}, fmt.Errorf("load agents: %w", err)
	}

	agents := denormalizeAgents(fbs, agentDocs, opts.ChainID)
	if opts.Filter.MinFeedbacks > 1 {
		agents = filterByMinFeedbacks(agents, opts.Filter.MinFeedbacks)
		keepIDs := agentIDSet(agents)
		fbs = filterFeedbacksByAgent(fbs, keepIDs)
	}

	feedbacks := projectFeedbacks(fbs)
	edges := buildEdges(fbs, agentDocs)
	baseline, err := computeBaseline(ctx, src, agents, opts)
	if err != nil {
		return SnapshotData{}, fmt.Errorf("compute baseline: %w", err)
	}

	return SnapshotData{
		Agents:    agents,
		Feedbacks: feedbacks,
		Edges:     edges,
		Baseline:  baseline,
		Filter:    opts.Filter,
	}, nil
}

func loadFilteredFeedbacks(ctx context.Context, coll *mongodrv.Collection, chainID int64, cfg FilterConfig) ([]feedback.FeedbackRecord, error) {
	query := bson.M{"chainId": chainID}
	if !cfg.IncludeSpam {
		// Filter on the EFFECTIVE top-level category (what actually scored), falling
		// back to the rule category for rows persisted before the top-level field.
		query["$or"] = []bson.M{
			{"category": cfg.Category},
			{"category": bson.M{"$exists": false}, "classification.rule.category": cfg.Category},
		}
	}
	if !cfg.IncludeRevoked {
		query["isRevoked"] = bson.M{"$ne": true}
	}
	if !cfg.IncludeSelf {
		query["isSelfFeedback"] = bson.M{"$ne": true}
	}
	cur, err := coll.Find(ctx, query)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []feedback.FeedbackRecord
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func uniqueAgentIDs(fbs []feedback.FeedbackRecord) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(fbs))
	for _, fb := range fbs {
		if _, ok := seen[fb.AgentID]; ok {
			continue
		}
		seen[fb.AgentID] = struct{}{}
		out = append(out, fb.AgentID)
	}
	return out
}

func loadAgents(ctx context.Context, coll *mongodrv.Collection, chainID int64, agentIDs []string) (map[string]agentMini, error) {
	if len(agentIDs) == 0 {
		return map[string]agentMini{}, nil
	}
	cur, err := coll.Find(ctx, bson.M{
		"chainId": chainID,
		"agentId": bson.M{"$in": agentIDs},
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var raw []struct {
		AgentID     string `bson:"agentId"`
		Owner       string `bson:"owner"`
		AgentWallet string `bson:"agentWallet"`
		Verified    bool   `bson:"verified"`
	}
	if err := cur.All(ctx, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]agentMini, len(raw))
	for _, r := range raw {
		out[r.AgentID] = agentMini{Owner: r.Owner, AgentWallet: r.AgentWallet, Verified: r.Verified}
	}
	return out, nil
}

func denormalizeAgents(fbs []feedback.FeedbackRecord, agentDocs map[string]agentMini, chainID int64) []AgentSnapshot {
	byID := map[string]*AgentSnapshot{}
	for _, fb := range fbs {
		s, ok := byID[fb.AgentID]
		if !ok {
			meta := agentDocs[fb.AgentID]
			s = &AgentSnapshot{
				ID:          fb.AgentID,
				ChainID:     chainID,
				Owner:       meta.Owner,
				AgentWallet: meta.AgentWallet,
				Verified:    meta.Verified,
			}
			byID[fb.AgentID] = s
		}
		s.FeedbackIDs = append(s.FeedbackIDs, fb.ID)
		s.FeedbackCount++
	}
	out := make([]AgentSnapshot, 0, len(byID))
	for _, s := range byID {
		out = append(out, *s)
	}
	return out
}

// filterByMinFeedbacks performs an in-place filter; caller MUST NOT retain a
// reference to the old slice header (the backing array is reused).
func filterByMinFeedbacks(agents []AgentSnapshot, min int) []AgentSnapshot {
	out := agents[:0]
	for _, a := range agents {
		if a.FeedbackCount >= min {
			out = append(out, a)
		}
	}
	return out
}

func agentIDSet(agents []AgentSnapshot) map[string]struct{} {
	out := make(map[string]struct{}, len(agents))
	for _, a := range agents {
		out[a.ID] = struct{}{}
	}
	return out
}

// filterFeedbacksByAgent performs an in-place filter; caller MUST NOT retain a
// reference to the old slice header (the backing array is reused).
func filterFeedbacksByAgent(fbs []feedback.FeedbackRecord, keep map[string]struct{}) []feedback.FeedbackRecord {
	out := fbs[:0]
	for _, fb := range fbs {
		if _, ok := keep[fb.AgentID]; ok {
			out = append(out, fb)
		}
	}
	return out
}

func projectFeedbacks(fbs []feedback.FeedbackRecord) []FeedbackSnapshot {
	out := make([]FeedbackSnapshot, 0, len(fbs))
	for _, fb := range fbs {
		// Quality signals for Cụm C (reasoningLen, attachmentCount, breakdown, proof).
		rl, ac, bk, pp := ExtractQualitySignals(fb.FeedbackParsed)
		out = append(out, FeedbackSnapshot{
			ID:                   fb.ID,
			AgentID:              fb.AgentID,
			ChainID:              fb.ChainID,
			ClientAddress:        fb.ClientAddress,
			FeedbackIndex:        fb.FeedbackIndex,
			Wi:                   fb.Wi,
			ValueScale:           fb.ValueScale,
			ValueNormalized:      normalizeValue(fb),
			ClassifierConfidence: classifierConfidence(fb),
			Timestamp:            fb.Timestamp,
			IsRevoked:            fb.IsRevoked,
			IsSelfFeedback:       fb.IsSelfFeedback,
			Category:             effectiveCategory(fb),
			ReasoningLen:         rl,
			AttachmentCount:      ac,
			HasRatingBreakdown:   bk,
			HasProofOfPayment:    pp,
		})
	}
	return out
}

// effectiveCategory returns the runtime category that actually drove scoring — the
// top-level field (LLM fallback overwrites it), falling back to the rule verdict for
// rows persisted before the top-level field existed.
func effectiveCategory(fb feedback.FeedbackRecord) string {
	if fb.Category != "" {
		return fb.Category
	}
	return fb.Classification.Rule.Category
}

// normalizeValue replays production value normalization (classifier.RawValueToReal
// → NormalizeValueWithScale) to recover vi ∈ [-1,1] for the snapshot. Falls back to
// AssignTier when the stored valueScale is empty.
func normalizeValue(fb feedback.FeedbackRecord) float64 {
	real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals))
	if !ok {
		return 0.0
	}
	scale := fb.ValueScale
	if scale == "" {
		scale = classifier.AssignTier(real)
	}
	return classifier.NormalizeValueWithScale(real, scale)
}

// classifierConfidence returns the LLM fallback confidence when the fallback ran,
// otherwise 1.0 (the rule engine was certain). Matches how downstream scoring
// treats rule-only classifications.
func classifierConfidence(fb feedback.FeedbackRecord) float64 {
	if fb.Classification.Fallback != nil && fb.Classification.Fallback.Confidence > 0 {
		return fb.Classification.Fallback.Confidence
	}
	return 1.0
}

// buildEdges groups feedbacks by (client → agent owner). Retained for graph-edge stats; no cluster sweeps edge weights directly.
func buildEdges(fbs []feedback.FeedbackRecord, agentDocs map[string]agentMini) []GraphEdge {
	type key struct{ from, to string }
	agg := map[key]*GraphEdge{}
	for _, fb := range fbs {
		meta := agentDocs[fb.AgentID]
		to := meta.Owner
		if to == "" {
			continue
		}
		from := fb.ClientAddress
		if from == "" {
			continue
		}
		k := key{from: from, to: to}
		e, ok := agg[k]
		if !ok {
			e = &GraphEdge{
				ID:         from + ":" + to,
				FromWallet: from,
				ToWallet:   to,
			}
			agg[k] = e
		}
		e.FeedbackCount++
	}
	out := make([]GraphEdge, 0, len(agg))
	for _, e := range agg {
		out = append(out, *e)
	}
	return out
}

// computeBaseline pulls existing agent_score_stats rows for the given agents and
// projects them into the v2 BaselineScore (5-component composite inputs + the
// weightMass / owner external score that Cụm B and Cụm D need). Agents without a
// score row get zeros. Returns (nil, nil) when the score-stats handle is nil or
// there are no agents — an intentional caller signal, not a failure.
func computeBaseline(ctx context.Context, src SourceCollections, agents []AgentSnapshot, opts BuildOptions) ([]BaselineScore, error) {
	if src.AgentScoreStats == nil || len(agents) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(agents))
	ownerByAgent := make(map[string]string, len(agents))
	for _, a := range agents {
		ids = append(ids, a.ID)
		ownerByAgent[a.ID] = a.Owner
	}
	cur, err := src.AgentScoreStats.Find(ctx, bson.M{
		"chainId": opts.ChainID,
		"agentId": bson.M{"$in": ids},
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var rows []struct {
		AgentID          string  `bson:"agentId"`
		ReputationScore  float64 `bson:"reputationScore"`
		AdoptionScore    float64 `bson:"adoptionScore"`
		ServicesScore    float64 `bson:"servicesScore"`
		PublisherScore   float64 `bson:"publisherScore"`
		PublisherPresent bool    `bson:"publisherPresent"`
		ComplianceScore  float64 `bson:"complianceScore"`
		CompositeScore   float64 `bson:"compositeScore"`
		WeightMass       float64 `bson:"weightMass"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, err
	}

	externalByOwner := loadOwnerExternalScores(ctx, src.Wallets, opts.ChainID, ownerByAgent)

	out := make([]BaselineScore, 0, len(rows))
	for _, r := range rows {
		out = append(out, BaselineScore{
			AgentID:          r.AgentID,
			ChainID:          opts.ChainID,
			ReputationScore:  r.ReputationScore,
			AdoptionScore:    r.AdoptionScore,
			ServicesScore:    r.ServicesScore,
			PublisherScore:   r.PublisherScore,
			PublisherPresent: r.PublisherPresent,
			ComplianceScore:  r.ComplianceScore,
			CompositeScore:   r.CompositeScore,
			WeightMass:       r.WeightMass,
			ExternalScore:    externalByOwner[ownerByAgent[r.AgentID]],
		})
	}
	return out, nil
}

// loadOwnerExternalScores returns owner-address → external on-chain score [0,100]
// from the wallets collection. Owners without an enrichment row (or a nil handle)
// simply map to 0 — Cụm D treats external=0 as signal absent (ePresent=false).
func loadOwnerExternalScores(ctx context.Context, coll *mongodrv.Collection, chainID int64, ownerByAgent map[string]string) map[string]float64 {
	out := map[string]float64{}
	if coll == nil {
		return out
	}
	ownerSet := map[string]struct{}{}
	owners := make([]string, 0, len(ownerByAgent))
	for _, owner := range ownerByAgent {
		if owner == "" {
			continue
		}
		if _, ok := ownerSet[owner]; ok {
			continue
		}
		ownerSet[owner] = struct{}{}
		owners = append(owners, owner)
	}
	if len(owners) == 0 {
		return out
	}
	cur, err := coll.Find(ctx, bson.M{"_id": bson.M{"$in": owners}})
	if err != nil {
		return out
	}
	defer cur.Close(ctx)
	var rows []struct {
		ID       string `bson:"_id"`
		External struct {
			Score float64 `bson:"score"`
		} `bson:"external"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return out
	}
	for _, r := range rows {
		out[r.ID] = r.External.Score
	}
	return out
}
