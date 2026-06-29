package wallet

// repo.go — WalletRepository for the "wallets" MongoDB collection.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
	agentrep "erc-8004-benchmarking-be/internal/repository/agent"
)

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[WalletDocument](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the wallets collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "address", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("ux_chain_address"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "trustScore", Value: -1},
			},
			Options: options.Index().SetName("idx_chain_trust_pp_desc"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "kind", Value: 1},
				{Key: "trustScore", Value: -1},
			},
			Options: options.Index().SetName("idx_chain_kind_trust_pp"),
		},
		{
			Keys: bson.D{
				{Key: "ownedAgentIds", Value: 1},
			},
			Options: options.Index().SetName("idx_owned_agents"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "junkRatio", Value: -1},
			},
			Options: options.Index().SetName("idx_chain_junk_ratio_desc"),
		},
	})
	return err
}

// GetByAddress fetches a wallet by chain + address. Returns mongo.ErrNoDocuments if not found.
func (r *Repository) GetByAddress(ctx context.Context, chainID int64, address string) (*WalletDocument, error) {
	id := WalletDocumentID(chainID, address)
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err != nil {
		return nil, fmt.Errorf("wallet repo: get by address (%d, %s): %w", chainID, address, err)
	}
	return &doc, nil
}

// DeltaInput is the payload for ApplyTrustDelta.
type DeltaInput struct {
	ChainID  int64
	Address  string
	NewTrust float64 // already clipped to [0, 100] by caller
	IsValid  bool    // true for valid feedback; false for junk/gate-failed
}

// UpsertCold ensures a wallet record exists for (chainID, address). If absent, inserts
// with trustScore = t0. If already present, only bumps updatedAt. Returns the post-upsert
// doc and wasNew (true if a new document was inserted).
func (r *Repository) UpsertCold(ctx context.Context, chainID int64, address string, t0 float64) (*WalletDocument, bool, error) {
	id := WalletDocumentID(chainID, address)
	now := time.Now().Unix()
	update := buildUpsertColdUpdate(chainID, address, t0, now)
	res, err := r.UpdateOne(ctx, bson.M{"_id": id}, update, options.Update().SetUpsert(true))
	if err != nil {
		return nil, false, err
	}
	doc, err := r.GetByAddress(ctx, chainID, address)
	if err != nil {
		return nil, false, err
	}
	return doc, res.UpsertedCount > 0, nil
}

// LinkOwnerAgent ensures an owner wallet exists and associates agentID with it.
// Safe to call repeatedly; uses $addToSet for ownedAgentIds.
func (r *Repository) LinkOwnerAgent(ctx context.Context, chainID int64, ownerAddr, agentID string, t0 float64) error {
	if strings.TrimSpace(ownerAddr) == "" || strings.TrimSpace(agentID) == "" {
		return nil
	}
	if _, _, err := r.UpsertCold(ctx, chainID, ownerAddr, t0); err != nil {
		return fmt.Errorf("wallet repo: link owner upsert (%d, %s): %w", chainID, ownerAddr, err)
	}
	id := WalletDocumentID(chainID, ownerAddr)
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$set": bson.M{
			"kind":      string(WalletKindOwner),
			"updatedAt": time.Now().Unix(),
		},
		"$addToSet": bson.M{"ownedAgentIds": agentID},
	})
	if err != nil {
		return fmt.Errorf("wallet repo: link owner agent (%d, %s, %s): %w", chainID, ownerAddr, agentID, err)
	}
	return nil
}

// UnlinkOwnerAgent removes agentID from the owner's ownedAgentIds list.
func (r *Repository) UnlinkOwnerAgent(ctx context.Context, chainID int64, ownerAddr, agentID string) error {
	if strings.TrimSpace(ownerAddr) == "" || strings.TrimSpace(agentID) == "" {
		return nil
	}
	id := WalletDocumentID(chainID, ownerAddr)
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$pull": bson.M{"ownedAgentIds": agentID},
		"$set":  bson.M{"updatedAt": time.Now().Unix()},
	})
	if err != nil {
		return fmt.Errorf("wallet repo: unlink owner agent (%d, %s, %s): %w", chainID, ownerAddr, agentID, err)
	}
	return nil
}

// ReconcileOwnership ensures agentID appears in exactly one wallet's ownedAgentIds
// on the given chain — the wallet for ownerAddr. It pulls agentID from every
// other wallet doc on the same chain that still claims it, and addToSet's it on
// the canonical owner doc. Idempotent.
// Caller must pass an already-normalized ownerAddr (lowercase, trimmed).
// Returns wasNew (true if the owner wallet document was just created by the
// internal UpsertCold call).
func (r *Repository) ReconcileOwnership(ctx context.Context, chainID int64, agentID, ownerAddr string, t0 float64) (bool, error) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(ownerAddr) == "" {
		return false, nil
	}
	_, wasNew, err := r.UpsertCold(ctx, chainID, ownerAddr, t0)
	if err != nil {
		return false, fmt.Errorf("wallet repo: reconcile upsert (%d, %s): %w", chainID, ownerAddr, err)
	}
	now := time.Now().Unix()
	ownerID := WalletDocumentID(chainID, ownerAddr)
	if _, err := r.UpdateMany(ctx, bson.M{
		"chainId":       chainID,
		"ownedAgentIds": agentID,
		"_id":           bson.M{"$ne": ownerID},
	}, bson.M{
		"$pull": bson.M{"ownedAgentIds": agentID},
		"$set":  bson.M{"updatedAt": now},
	}); err != nil {
		return false, fmt.Errorf("wallet repo: reconcile pull stale (%d, %s): %w", chainID, agentID, err)
	}
	if _, err := r.UpdateOne(ctx, bson.M{"_id": ownerID}, bson.M{
		"$addToSet": bson.M{"ownedAgentIds": agentID},
		"$set": bson.M{
			"kind":      string(WalletKindOwner),
			"updatedAt": now,
		},
	}); err != nil {
		return false, fmt.Errorf("wallet repo: reconcile add (%d, %s, %s): %w", chainID, ownerAddr, agentID, err)
	}
	return wasNew, nil
}

// ApplyTrustDelta atomically writes the new trustScore + bumps feedback counters.
// Caller must clip NewTrust before calling (use clipTrustScore).
// Caller must hold a per-wallet mutex before calling to avoid concurrent writes.
func (r *Repository) ApplyTrustDelta(ctx context.Context, in DeltaInput) error {
	id := WalletDocumentID(in.ChainID, in.Address)
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$set": bson.M{
			"trustScore": clipTrustScore(in.NewTrust),
			"updatedAt":  time.Now().Unix(),
		},
		"$inc": computeCounterIncrements(in.IsValid),
	})
	if err != nil {
		return fmt.Errorf("wallet repo: apply trust delta (%d, %s): %w", in.ChainID, in.Address, err)
	}
	return nil
}

// IncrementFeedbackCounters bumps feedbackTotalCount and the valid/junk counter
// for a wallet WITHOUT touching trustScore (owned by the propagation pass).
func (r *Repository) IncrementFeedbackCounters(ctx context.Context, chainID int64, address string, isValid bool) error {
	id := WalletDocumentID(chainID, address)
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$set": bson.M{"updatedAt": time.Now().Unix()},
		"$inc": computeCounterIncrements(isValid),
	})
	if err != nil {
		return fmt.Errorf("wallet repo: increment counters (%d, %s): %w", chainID, address, err)
	}
	return nil
}

// WalletRankingSummary holds wallet fields needed for the wallet ranking list.
type WalletRankingSummary struct {
	TrustScore         *float64
	FeedbackTotalCount int64
}

// pickProfileWalletDoc mirrors wallet.Profile selection: prefer trustRated docs,
// then highest trustScore across chain-specific wallet records.
func pickProfileWalletDoc(docs []WalletDocument) *WalletDocument {
	if len(docs) == 0 {
		return nil
	}
	best := &docs[0]
	for i := 1; i < len(docs); i++ {
		d := &docs[i]
		if d.TrustRated && !best.TrustRated {
			best = d
			continue
		}
		if d.TrustRated == best.TrustRated && d.TrustScore > best.TrustScore {
			best = d
		}
	}
	return best
}

// FindRankingSummariesByAddresses returns profile-style wallet summaries for many
// addresses in one query. Missing addresses are omitted from the result map.
func (r *Repository) FindRankingSummariesByAddresses(ctx context.Context, addresses []string) (map[string]WalletRankingSummary, error) {
	norm := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		n := normalizeAddress(addr)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		norm = append(norm, n)
	}
	if len(norm) == 0 {
		return map[string]WalletRankingSummary{}, nil
	}

	docs, err := r.Find(ctx, bson.M{"address": bson.M{"$in": norm}},
		options.Find().SetProjection(bson.M{
			"address": 1, "trustScore": 1, "trustRated": 1, "feedbackTotalCount": 1,
		}))
	if err != nil {
		return nil, fmt.Errorf("wallet repo: find ranking summaries: %w", err)
	}

	byAddr := make(map[string][]WalletDocument, len(norm))
	for _, doc := range docs {
		byAddr[doc.Address] = append(byAddr[doc.Address], doc)
	}

	out := make(map[string]WalletRankingSummary, len(byAddr))
	for addr, group := range byAddr {
		doc := pickProfileWalletDoc(group)
		if doc == nil {
			continue
		}
		var trust *float64
		if doc.TrustRated {
			v := doc.TrustScore
			trust = &v
		}
		out[addr] = WalletRankingSummary{
			TrustScore:         trust,
			FeedbackTotalCount: doc.FeedbackTotalCount,
		}
	}
	return out, nil
}

// FindBestTrustByAddresses returns the highest trustScore per address across chains.
// Only rated wallets (trustRated=true) are included.
func (r *Repository) FindBestTrustByAddresses(ctx context.Context, addresses []string) (map[string]float64, error) {
	norm := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		n := normalizeAddress(addr)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		norm = append(norm, n)
	}
	if len(norm) == 0 {
		return map[string]float64{}, nil
	}

	docs, err := r.Find(ctx, bson.M{
		"address":    bson.M{"$in": norm},
		"trustRated": true,
	}, options.Find().SetProjection(bson.M{"address": 1, "trustScore": 1}))
	if err != nil {
		return nil, fmt.Errorf("wallet repo: find best trust by addresses: %w", err)
	}

	best := make(map[string]float64, len(norm))
	for _, doc := range docs {
		if prev, ok := best[doc.Address]; !ok || doc.TrustScore > prev {
			best[doc.Address] = doc.TrustScore
		}
	}
	return best, nil
}

// ENSInfo is the resolved ENS primary name + avatar for an address.
type ENSInfo struct {
	ENS       string
	ENSAvatar string
}

// FindENSByAddresses returns the resolved ENS primary name + avatar for the
// given addresses, across all chains. Addresses with no resolved ENS name (or
// not found at all) are omitted from the result map.
func (r *Repository) FindENSByAddresses(ctx context.Context, addresses []string) (map[string]ENSInfo, error) {
	norm := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		n := normalizeAddress(addr)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		norm = append(norm, n)
	}
	if len(norm) == 0 {
		return map[string]ENSInfo{}, nil
	}

	docs, err := r.Find(ctx, bson.M{"address": bson.M{"$in": norm}},
		options.Find().SetProjection(bson.M{"address": 1, "external.ens": 1, "external.ensAvatar": 1}))
	if err != nil {
		return nil, fmt.Errorf("wallet repo: find ens by addresses: %w", err)
	}

	out := make(map[string]ENSInfo, len(norm))
	for _, doc := range docs {
		if doc.External.ENS == "" {
			continue
		}
		if _, ok := out[doc.Address]; ok {
			continue
		}
		out[doc.Address] = ENSInfo{ENS: doc.External.ENS, ENSAvatar: doc.External.ENSAvatar}
	}
	return out, nil
}

// FindAllByAddress returns wallet documents for a given address across all chains,
// sorted by trustScore descending. At most 10 results.
func (r *Repository) FindAllByAddress(ctx context.Context, address string) ([]WalletDocument, error) {
	norm := normalizeAddress(address)
	if norm == "" {
		return nil, nil
	}
	docs, err := r.Find(ctx, bson.M{"address": norm},
		options.Find().
			SetSort(bson.D{{Key: "trustRated", Value: -1}, {Key: "trustScore", Value: -1}}).
			SetLimit(10),
	)
	if err != nil {
		return nil, fmt.Errorf("wallet repo: find all by address (%s): %w", address, err)
	}
	return docs, nil
}

// ScanAll returns all wallet documents for a chain (projection: fields for graph construction).
// Pass chainID=0 for all chains.
func (r *Repository) ScanAll(ctx context.Context, chainID int64) ([]WalletDocument, error) {
	filter := bson.M{}
	if chainID > 0 {
		filter["chainId"] = chainID
	}
	return r.Find(ctx, filter, options.Find().
		SetProjection(bson.M{
			"_id": 1, "address": 1, "chainId": 1,
			"feedbackValidCount": 1, "feedbackJunkCount": 1, "ownedAgentIds": 1,
			"external": 1,
		}).
		SetBatchSize(10000),
	)
}

// BulkSetTrustScore writes the propagation result into trustScore (the single
// canonical wallet trust score) using unordered bulk writes.
func (r *Repository) BulkSetTrustScore(ctx context.Context, scores []agentrep.WalletScore) error {
	if len(scores) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(scores))
	for _, s := range scores {
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": s.ID}).
			SetUpdate(bson.M{"$set": bson.M{
				"trustScore":           clipTrustScore(s.Score),
				"trustRated":           true,
				"propagationUpdatedAt": s.At,
			}}))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// RatedTrust is a minimal projection used by ScanRatedTrust.
type RatedTrust struct {
	ID         string  `bson:"_id"`
	TrustScore float64 `bson:"trustScore"`
}

// BulkGetTrustScores loads trustScore for wallet documents by _id.
// Missing ids are omitted from the result map. ids are deduped; queries are chunked at 5000.
func (r *Repository) BulkGetTrustScores(ctx context.Context, ids []string) (map[string]float64, error) {
	unique := dedupeNonEmptyIDs(ids)
	if len(unique) == 0 {
		return map[string]float64{}, nil
	}

	out := make(map[string]float64, len(unique))
	for _, chunk := range chunkStrings(unique, bulkTrustScoreChunkSize) {
		docs, err := r.Find(ctx, bson.M{"_id": bson.M{"$in": chunk}},
			options.Find().SetProjection(bson.M{"_id": 1, "trustScore": 1}))
		if err != nil {
			return nil, fmt.Errorf("wallet repo: bulk get trust scores: %w", err)
		}
		for _, doc := range docs {
			out[doc.ID] = doc.TrustScore
		}
	}
	return out, nil
}

// ScanRatedTrust returns (_id, trustScore) for every RATED wallet across all chains.
// Used to build the publisher-reputation snapshot for score-refresh.
func (r *Repository) ScanRatedTrust(ctx context.Context) ([]RatedTrust, error) {
	cur, err := r.Coll.Find(ctx,
		bson.M{"trustRated": true},
		options.Find().SetProjection(bson.M{"_id": 1, "trustScore": 1}).SetBatchSize(10000),
	)
	if err != nil {
		return nil, fmt.Errorf("wallet repo: scan rated trust: %w", err)
	}
	defer cur.Close(ctx)

	var out []RatedTrust
	for cur.Next(ctx) {
		var rt RatedTrust
		if err := cur.Decode(&rt); err != nil {
			return nil, fmt.Errorf("wallet repo: scan rated trust decode: %w", err)
		}
		out = append(out, rt)
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("wallet repo: scan rated trust cursor: %w", err)
	}
	return out, nil
}

// BulkSetUnrated marks wallets with no trust evidence: clears trustScore and sets trustRated=false.
func (r *Repository) BulkSetUnrated(ctx context.Context, ids []string, at int64) error {
	if len(ids) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(ids))
	for _, id := range ids {
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": id}).
			SetUpdate(bson.M{"$set": bson.M{
				"trustRated":           false,
				"trustScore":           nil,
				"propagationUpdatedAt": at,
			}}))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// FeedbackCounterSet is one reviewer wallet's derived valid/junk verdict tally
// for BulkSetFeedbackCounters.
type FeedbackCounterSet struct {
	ID    string
	Valid int64
	Junk  int64
}

// BulkSetFeedbackCounters SETS feedbackValidCount / feedbackJunkCount for many reviewer
// wallets by _id. Unlike the per-event $inc (ApplyTrustDelta / IncrementFeedbackCounters),
// this overwrites with counts derived from the full verdict corpus in a score-refresh
// cycle, so it is idempotent. Only called on the full cycle (chainID==0) where every
// agent — and thus every reviewer's complete verdict set — was replayed.
func (r *Repository) BulkSetFeedbackCounters(ctx context.Context, counters []FeedbackCounterSet) error {
	if len(counters) == 0 {
		return nil
	}
	now := time.Now().Unix()
	ops := make([]mongodrv.WriteModel, 0, len(counters))
	for _, c := range counters {
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": c.ID}).
			SetUpdate(bson.M{"$set": bson.M{
				"feedbackValidCount": c.Valid,
				"feedbackJunkCount":  c.Junk,
				"updatedAt":          now,
			}}))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("wallet repo: bulk set feedback counters: %w", err)
	}
	return nil
}

// ExternalUpdate is one wallet's external enrichment to persist.
type ExternalUpdate struct {
	ID  string
	Doc ExternalDoc
}

// buildExternalSet maps an ExternalUpdate to the $set field paths (kept pure for
// testing). Only dotted paths are written so other wallet fields are untouched.
func buildExternalSet(u ExternalUpdate) bson.M {
	d := u.Doc
	return bson.M{
		"external.score":           d.Score,
		"external.complete":        d.Complete,
		"external.present":         d.Present,
		"external.balanceUSD":      d.BalanceUSD,
		"external.nonce":           d.Nonce,
		"external.ageDays":         d.AgeDays,
		"external.counterparties":  d.Counterparties,
		"external.cheapAt":         d.CheapAt,
		"external.explorerAt":      d.ExplorerAt,
		"external.explorerSkipped": d.ExplorerSkipped,
		"external.cheapFetched":    d.CheapFetched,
		"external.richFetched":     d.RichFetched,
		"external.ensFetched":      d.ENSFetched,
	}
}

// BulkSetExternal upserts external enrichment for many wallets by _id.
func (r *Repository) BulkSetExternal(ctx context.Context, updates []ExternalUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongodrv.WriteModel, 0, len(updates))
	for _, u := range updates {
		models = append(models, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": u.ID}).
			SetUpdate(bson.M{"$set": buildExternalSet(u)}).
			SetUpsert(true))
	}
	_, err := r.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// ExternalENSUpdate is one wallet's ENS enrichment to persist. It is a
// separate, narrower update than ExternalUpdate so the ENS pass (independent
// of the cheap/explorer passes) never clobbers — and is never clobbered by —
// the cheap/explorer fields on the same external subdocument.
type ExternalENSUpdate struct {
	ID         string
	Score      float64
	ENS        string
	ENSAvatar  string
	ENSAt      int64
	ENSFetched bool
}

// buildExternalENSSet maps an ExternalENSUpdate to the $set field paths (kept
// pure for testing).
func buildExternalENSSet(u ExternalENSUpdate) bson.M {
	return bson.M{
		"external.score":      u.Score,
		"external.ens":        u.ENS,
		"external.ensAvatar":  u.ENSAvatar,
		"external.ensAt":      u.ENSAt,
		"external.ensFetched": u.ENSFetched,
	}
}

// BulkSetExternalENS upserts ENS enrichment for many wallets by _id.
func (r *Repository) BulkSetExternalENS(ctx context.Context, updates []ExternalENSUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	models := make([]mongodrv.WriteModel, 0, len(updates))
	for _, u := range updates {
		models = append(models, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": u.ID}).
			SetUpdate(bson.M{"$set": buildExternalENSSet(u)}).
			SetUpsert(true))
	}
	_, err := r.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// FindExternalByIDs returns cached external enrichment for the given wallet
// ids. Ids not present in the collection are simply absent from the result
// map (not an error).
func (r *Repository) FindExternalByIDs(ctx context.Context, ids []string) (map[string]ExternalDoc, error) {
	if len(ids) == 0 {
		return map[string]ExternalDoc{}, nil
	}
	docs, err := r.Find(ctx, bson.M{"_id": bson.M{"$in": ids}},
		options.Find().SetProjection(bson.M{"_id": 1, "external": 1}))
	if err != nil {
		return nil, fmt.Errorf("wallet repo: find external by ids: %w", err)
	}
	out := make(map[string]ExternalDoc, len(docs))
	for _, d := range docs {
		out[d.ID] = d.External
	}
	return out, nil
}
