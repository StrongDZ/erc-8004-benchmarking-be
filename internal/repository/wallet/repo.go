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
// with trustScore = t0. If already present, only bumps updatedAt. Returns the post-upsert doc.
func (r *Repository) UpsertCold(ctx context.Context, chainID int64, address string, t0 float64) (*WalletDocument, error) {
	id := WalletDocumentID(chainID, address)
	now := time.Now().Unix()
	update := buildUpsertColdUpdate(chainID, address, t0, now)
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, update, options.Update().SetUpsert(true))
	if err != nil {
		return nil, err
	}
	return r.GetByAddress(ctx, chainID, address)
}

// LinkOwnerAgent ensures an owner wallet exists and associates agentID with it.
// Safe to call repeatedly; uses $addToSet for ownedAgentIds.
func (r *Repository) LinkOwnerAgent(ctx context.Context, chainID int64, ownerAddr, agentID string, t0 float64) error {
	if strings.TrimSpace(ownerAddr) == "" || strings.TrimSpace(agentID) == "" {
		return nil
	}
	if _, err := r.UpsertCold(ctx, chainID, ownerAddr, t0); err != nil {
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
func (r *Repository) ReconcileOwnership(ctx context.Context, chainID int64, agentID, ownerAddr string, t0 float64) error {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(ownerAddr) == "" {
		return nil
	}
	if _, err := r.UpsertCold(ctx, chainID, ownerAddr, t0); err != nil {
		return fmt.Errorf("wallet repo: reconcile upsert (%d, %s): %w", chainID, ownerAddr, err)
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
		return fmt.Errorf("wallet repo: reconcile pull stale (%d, %s): %w", chainID, agentID, err)
	}
	if _, err := r.UpdateOne(ctx, bson.M{"_id": ownerID}, bson.M{
		"$addToSet": bson.M{"ownedAgentIds": agentID},
		"$set": bson.M{
			"kind":      string(WalletKindOwner),
			"updatedAt": now,
		},
	}); err != nil {
		return fmt.Errorf("wallet repo: reconcile add (%d, %s, %s): %w", chainID, ownerAddr, agentID, err)
	}
	return nil
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
