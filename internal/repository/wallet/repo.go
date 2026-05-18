package wallet

// repo.go — WalletRepository for the "wallets" MongoDB collection.

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
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
				{Key: "trustScorePropagated", Value: -1},
			},
			Options: options.Index().SetName("idx_chain_trust_pp_desc"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "kind", Value: 1},
				{Key: "trustScorePropagated", Value: -1},
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
