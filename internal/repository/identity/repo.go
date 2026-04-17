package identity

// repo.go — IdentityHistoryRepository for the "identity_history" MongoDB collection.
// Stores the chronological history of identity events per agent.

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// IdentityChangeDocID returns the identity_history _id: {chainId}:{txHash}:{logIndex}.
func IdentityChangeDocID(chainID int64, txHash string, logIndex uint) string {
	return fmt.Sprintf("%d:%s:%d", chainID, strings.ToLower(txHash), logIndex)
}

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[IdentityChange](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the identity_history collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
				{Key: "blockNumber", Value: 1},
				{Key: "logIndex", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_agent_block_log"),
		},
	})
	return err
}

// Insert inserts a single identity change event. Duplicate _id is silently ignored (idempotent).
func (r *Repository) Insert(ctx context.Context, doc IdentityChange) error {
	doc.ID = IdentityChangeDocID(doc.ChainID, doc.TxHash, doc.LogIndex)
	_, err := r.InsertOne(ctx, doc)
	if mongodrv.IsDuplicateKeyError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("identity repo: insert %s: %w", doc.ID, err)
	}
	return nil
}

// BulkInsert inserts multiple identity changes. Duplicate _id errors are silently ignored (idempotent).
func (r *Repository) BulkInsert(ctx context.Context, docs []IdentityChange) error {
	if len(docs) == 0 {
		return nil
	}
	for i := range docs {
		docs[i].ID = IdentityChangeDocID(docs[i].ChainID, docs[i].TxHash, docs[i].LogIndex)
	}
	_, err := r.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil && !mongodrv.IsDuplicateKeyError(err) {
		return fmt.Errorf("identity repo: bulk insert: %w", err)
	}
	return nil
}

// ListByAgent returns identity history for an agent, ordered by blockNumber ASC.
func (r *Repository) ListByAgent(ctx context.Context, chainID int64, agentID string) ([]IdentityChange, error) {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	opts := options.Find().SetSort(bson.D{
		{Key: "blockNumber", Value: 1},
		{Key: "logIndex", Value: 1},
	})
	return r.Find(ctx, filter, opts)
}
