package contracts

import (
	"context"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// ContractsDocumentID returns the contracts collection _id (equals chainId).
func ContractsDocumentID(chainID int64) int64 {
	return chainID
}

// NewContractsRepository returns a ContractsRepository bound to the named collection.
func NewContractsRepository(db *mongodrv.Database, collectionName string) *ContractsRepository {
	m := mongorepo.NewMongoRepo[ContractsConfig](db, collectionName)
	return &ContractsRepository{MongoRepoImpl: *m}
}

// EnsureIndexes creates indexes on the contracts collection.
func (r *ContractsRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys:    map[string]int{"chainId": 1},
			Options: options.Index().SetName("idx_chain_id"),
		},
		{
			Keys:    map[string]int{"active": 1},
			Options: options.Index().SetName("idx_active"),
		},
	})
	return err
}

// FindActive returns all active chain configs.
func (r *ContractsRepository) FindActive(ctx context.Context) ([]ContractsConfig, error) {
	return r.Find(ctx, map[string]any{"active": true})
}

// FindByChainID returns the contracts config for a single chain, or ErrNoDocuments.
func (r *ContractsRepository) FindByChainID(ctx context.Context, chainID int64) (ContractsConfig, error) {
	return r.FindOne(ctx, map[string]any{"_id": ContractsDocumentID(chainID)})
}

// Upsert inserts or replaces the config document for the given chain.
func (r *ContractsRepository) Upsert(ctx context.Context, item ContractsConfig) error {
	item.ID = ContractsDocumentID(item.ChainID)
	filter := map[string]any{"_id": item.ID}
	update := map[string]any{"$set": item}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}
