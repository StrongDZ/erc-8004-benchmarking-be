package config

// repo.go — ConfigRepository and ContractsRepository for their respective MongoDB collections.
// ConfigRepository: key-value store for global runtime config (e.g. bootstrap watermarks).
// ContractsRepository: stores chain + contract + ABI definitions used by the crawler.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// ---- Config ----------------------------------------------------------------

const (
	// ConfigKeyURIBootstrapWatermark stores the global decodedAt cutoff for bootstrap URI producer.
	ConfigKeyURIBootstrapWatermark = "uri_bootstrap_watermark"
)

// ContractsDocumentID returns the contracts collection _id (equals chainId).
func ContractsDocumentID(chainID int64) int64 {
	return chainID
}

// NewConfigRepository returns a ConfigRepository bound to the named collection.
func NewConfigRepository(db *mongodrv.Database, collectionName string) *ConfigRepository {
	m := mongorepo.NewMongoRepo[ConfigEntry](db, collectionName)
	return &ConfigRepository{MongoRepoImpl: *m}
}

// EnsureIndexes creates a unique index on the key field.
func (r *ConfigRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateOne(ctx, mongodrv.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("ux_key"),
	})
	return err
}

// Get returns the config entry by key, or ErrNoDocuments if absent.
func (r *ConfigRepository) Get(ctx context.Context, key string) (*ConfigEntry, error) {
	entry, err := r.FindOne(ctx, bson.M{"_id": key})
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// SetIfAbsent inserts the config entry only when it does not exist.
// Returns true when the document was newly inserted.
func (r *ConfigRepository) SetIfAbsent(ctx context.Context, key string, metadata map[string]any) (bool, error) {
	filter := bson.M{"_id": key}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":      key,
			"key":      key,
			"metadata": metadata,
		},
	}
	res, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return false, err
	}
	return res.UpsertedCount > 0, nil
}

// UpdateMetadata atomically updates the metadata map on an existing config entry.
func (r *ConfigRepository) UpdateMetadata(ctx context.Context, key string, metadata map[string]any) error {
	filter := bson.M{"_id": key}
	update := bson.M{"$set": bson.M{"metadata": metadata}}
	_, err := r.UpdateOne(ctx, filter, update)
	return err
}

// ---- Contracts -------------------------------------------------------------

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

// Upsert inserts or replaces the config document for the given chain.
func (r *ContractsRepository) Upsert(ctx context.Context, item ContractsConfig) error {
	item.ID = ContractsDocumentID(item.ChainID)
	filter := map[string]any{"_id": item.ID}
	update := map[string]any{"$set": item}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}
