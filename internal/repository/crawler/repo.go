package crawler

// repo.go — CrawlersRepository for the "crawlers" MongoDB collection.
// Tracks the last processed block per (chainId, contractAddress, mode) tuple.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// CrawlerDocumentID returns the crawlers collection _id: chainId:contractAddress:mode.
func CrawlerDocumentID(chainID int64, contractAddress string, mode string) string {
	return fmt.Sprintf("%d:%s:%s", chainID, strings.ToLower(contractAddress), mode)
}

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[CrawlerState](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the crawlers collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateOne(ctx, mongodrv.IndexModel{
		Keys: bson.D{
			{Key: "chainId", Value: 1},
			{Key: "contractAddress", Value: 1},
			{Key: "mode", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetName("ux_crawler_key"),
	})
	return err
}

// Get returns the crawler state for the given key, or nil if not found.
func (r *Repository) Get(ctx context.Context, chainID int64, contractAddress string, mode CrawlMode) (*CrawlerState, error) {
	id := CrawlerDocumentID(chainID, contractAddress, string(mode))
	st, err := r.FindOne(ctx, map[string]any{"_id": id})
	if err == mongodrv.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// Upsert persists the crawler state, removing the legacy "direction" field.
func (r *Repository) Upsert(ctx context.Context, st CrawlerState) error {
	st.UpdatedAt = time.Now().Unix()
	st.ID = CrawlerDocumentID(st.ChainID, st.ContractAddress, string(st.Mode))

	filter := map[string]any{"_id": st.ID}
	update := bson.M{
		"$set":   st,
		"$unset": bson.M{"direction": ""},
	}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}
