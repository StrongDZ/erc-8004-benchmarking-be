package offchain

// repo.go — OffchainDataRepository for the "offchain_data" MongoDB collection.
// Stores resolved URI content or fetch errors keyed by stable hash of the URI.
// Status field encodes the fetch outcome: -1 = fetch error, 1 = fetched non-JSON, 5 = fetched JSON.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// OffchainDocumentID returns a stable SHA-256 hash id for any URI.
func OffchainDocumentID(uri string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(uri)))
	return hex.EncodeToString(sum[:])
}

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[OffchainData](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the offchain_data collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys:    bson.D{{Key: "uri", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("ux_uri"),
		},
		{
			Keys:    bson.D{{Key: "status", Value: 1}},
			Options: options.Index().SetName("ix_status"),
		},
	})
	return err
}

// UpsertSuccess writes valid JSON content for uri (status=5).
func (r *Repository) UpsertSuccess(ctx context.Context, uri, jsonText, sourceType, eventType, contractType string) error {
	id := OffchainDocumentID(uri)
	update := bson.M{"$set": bson.M{
		"_id":          id,
		"uri":          uri,
		"status":       StatusFetchedJSON,
		"content":      jsonText,
		"sourceType":   sourceType,
		"eventType":    eventType,
		"contractType": contractType,
		"fetchError":   "",
		"contentSize":  len(jsonText),
	}}
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, update, options.Update().SetUpsert(true))
	return err
}

// UpsertFetchedNotJSON records a successful fetch where the content is not valid JSON (status=1).
func (r *Repository) UpsertFetchedNotJSON(ctx context.Context, uri, rawBody, sourceType, eventType, contractType string) error {
	id := OffchainDocumentID(uri)
	update := bson.M{"$set": bson.M{
		"_id":          id,
		"uri":          uri,
		"status":       StatusFetchedNotJSON,
		"content":      rawBody,
		"sourceType":   sourceType,
		"eventType":    eventType,
		"contractType": contractType,
		"fetchError":   "",
		"contentSize":  len(rawBody),
	}}
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, update, options.Update().SetUpsert(true))
	return err
}

// UpsertFailure records that uri could not be fetched due to a network/HTTP error (status=-1).
func (r *Repository) UpsertFailure(ctx context.Context, uri, sourceType, eventType, contractType, errMsg string) error {
	id := OffchainDocumentID(uri)
	update := bson.M{"$set": bson.M{
		"_id":          id,
		"uri":          uri,
		"status":       StatusFetchFailed,
		"content":      "",
		"sourceType":   sourceType,
		"eventType":    eventType,
		"contractType": contractType,
		"fetchError":   errMsg,
		"contentSize":  0,
	}}
	_, err := r.UpdateOne(ctx, bson.M{"_id": id}, update, options.Update().SetUpsert(true))
	return err
}

// Exists reports whether any record (any status) exists for the given URI.
func (r *Repository) Exists(ctx context.Context, uri string) (bool, error) {
	id := OffchainDocumentID(uri)
	_, err := r.FindOne(ctx, bson.M{"_id": id})
	if err == mongodrv.ErrNoDocuments {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// FindByURIs loads offchain_data rows for the given logical URIs (matched by stable hash _id).
func (r *Repository) FindByURIs(ctx context.Context, uris []string) ([]OffchainData, error) {
	if len(uris) == 0 {
		return nil, nil
	}
	ids := make([]string, len(uris))
	for i, uri := range uris {
		ids[i] = OffchainDocumentID(uri)
	}
	docs, err := r.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, fmt.Errorf("offchain repo: find by uris: %w", err)
	}
	return docs, nil
}

// HasSuccessfulFetch returns true if a document exists with status=5 (valid JSON fetched).
func (r *Repository) HasSuccessfulFetch(ctx context.Context, uri string) (bool, error) {
	id := OffchainDocumentID(uri)
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err == mongodrv.ErrNoDocuments {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return doc.Status == StatusFetchedJSON, nil
}

// GetContent returns the cached JSON content for a URI, or ("", false) if status != 5.
func (r *Repository) GetContent(ctx context.Context, uri string) (string, bool, error) {
	id := OffchainDocumentID(uri)
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err == mongodrv.ErrNoDocuments {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if doc.Status != StatusFetchedJSON {
		return "", false, nil
	}
	return doc.Content, true, nil
}
