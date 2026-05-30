package oasfschema

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Entry is one flattened OASF taxonomy node stored in oasf_skills or oasf_domains.
// _id is the full hierarchical path key (e.g. "natural_language_processing/summarization").
type Entry struct {
	Key          string    `bson:"_id"                     json:"key"`
	ShortName    string    `bson:"shortName"               json:"shortName"`
	Caption      string    `bson:"caption"                 json:"caption"`
	Description  string    `bson:"description"             json:"description"`
	UID          int       `bson:"uid"                     json:"uid"`
	Category     string    `bson:"category"                json:"category"`
	CategoryName string    `bson:"categoryName"            json:"categoryName"`
	Depth        int       `bson:"depth"                   json:"depth"`
	ParentKey    string    `bson:"parentKey,omitempty"     json:"parentKey,omitempty"`
	UpdatedAt    time.Time `bson:"updatedAt"               json:"-"`
}

// Repository wraps one oasf_skills or oasf_domains collection.
type Repository struct {
	coll *mongodrv.Collection
}

// NewRepository returns a Repository bound to the named collection in db.
func NewRepository(db *mongodrv.Database, collName string) *Repository {
	return &Repository{coll: db.Collection(collName)}
}

// UpsertAll bulk-upserts all entries using replaceOne(upsert=true) keyed on _id.
func (r *Repository) UpsertAll(ctx context.Context, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	now := time.Now().UTC()
	models := make([]mongodrv.WriteModel, len(entries))
	for i, e := range entries {
		e.UpdatedAt = now
		models[i] = mongodrv.NewReplaceOneModel().
			SetFilter(bson.M{"_id": e.Key}).
			SetReplacement(e).
			SetUpsert(true)
	}
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := r.coll.BulkWrite(ctx, models, opts); err != nil {
		return fmt.Errorf("oasfschema upsert: %w", err)
	}
	return nil
}

// FindAll returns all entries sorted by _id ascending.
func (r *Repository) FindAll(ctx context.Context) ([]Entry, error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	cur, err := r.coll.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("oasfschema find: %w", err)
	}
	defer cur.Close(ctx)
	var out []Entry
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("oasfschema decode: %w", err)
	}
	return out, nil
}

// Count returns the number of documents in the collection.
func (r *Repository) Count(ctx context.Context) (int64, error) {
	n, err := r.coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("oasfschema count: %w", err)
	}
	return n, nil
}
