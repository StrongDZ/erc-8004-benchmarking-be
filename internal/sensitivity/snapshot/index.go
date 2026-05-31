package snapshot

// index.go — IndexRepository wraps bachelor_sensitivity_index.snapshots.
// One document per snapshot; primary key is SnapshotMeta.ID.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

const (
	IndexCollectionName = "snapshots"
	IndexDatabaseName   = "bachelor_sensitivity_index"
)

// IndexRepository provides CRUD over bachelor_sensitivity_index.snapshots.
type IndexRepository struct {
	mongorepo.MongoRepoImpl[SnapshotMeta]
}

// NewIndexRepository wires the repository to the given index database.
// Caller should pass mc.Database("bachelor_sensitivity_index").
func NewIndexRepository(db *mongodrv.Database) *IndexRepository {
	m := mongorepo.NewMongoRepo[SnapshotMeta](db, IndexCollectionName)
	return &IndexRepository{MongoRepoImpl: *m}
}

// Insert writes a new SnapshotMeta. Fails if a document with the same _id exists.
func (r *IndexRepository) Insert(ctx context.Context, meta SnapshotMeta) error {
	_, err := r.Coll.InsertOne(ctx, meta)
	return err
}

// List returns all snapshots sorted by createdAt DESC.
func (r *IndexRepository) List(ctx context.Context) ([]SnapshotMeta, error) {
	findOpts := options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}})
	cursor, err := r.Coll.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var out []SnapshotMeta
	if err := cursor.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// FindByID returns the snapshot meta or nil when no document matches.
func (r *IndexRepository) FindByID(ctx context.Context, id string) (*SnapshotMeta, error) {
	var meta SnapshotMeta
	err := r.Coll.FindOne(ctx, bson.M{"_id": id}).Decode(&meta)
	if err == mongodrv.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Delete removes one snapshot meta by ID.
func (r *IndexRepository) Delete(ctx context.Context, id string) error {
	_, err := r.Coll.DeleteOne(ctx, bson.M{"_id": id})
	return err
}
