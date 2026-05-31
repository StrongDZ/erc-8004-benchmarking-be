package snapshot

// index.go — IndexRepository wraps bachelor_sensitivity_index.snapshots.
// One document per snapshot; primary key is SnapshotMeta.ID.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// IndexCollectionName is the collection storing snapshot metadata.
const IndexCollectionName = "snapshots"

// IndexRepository provides CRUD over bachelor_sensitivity_index.snapshots.
type IndexRepository struct {
	coll *mongodrv.Collection
}

// NewIndexRepository wires the repository to the given index database.
// Caller should pass mc.Database("bachelor_sensitivity_index").
func NewIndexRepository(db *mongodrv.Database) *IndexRepository {
	return &IndexRepository{coll: db.Collection(IndexCollectionName)}
}

// Insert writes a new SnapshotMeta. Fails if a document with the same _id exists.
func (r *IndexRepository) Insert(ctx context.Context, meta SnapshotMeta) error {
	_, err := r.coll.InsertOne(ctx, meta)
	return err
}

// List returns all snapshots sorted by createdAt DESC.
func (r *IndexRepository) List(ctx context.Context) ([]SnapshotMeta, error) {
	opts := bson.D{}
	cursor, err := r.coll.Find(ctx, opts)
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
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&meta)
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
	_, err := r.coll.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// IndexDatabaseName is the well-known name for the metadata index database.
const IndexDatabaseName = "bachelor_sensitivity_index"
