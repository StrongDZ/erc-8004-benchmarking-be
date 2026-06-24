package adminaudit

// repo.go — Repository for the "admin_audit_log" MongoDB collection.

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[Entry](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the indexes admin-audit queries rely on.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateOne(ctx, mongodrv.IndexModel{
		Keys:    bson.D{{Key: "timestamp", Value: -1}},
		Options: options.Index().SetName("ix_admin_audit_timestamp"),
	})
	return err
}

// Append inserts one audit entry. Failures are the caller's concern (the
// admin action itself must not be blocked by an audit-log write error).
func (r *Repository) Append(ctx context.Context, e Entry) error {
	_, err := r.InsertOne(ctx, e)
	return err
}
