package mongo

// types.go — Generic MongoDB repository types used by all repository packages.

import (
	"context"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BaseRepo defines the common CRUD contract for all repositories.
type BaseRepo[T any] interface {
	EnsureIndexes(ctx context.Context) error
	Find(ctx context.Context, filter any, opts ...*options.FindOptions) ([]T, error)
	FindOne(ctx context.Context, filter any, opts ...*options.FindOneOptions) (T, error)
	InsertOne(ctx context.Context, doc T) (*mongodrv.InsertOneResult, error)
	UpdateOne(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (*mongodrv.UpdateResult, error)
	UpdateMany(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (*mongodrv.UpdateResult, error)
	DeleteOne(ctx context.Context, filter any) (*mongodrv.DeleteResult, error)
	DeleteMany(ctx context.Context, filter any) (*mongodrv.DeleteResult, error)
	Count(ctx context.Context, filter any) (int64, error)
	BulkWrite(ctx context.Context, models []mongodrv.WriteModel, opts ...*options.BulkWriteOptions) (*mongodrv.BulkWriteResult, error)
}

// MongoRepoImpl is a generic MongoDB collection wrapper that satisfies BaseRepo[T].
type MongoRepoImpl[T any] struct {
	Coll *mongodrv.Collection
}

var _ BaseRepo[any] = (*MongoRepoImpl[any])(nil)
