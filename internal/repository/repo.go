package mongo

// repo.go — Generic MongoDB base repository implementation.

import (
	"context"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// NewMongoRepo returns a MongoRepoImpl bound to the named collection.
func NewMongoRepo[T any](db *mongodrv.Database, collectionName string) *MongoRepoImpl[T] {
	return &MongoRepoImpl[T]{Coll: db.Collection(collectionName)}
}

func (r *MongoRepoImpl[T]) EnsureIndexes(_ context.Context) error { return nil }

// Indexes returns the collection index view (CreateOne / CreateMany, etc.).
func (r *MongoRepoImpl[T]) Indexes() mongodrv.IndexView {
	return r.Coll.Indexes()
}

func (r *MongoRepoImpl[T]) Find(ctx context.Context, filter any, opts ...*options.FindOptions) ([]T, error) {
	cur, err := r.Coll.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	docs := make([]T, 0)
	for cur.Next(ctx) {
		var doc T
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, cur.Err()
}

func (r *MongoRepoImpl[T]) FindOne(ctx context.Context, filter any, opts ...*options.FindOneOptions) (T, error) {
	var out T
	err := r.Coll.FindOne(ctx, filter, opts...).Decode(&out)
	return out, err
}

func (r *MongoRepoImpl[T]) InsertOne(ctx context.Context, doc T) (*mongodrv.InsertOneResult, error) {
	return r.Coll.InsertOne(ctx, doc)
}

func (r *MongoRepoImpl[T]) InsertMany(ctx context.Context, docs []T, opts ...*options.InsertManyOptions) (*mongodrv.InsertManyResult, error) {
	ifaces := make([]interface{}, len(docs))
	for i := range docs {
		ifaces[i] = docs[i]
	}
	return r.Coll.InsertMany(ctx, ifaces)
}

func (r *MongoRepoImpl[T]) UpdateOne(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (*mongodrv.UpdateResult, error) {
	return r.Coll.UpdateOne(ctx, filter, update, opts...)
}

func (r *MongoRepoImpl[T]) UpdateMany(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (*mongodrv.UpdateResult, error) {
	return r.Coll.UpdateMany(ctx, filter, update, opts...)
}

func (r *MongoRepoImpl[T]) DeleteOne(ctx context.Context, filter any) (*mongodrv.DeleteResult, error) {
	return r.Coll.DeleteOne(ctx, filter)
}

func (r *MongoRepoImpl[T]) DeleteMany(ctx context.Context, filter any) (*mongodrv.DeleteResult, error) {
	return r.Coll.DeleteMany(ctx, filter)
}

func (r *MongoRepoImpl[T]) Count(ctx context.Context, filter any) (int64, error) {
	return r.Coll.CountDocuments(ctx, filter)
}

func (r *MongoRepoImpl[T]) BulkWrite(ctx context.Context, models []mongodrv.WriteModel, opts ...*options.BulkWriteOptions) (*mongodrv.BulkWriteResult, error) {
	return r.Coll.BulkWrite(ctx, models, opts...)
}
