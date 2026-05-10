package mongo

// client.go — MongoDB client factory with connection pool tuning.

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PoolOptions carries connection pool tuning parameters.
type PoolOptions struct {
	MaxPoolSize    uint64
	MinPoolSize    uint64
	MaxConnIdleTime time.Duration
}

// DefaultPoolOptions returns conservative defaults suitable for most deployments.
func DefaultPoolOptions() PoolOptions {
	return PoolOptions{
		MaxPoolSize:     200,
		MinPoolSize:     10,
		MaxConnIdleTime: 10 * time.Minute,
	}
}

// NewClient dials MongoDB with explicit pool tuning, pings it, and returns a
// ready-to-use client. Caller is responsible for calling client.Disconnect().
func NewClient(ctx context.Context, uri string, pool PoolOptions) (*mongo.Client, error) {
	clientOpts := options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(pool.MaxPoolSize).
		SetMinPoolSize(pool.MinPoolSize).
		SetMaxConnIdleTime(pool.MaxConnIdleTime)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	return client, nil
}
