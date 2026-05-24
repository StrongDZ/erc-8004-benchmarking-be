// migrate-feedback-fields backfills validationVerdict / qualityScore / wiComputedAt
// on existing feedback_history records. Idempotent: only writes records where
// validationVerdict is absent.
//
// Usage:
//
//	go run ./cmd/tools/migrate-feedback-fields [--dry-run] [--chain-id=8453]
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "count records without writing")
	chainFilter := flag.Int64("chain-id", 0, "only migrate this chain (0 = all)")
	flag.Parse()

	ctx := context.Background()
	cli, err := mongoinfra.NewClient(ctx, mustEnv("MONGO_URI"), mongoinfra.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer cli.Disconnect(ctx)

	db := cli.Database(mustEnv("MONGO_DATABASE_ANALYZED_AGENTS"))
	fbColl := db.Collection(envOr("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history"))

	filter := bson.M{"validationVerdict": bson.M{"$exists": false}}
	if *chainFilter > 0 {
		filter["chainId"] = *chainFilter
	}

	count, err := fbColl.CountDocuments(ctx, filter)
	if err != nil {
		log.Fatalf("count: %v", err)
	}
	log.Printf("records missing validationVerdict: %d", count)

	if *dryRun {
		log.Printf("dry-run: would update %d records", count)
		return
	}

	res, err := fbColl.UpdateMany(ctx, filter, bson.M{
		"$set": bson.M{
			"validationVerdict": "legacy",
			"qualityScore":      0.0,
			"wiComputedAt":      int64(0),
		},
	}, options.Update())
	if err != nil {
		log.Fatalf("update: %v", err)
	}
	log.Printf("done: matched=%d modified=%d", res.MatchedCount, res.ModifiedCount)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing required env %s", k)
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
