package main

// backfill-tags — one-shot migration that populates the top-level `tags` field on
// each agent from onchainMetadata.tags.decoded (JSON-encoded array string).
// Run after deploying the tags schema changes.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("backfill-tags: load config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("backfill-tags: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database(cfg.AnalyzedDatabase).Collection(cfg.AgentsColl)

	// Select only agents that have a tags entry in onchainMetadata and either
	// a missing or empty top-level tags field.
	filter := bson.M{
		"onchainMetadata.tags.decoded": bson.M{"$exists": true, "$ne": ""},
		"$or": bson.A{
			bson.M{"tags": bson.M{"$exists": false}},
			bson.M{"tags": bson.M{"$size": 0}},
		},
	}

	cur, err := coll.Find(ctx, filter, options.Find().SetProjection(bson.M{
		"_id":                          1,
		"onchainMetadata.tags.decoded": 1,
	}))
	if err != nil {
		log.Fatalf("backfill-tags: find: %v", err)
	}
	defer cur.Close(ctx)

	var (
		scanned int
		updated int
		skipped int
	)

	for cur.Next(ctx) {
		var row struct {
			ID              string `bson:"_id"`
			OnchainMetadata struct {
				Tags struct {
					Decoded string `bson:"decoded"`
				} `bson:"tags"`
			} `bson:"onchainMetadata"`
		}
		if err := cur.Decode(&row); err != nil {
			log.Printf("backfill-tags: decode row: %v", err)
			continue
		}
		scanned++

		tags := parseTagsDecoded(row.OnchainMetadata.Tags.Decoded)
		if len(tags) == 0 {
			skipped++
			continue
		}

		_, err := coll.UpdateOne(ctx,
			bson.M{"_id": row.ID},
			bson.M{"$set": bson.M{"tags": tags}},
		)
		if err != nil {
			log.Printf("backfill-tags: update %s: %v", row.ID, err)
			continue
		}
		updated++
	}
	if err := cur.Err(); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("backfill-tags: cursor: %v", err)
	}

	fmt.Printf("backfill-tags: scanned=%d updated=%d skipped=%d\n", scanned, updated, skipped)
}

// parseTagsDecoded parses a JSON-array string like ["a","b"] into a lowercase,
// deduped, trimmed slice. Returns nil for empty/malformed input.
func parseTagsDecoded(decoded string) []string {
	s := strings.TrimSpace(decoded)
	if s == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	seen := make(map[string]struct{}, len(arr))
	out := make([]string, 0, len(arr))
	for _, t := range arr {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
