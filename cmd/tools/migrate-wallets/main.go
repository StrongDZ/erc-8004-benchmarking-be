// migrate-wallets backfills the wallets collection from existing agents.owner
// and feedback_history.clientAddress values. Safe to re-run (idempotent).
//
// Usage:
//
//	go run ./cmd/tools/migrate-wallets [--dry-run] [--chain-id=8453]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	mongoinfra "erc-8004-benchmarking-be/internal/infra/mongo"
	mongorepo "erc-8004-benchmarking-be/internal/repository"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "print counts without writing")
	chainFilter := flag.Int64("chain-id", 0, "only migrate this chain (0 = all)")
	flag.Parse()

	ctx := context.Background()
	cli, err := mongoinfra.NewClient(ctx, mustEnv("MONGO_URI"), mongoinfra.DefaultPoolOptions())
	must(err, "mongo connect")
	defer cli.Disconnect(ctx)

	db := cli.Database(mustEnv("MONGO_DATABASE_ANALYZED_AGENTS"))
	walletColl := envOr("MONGO_COLLECTION_WALLETS", "wallets")
	agentsColl := envOr("MONGO_COLLECTION_AGENTS", "agents")
	fbColl := envOr("MONGO_COLLECTION_FEEDBACK_HISTORY", "feedback_history")
	t0 := envFloat("TRUST_WEIGHT_COLD_START_T0", 10.0)

	wr := walletrepo.NewRepository(db, walletColl)
	if !*dryRun {
		must(wr.EnsureIndexes(ctx), "ensure indexes")
	}

	ownership, err := collectOwnership(ctx, db.Collection(agentsColl), *chainFilter)
	must(err, "collect ownership")
	log.Printf("owners found: %d unique (chainId,address) pairs", len(ownership))

	senders, err := collectSenders(ctx, db.Collection(fbColl), *chainFilter)
	must(err, "collect senders")
	log.Printf("senders found: %d unique (chainId,address) pairs", len(senders))

	merged := mergeKeys(ownership, senders)
	log.Printf("total wallet candidates: %d", len(merged))

	if *dryRun {
		log.Printf("dry-run: would upsert %d wallets (t0=%.1f)", len(merged), t0)
		return
	}

	owners, users := 0, 0
	for key, ownedAgents := range merged {
		_, err := wr.UpsertCold(ctx, key.chainID, key.address, t0)
		if err != nil {
			log.Printf("WARN upsert (%d,%s): %v", key.chainID, key.address, err)
			continue
		}
		if len(ownedAgents) > 0 {
			if err := setOwnerFields(ctx, db.Collection(walletColl), key.chainID, key.address, ownedAgents); err != nil {
				log.Printf("WARN setOwner (%d,%s): %v", key.chainID, key.address, err)
				continue
			}
			owners++
		} else {
			users++
		}
	}
	log.Printf("done: %d owner-wallets, %d user-wallets", owners, users)
}

type chainAddr struct {
	chainID int64
	address string
}

func collectOwnership(ctx context.Context, coll *mongo.Collection, chainFilter int64) (map[chainAddr][]string, error) {
	filter := bson.M{"owner": bson.M{"$ne": ""}}
	if chainFilter > 0 {
		filter["chainId"] = chainFilter
	}
	cur, err := coll.Find(ctx, filter, mongorepo.ProjectionForOwnership())
	if err != nil {
		return nil, fmt.Errorf("find agents: %w", err)
	}
	defer cur.Close(ctx)

	out := make(map[chainAddr][]string)
	for cur.Next(ctx) {
		var row struct {
			AgentID string `bson:"agentId"`
			ChainID int64  `bson:"chainId"`
			Owner   string `bson:"owner"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode agent row: %w", err)
		}
		addr := strings.ToLower(strings.TrimSpace(row.Owner))
		if addr == "" {
			continue
		}
		key := chainAddr{chainID: row.ChainID, address: addr}
		out[key] = append(out[key], row.AgentID)
	}
	return out, cur.Err()
}

func collectSenders(ctx context.Context, coll *mongo.Collection, chainFilter int64) (map[chainAddr]struct{}, error) {
	filter := bson.M{"clientAddress": bson.M{"$ne": ""}}
	if chainFilter > 0 {
		filter["chainId"] = chainFilter
	}
	cur, err := coll.Find(ctx, filter, mongorepo.ProjectionForSender())
	if err != nil {
		return nil, fmt.Errorf("find feedback: %w", err)
	}
	defer cur.Close(ctx)

	out := make(map[chainAddr]struct{})
	for cur.Next(ctx) {
		var row struct {
			ChainID       int64  `bson:"chainId"`
			ClientAddress string `bson:"clientAddress"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode feedback row: %w", err)
		}
		addr := strings.ToLower(strings.TrimSpace(row.ClientAddress))
		if addr == "" {
			continue
		}
		out[chainAddr{chainID: row.ChainID, address: addr}] = struct{}{}
	}
	return out, cur.Err()
}

func mergeKeys(owners map[chainAddr][]string, senders map[chainAddr]struct{}) map[chainAddr][]string {
	out := make(map[chainAddr][]string, len(owners)+len(senders))
	for k, v := range owners {
		out[k] = v
	}
	for k := range senders {
		if _, ok := out[k]; !ok {
			out[k] = nil
		}
	}
	return out
}

func setOwnerFields(ctx context.Context, coll *mongo.Collection, chainID int64, addr string, ownedAgents []string) error {
	id := fmt.Sprintf("%d:%s", chainID, strings.ToLower(addr))
	_, err := coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$set": bson.M{
			"kind":          "owner",
			"ownedAgentIds": ownedAgents,
			"updatedAt":     time.Now().Unix(),
		},
	})
	return err
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

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func must(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}
