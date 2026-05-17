package main

// restart-trustrank — destructive reset for TrustRank re-processing:
//   1) Drop the entire analyzed agents database (agents, scores, feedbacks, …).
//   2) Delete config documents whose _id matches trustrank_worker_<chain_id>
//      (per-chain TrustRank cursors in the primary DB config collection).
//
// Requires .env / MONGO_URI like other tools. Use --dry-run first.
// scripts/restart-trustrank.sh runs `docker compose up --build` after success (skipped with --dry-run).

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "only print what would be done (no writes)")
	yes := flag.Bool("y", false, "skip interactive confirmation")
	flag.Parse()

	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("restart-trustrank: config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("restart-trustrank: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	analyzedName := cfg.AnalyzedDatabase
	primaryName := cfg.MongoDatabase
	configColl := cfg.ConfigColl

	checkpointFilter := bson.M{
		"_id": primitive.Regex{Pattern: `^trustrank_worker_\d+$`, Options: ""},
	}

	cfgDB := client.Database(primaryName).Collection(configColl)
	delCount, err := cfgDB.CountDocuments(ctx, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: count checkpoints: %v", err)
	}

	log.Printf("analyzed DB to drop: %q", analyzedName)
	log.Printf("checkpoints to delete: %d doc(s) in %q.%q matching trustrank_worker_<chain_id>", delCount, primaryName, configColl)

	if *dryRun {
		log.Print("dry-run: no changes made")
		return
	}

	if !*yes {
		fmt.Fprintf(os.Stderr, "This will DROP database %q and delete %d checkpoint(s). Type YES to continue: ", analyzedName, delCount)
		var confirm string
		if _, err := fmt.Scanln(&confirm); err != nil || confirm != "YES" {
			log.Fatal("restart-trustrank: aborted (expected YES)")
		}
	}

	if err := client.Database(analyzedName).Drop(ctx); err != nil {
		log.Fatalf("restart-trustrank: drop database %q: %v", analyzedName, err)
	}
	log.Printf("dropped database %q", analyzedName)

	res, err := cfgDB.DeleteMany(ctx, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: delete checkpoints: %v", err)
	}
	log.Printf("deleted %d checkpoint document(s) from %q.%q", res.DeletedCount, primaryName, configColl)
}
