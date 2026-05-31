package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

func snapshotInspect(args []string) {
	fs := flag.NewFlagSet("snapshot inspect", flag.ExitOnError)
	id := fs.String("id", "", "snapshot ID (required)")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *id == "" {
		fmt.Fprintln(os.Stderr, "--id is required")
		os.Exit(1)
	}

	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer mc.Disconnect(ctx)

	idxRepo := snapshot.NewIndexRepository(mc.Database(snapshot.IndexDatabaseName))
	meta, err := idxRepo.FindByID(ctx, *id)
	if err != nil {
		log.Fatalf("find: %v", err)
	}
	if meta == nil {
		fmt.Fprintf(os.Stderr, "no snapshot with id %q\n", *id)
		os.Exit(1)
	}

	fmt.Printf("Snapshot %s\n", meta.ID)
	fmt.Printf("  label:                 %s\n", meta.Label)
	fmt.Printf("  created at:            %s\n", time.Unix(meta.CreatedAt, 0).Format(time.RFC3339))
	fmt.Printf("  chain ID:              %d\n", meta.ChainID)
	fmt.Printf("  source DB:             %s\n", meta.SourceDB)
	fmt.Printf("  source last event ts:  %s\n", time.Unix(meta.SourceLastEventTs, 0).Format(time.RFC3339))
	fmt.Printf("  git commit:            %s\n", meta.GitCommitSHA)
	fmt.Printf("  filters:               %+v\n", meta.Filters)
	fmt.Printf("  stats:\n")
	fmt.Printf("    agents:     %d\n", meta.Stats.Agents)
	fmt.Printf("    feedbacks:  %d\n", meta.Stats.Feedbacks)
	fmt.Printf("    edges:      %d\n", meta.Stats.Edges)
	fmt.Printf("  database name: %s\n", snapshot.SnapshotDBName(meta.ID))
}
