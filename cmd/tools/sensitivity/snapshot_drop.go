package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

func snapshotDrop(args []string) {
	fs := flag.NewFlagSet("snapshot drop", flag.ExitOnError)
	id := fs.String("id", "", "snapshot ID (required)")
	force := fs.Bool("force", false, "skip confirmation prompt")
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

	if !*force {
		fmt.Printf("Drop snapshot %s? This deletes database %s and removes the index row.\n",
			*id, snapshot.SnapshotDBName(*id))
		fmt.Print("Type the snapshot ID to confirm: ")
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		if strings.TrimSpace(line) != *id {
			fmt.Fprintln(os.Stderr, "confirmation mismatch — aborting")
			os.Exit(1)
		}
	}

	dbName := snapshot.SnapshotDBName(*id)
	if err := mc.Database(dbName).Drop(ctx); err != nil {
		log.Fatalf("drop snapshot db: %v", err)
	}

	idxRepo := snapshot.NewIndexRepository(mc.Database(snapshot.IndexDatabaseName))
	if err := idxRepo.Delete(ctx, *id); err != nil {
		log.Fatalf("delete index row: %v", err)
	}

	fmt.Printf("Dropped snapshot %s (database %s and index row removed)\n", *id, dbName)
}
