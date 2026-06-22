package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"

	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

func snapshotCreate(args []string) {
	fs := flag.NewFlagSet("snapshot create", flag.ExitOnError)
	label := fs.String("label", "", "human-readable label for the snapshot (required)")
	chainID := fs.Int64("chain-id", 11155111, "EVM chain ID to snapshot")
	includeSpam := fs.Bool("include-spam", false, "include spam/noise feedbacks (default false)")
	minFeedbacks := fs.Int("min-feedbacks", 1, "agents must have at least this many feedbacks")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *label == "" {
		fmt.Fprintln(os.Stderr, "--label is required")
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

	// Build filter.
	filt := snapshot.DefaultFilter()
	filt.IncludeSpam = *includeSpam
	filt.MinFeedbacks = *minFeedbacks

	// Source collections.
	srcDB := mc.Database(cfg.AnalyzedDatabase)
	src := snapshot.SourceCollections{
		Feedbacks:       srcDB.Collection(cfg.FeedbackHistColl),
		Agents:          srcDB.Collection(cfg.AgentsColl),
		AgentScoreStats: srcDB.Collection(cfg.ScoreStatsColl),
		Wallets:         srcDB.Collection(cfg.WalletColl),
	}

	now := time.Now()
	snapID := snapshot.NewSnapshotID(now)

	log.Printf("snapshot create: id=%s label=%q chain=%d filters=%+v", snapID, *label, *chainID, filt)

	buildOpts := snapshot.BuildOptions{
		ChainID:    *chainID,
		Filter:     filt,
		Now:        now,
		Composite:  scoring.DefaultCompositeWeights(),
		Compliance: scoring.DefaultComplianceWeights(),
	}
	data, err := snapshot.BuildFromSource(ctx, src, buildOpts)
	if err != nil {
		log.Fatalf("build snapshot: %v", err)
	}

	// Write the snapshot DB.
	w := snapshot.NewWriter(mc, snapID)
	if err := w.Write(ctx, data); err != nil {
		log.Fatalf("write snapshot: %v", err)
	}

	// Insert index metadata.
	idxRepo := snapshot.NewIndexRepository(mc.Database(snapshot.IndexDatabaseName))
	meta := snapshot.SnapshotMeta{
		ID:        snapID,
		Label:     *label,
		CreatedAt: now.Unix(),
		ChainID:   *chainID,
		Filters:   filt,
		Stats: snapshot.SnapshotStats{
			Agents:    int64(len(data.Agents)),
			Feedbacks: int64(len(data.Feedbacks)),
			Edges:     int64(len(data.Edges)),
		},
		SourceDB:     cfg.AnalyzedDatabase,
		GitCommitSHA: readBuildVCS(),
	}
	// Latest source event timestamp (for documenting "when in time" the snapshot captured).
	for _, fb := range data.Feedbacks {
		if fb.Timestamp > meta.SourceLastEventTs {
			meta.SourceLastEventTs = fb.Timestamp
		}
	}
	if err := idxRepo.Insert(ctx, meta); err != nil {
		log.Fatalf("insert index meta: %v", err)
	}

	fmt.Printf("Created snapshot %s\n", snapID)
	fmt.Printf("  label:     %s\n", *label)
	fmt.Printf("  agents:    %d\n", meta.Stats.Agents)
	fmt.Printf("  feedbacks: %d\n", meta.Stats.Feedbacks)
	fmt.Printf("  edges:     %d\n", meta.Stats.Edges)
	fmt.Printf("  database:  %s\n", snapshot.SnapshotDBName(snapID))
}

// readBuildVCS returns the embedded VCS commit SHA, or "" if unavailable.
func readBuildVCS() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	return ""
}
