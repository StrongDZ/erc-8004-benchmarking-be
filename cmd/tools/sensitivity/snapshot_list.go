package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

func snapshotList(args []string) {
	fs := flag.NewFlagSet("snapshot list", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
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

	repo := snapshot.NewIndexRepository(mc.Database(snapshot.IndexDatabaseName))
	snaps, err := repo.List(ctx)
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	if len(snaps) == 0 {
		fmt.Println("(no snapshots)")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tLABEL\tCHAIN\tAGENTS\tFEEDBACKS\tEDGES")
	for _, s := range snaps {
		created := time.Unix(s.CreatedAt, 0).Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%d\n",
			s.ID, created, s.Label, s.ChainID, s.Stats.Agents, s.Stats.Feedbacks, s.Stats.Edges)
	}
	w.Flush()
}
