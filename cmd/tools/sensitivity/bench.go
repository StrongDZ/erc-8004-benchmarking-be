package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/sensitivity/metrics"
	"erc-8004-benchmarking-be/internal/sensitivity/pipeline"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
	"erc-8004-benchmarking-be/internal/sensitivity/snapshot"
)

// meanScalar reduces per-agent scores to their mean — the default tornado/sobol metric.
func meanScalar(s map[string]float64) float64 {
	return metrics.Describe(mapValuesToSlice(s)).Mean
}

func benchDispatch(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cluster := fs.String("cluster", "", "A | B | C | D (required)")
	method := fs.String("method", "", "oat | tornado | sobol | grid | simplex (required)")
	snapshotID := fs.String("snapshot", "", "snapshot ID from `snapshot list` (required)")
	oatPoints := fs.Int("oat-points", 11, "[oat] points per param")
	sobolN := fs.Int("sobol-n", 1024, "[sobol] base sample count")
	sobolSeed := fs.Int64("sobol-seed", 42, "[sobol] RNG seed")
	gridLevels := fs.Int("grid-levels", 5, "[grid] levels per top-3 param")
	simplexN := fs.Int("simplex-n", 200, "[simplex] Dirichlet sample count (cluster B only)")
	simplexSeed := fs.Int64("simplex-seed", 42, "[simplex] RNG seed (cluster B only)")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *cluster == "" || *method == "" || *snapshotID == "" {
		fmt.Fprintln(os.Stderr, "--cluster, --method, --snapshot are all required")
		os.Exit(1)
	}
	cl := strings.ToUpper(*cluster)
	if cl != "A" && cl != "B" && cl != "C" && cl != "D" {
		log.Fatalf("invalid --cluster=%s", *cluster)
	}

	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mongo: %v", err)
	}
	defer mc.Disconnect(ctx)

	r := snapshot.NewReader(mc, *snapshotID)
	agents, err := r.ListAgents(ctx)
	if err != nil {
		log.Fatalf("read agents: %v", err)
	}
	feedbacks, err := r.ListFeedbacks(ctx)
	if err != nil {
		log.Fatalf("read feedbacks: %v", err)
	}
	edges, err := r.ListEdges(ctx)
	if err != nil {
		log.Fatalf("read edges: %v", err)
	}
	baselineScores, err := r.ListBaseline(ctx)
	if err != nil {
		log.Fatalf("read baseline: %v", err)
	}
	data := snapshot.SnapshotData{Agents: agents, Feedbacks: feedbacks, Edges: edges, Baseline: baselineScores}
	nowUnix := time.Now().Unix()

	// Resolve cluster pipeline.
	var (
		specs      []runner.ParamSpec
		recompute  runner.RecomputeFn
		paramOrder []string
	)
	switch cl {
	case "A":
		specs = pipeline.ClusterAParamSpecs()
		recompute = pipeline.ClusterARecompute(data, nowUnix)
	case "B":
		specs = pipeline.ClusterBParamSpecs()
		recompute = pipeline.ClusterBRecompute(data)
	case "C":
		specs = pipeline.ClusterCParamSpecs()
		recompute = pipeline.ClusterCRecompute(data)
	default:
		log.Fatalf("cluster %s pipeline not implemented yet — see Plan-%s", cl, cl)
	}
	for _, p := range specs {
		paramOrder = append(paramOrder, p.Name)
	}

	// Compute baseline once.
	baseCfg := map[string]float64{}
	for _, p := range specs {
		baseCfg[p.Name] = p.Default
	}
	baseline := recompute(baseCfg)

	outDir := outputDir(*snapshotID, cl)

	switch *method {
	case "oat":
		results := runner.OATSweep(specs, *oatPoints, recompute)
		if err := writeOATCSV(filepath.Join(outDir, "oat.csv"), results, baseline); err != nil {
			log.Fatalf("write oat csv: %v", err)
		}
		_ = writeManifest(outDir, *snapshotID, cl, "oat", 0, map[string]any{"oatPoints": *oatPoints})
		fmt.Printf("OAT: %d results → %s/oat.csv\n", len(results), outDir)

	case "tornado":
		entries := runner.Tornado(specs, recompute, meanScalar)
		if err := writeTornadoCSV(filepath.Join(outDir, "tornado.csv"), entries); err != nil {
			log.Fatalf("write tornado csv: %v", err)
		}
		_ = writeManifest(outDir, *snapshotID, cl, "tornado", 0, nil)
		fmt.Printf("Tornado: %d entries → %s/tornado.csv\n", len(entries), outDir)

	case "sobol":
		entries := runner.Sobol(specs, *sobolN, *sobolSeed, recompute, meanScalar)
		if err := writeSobolCSV(filepath.Join(outDir, "sobol.csv"), entries); err != nil {
			log.Fatalf("write sobol csv: %v", err)
		}
		_ = writeManifest(outDir, *snapshotID, cl, "sobol", *sobolSeed,
			map[string]any{"sobolN": *sobolN})
		fmt.Printf("Sobol: %d entries → %s/sobol.csv\n", len(entries), outDir)

	case "grid":
		// Top-3 by absolute |Δ_high| + |Δ_low| from a quick tornado pass.
		tor := runner.Tornado(specs, recompute, meanScalar)
		topNames := map[string]struct{}{}
		for i := 0; i < 3 && i < len(tor); i++ {
			topNames[tor[i].Param] = struct{}{}
		}
		var topSpecs []runner.ParamSpec
		var topOrder []string
		for _, p := range specs {
			if _, ok := topNames[p.Name]; ok {
				topSpecs = append(topSpecs, p)
				topOrder = append(topOrder, p.Name)
			}
		}
		// Wrap recompute to overlay top-spec values onto baseline config.
		wrapped := func(cfg map[string]float64) map[string]float64 {
			full := map[string]float64{}
			for k, v := range baseCfg {
				full[k] = v
			}
			for k, v := range cfg {
				full[k] = v
			}
			return recompute(full)
		}
		results := runner.Grid(topSpecs, *gridLevels, wrapped)
		if err := writeGridCSV(filepath.Join(outDir, "grid.csv"), results, topOrder, baseline); err != nil {
			log.Fatalf("write grid csv: %v", err)
		}
		_ = writeManifest(outDir, *snapshotID, cl, "grid", 0,
			map[string]any{"gridLevels": *gridLevels, "topParams": topOrder})
		fmt.Printf("Grid: %d combos on %v → %s/grid.csv\n", len(results), topOrder, outDir)

	case "simplex":
		if cl != "B" {
			log.Fatalf("--method=simplex only valid for cluster B")
		}
		results := pipeline.ClusterBSimplexRunner(data, *simplexN, *simplexSeed)
		if err := writeSimplexCSV(filepath.Join(outDir, "simplex.csv"), results, baseline); err != nil {
			log.Fatalf("write simplex csv: %v", err)
		}
		_ = writeManifest(outDir, *snapshotID, cl, "simplex", *simplexSeed,
			map[string]any{"simplexN": *simplexN})
		fmt.Printf("Simplex: %d samples → %s/simplex.csv\n", len(results), outDir)

	default:
		log.Fatalf("invalid --method=%s (want oat|tornado|sobol|grid|simplex)", *method)
	}
}
