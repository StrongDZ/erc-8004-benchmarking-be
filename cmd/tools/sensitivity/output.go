package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"erc-8004-benchmarking-be/internal/sensitivity/metrics"
	"erc-8004-benchmarking-be/internal/sensitivity/pipeline"
	"erc-8004-benchmarking-be/internal/sensitivity/runner"
)

// outputDir returns the per-snapshot per-cluster output directory.
func outputDir(snapshotID, cluster string) string {
	return filepath.Join("scripts", "sensitivity", "output", snapshotID, "cluster_"+cluster)
}

// writeOATCSV writes one row per (param, value) with rank stability + dist stats vs baseline.
func writeOATCSV(path string, results []runner.RunResult, baseline map[string]float64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"param", "value", "spearman", "kendall", "top10_churn",
		"mean", "std", "skew", "ks_d",
	}); err != nil {
		return err
	}
	baseSlice := mapValuesToSlice(baseline)
	for _, r := range results {
		desc := metrics.Describe(mapValuesToSlice(r.Scores))
		row := []string{
			r.ParamName,
			ftoa(r.ParamValue),
			ftoa(metrics.Spearman(baseline, r.Scores)),
			ftoa(metrics.Kendall(baseline, r.Scores)),
			ftoa(metrics.TopKChurn(baseline, r.Scores, 10)),
			ftoa(desc.Mean),
			ftoa(desc.Std),
			ftoa(desc.Skew),
			ftoa(metrics.KS2Sample(baseSlice, mapValuesToSlice(r.Scores))),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// writeTornadoCSV writes one row per param with |Δ_low|, |Δ_high|.
func writeTornadoCSV(path string, entries []runner.TornadoEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"param", "delta_low", "delta_high"}); err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Write([]string{e.Param, ftoa(e.DeltaLow), ftoa(e.DeltaHigh)}); err != nil {
			return err
		}
	}
	return nil
}

// writeSobolCSV writes one row per param with first-order S1 and total-order ST.
func writeSobolCSV(path string, entries []runner.SobolEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"param", "s1", "st"}); err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Write([]string{e.Param, ftoa(e.FirstOrder), ftoa(e.TotalOrder)}); err != nil {
			return err
		}
	}
	return nil
}

// writeGridCSV writes one row per combination with all param values + scalar metrics.
func writeGridCSV(path string, results []runner.RunResult, paramOrder []string, baseline map[string]float64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"combo_id"}
	header = append(header, paramOrder...)
	header = append(header, "spearman", "kendall", "mean", "std")
	if err := w.Write(header); err != nil {
		return err
	}
	for i, r := range results {
		desc := metrics.Describe(mapValuesToSlice(r.Scores))
		row := []string{strconv.Itoa(i)}
		for _, p := range paramOrder {
			row = append(row, ftoa(r.Config[p]))
		}
		row = append(row,
			ftoa(metrics.Spearman(baseline, r.Scores)),
			ftoa(metrics.Kendall(baseline, r.Scores)),
			ftoa(desc.Mean),
			ftoa(desc.Std),
		)
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// writeSimplexCSV writes one row per Dirichlet sample with the 4 sampled weights
// plus rank-stability + distribution metrics versus the baseline (default-weight)
// composite scores.
func writeSimplexCSV(path string, results []runner.RunResult, baseline map[string]float64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"sample_id", "w_reputation", "w_services", "w_publisher", "w_compliance",
		"spearman", "kendall", "mean", "std",
	}); err != nil {
		return err
	}
	for i, r := range results {
		desc := metrics.Describe(mapValuesToSlice(r.Scores))
		row := []string{
			strconv.Itoa(i),
			ftoa(r.Config["WReputation"]),
			ftoa(r.Config["WServices"]),
			ftoa(r.Config["WPublisher"]),
			ftoa(r.Config["WCompliance"]),
			ftoa(metrics.Spearman(baseline, r.Scores)),
			ftoa(metrics.Kendall(baseline, r.Scores)),
			ftoa(desc.Mean),
			ftoa(desc.Std),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// writeConvergenceCSV writes Alpha → #iterations for the Cluster-D convergence study.
func writeConvergenceCSV(path string, points []pipeline.ConvergencePoint) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"alpha", "iterations"}); err != nil {
		return err
	}
	for _, p := range points {
		if err := w.Write([]string{ftoa(p.Alpha), strconv.Itoa(p.Iterations)}); err != nil {
			return err
		}
	}
	return nil
}

// writeManifest writes a JSON manifest.json with run metadata.
// readBuildVCS() is defined in snapshot_create.go (same package).
func writeManifest(outDir, snapshotID, cluster, method string, seed int64, config map[string]any) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	man := map[string]any{
		"snapshotId":   snapshotID,
		"cluster":      cluster,
		"method":       method,
		"seed":         seed,
		"runTimestamp": time.Now().UTC().Format(time.RFC3339),
		"gitCommitSha": readBuildVCS(),
		"config":       config,
	}
	f, err := os.Create(filepath.Join(outDir, fmt.Sprintf("manifest_%s.json", method)))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(man)
}

func ftoa(v float64) string {
	return strconv.FormatFloat(v, 'g', 10, 64)
}

func mapValuesToSlice(m map[string]float64) []float64 {
	out := make([]float64, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
