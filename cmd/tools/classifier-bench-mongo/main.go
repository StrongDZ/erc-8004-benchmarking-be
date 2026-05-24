package main

// classifier-bench-mongo — Benchmark self-hosted LLM models against the
// rule-based classifier on real feedback_history records. Outputs a CSV
// (with every prompt-input column for off-line inspection) and per-model
// P/R/F1 + confusion matrices to stdout.
//
// Two benchmarks are supported:
//
//   Bench 1 (--source=category): rule-as-gold over the 4 deterministic
//     categories {service_feedback, app_specific, config_feedback, junk}.
//     Samples N records per category from classification.rule.category,
//     runs each LLM via the AI service, and reports F1 vs the rule label.
//     The "others" category is intentionally excluded — those records need
//     a human-labelled gold set (Bench 2).
//
//   Bench 2 (--source=others / --source=labelled): all records with
//     classification.rule.category = "others" (empty tags or unmatched
//     patterns) form a candidate pool. The labelling workflow is:
//        1. --source=others --export-only --total=200 → dump CSV with
//           every prompt-input column plus an empty gold_category for human
//           pre-labelling (or AI pre-labelling, then human verification).
//        2. fill gold_category in the exported CSV.
//        3. --source=labelled --labels=... → run every LLM against the
//           hand-labelled gold set, print macro-F1, confusion matrix.
//
// Usage examples:
//
//   # Bench 1 — rule-as-gold, 5 categories
//   go run ./cmd/tools/classifier-bench-mongo \
//       --source=category --per-bucket=200 \
//       --out=./scripts/output/bench1_rule_vs_llm.csv
//
//   # Bench 2 step 1 — export 200 stratified others-records for labelling
//   go run ./cmd/tools/classifier-bench-mongo \
//       --source=others --export-only \
//       --per-cluster=3 --total=200 \
//       --out=./scripts/labelled/others_gold_v1.csv
//
//   # Bench 2 step 2 — labelled bench
//   go run ./cmd/tools/classifier-bench-mongo \
//       --source=labelled --labels=./scripts/labelled/others_gold_v1.csv \
//       --out=./scripts/output/bench2_others_vs_gold.csv
//
// Flags shared by all modes:
//
//   --out         CSV output path (default ./scripts/output/classifier_eval_<ts>.csv)
//   --timeout     AI service request timeout seconds (default 120)
//   --base-url    override AI_SERVICE_URL (default http://localhost:8000)
//   --skip-llm    skip every AI service call (rule-only run, useful for export-only)
//   --seed        RNG seed for stratified sampling (default 42)
//   --llm-concurrency  max in-flight HTTP calls per model pass (default 1).
//             Rows are independent; this overlaps work across different agents
//             and feedbacks. Use 1 to restore the legacy strictly-serial loop.
//             NOTE: the Python service serialises Ollama calls per loaded model
//             anyway, so values >1 yield diminishing returns.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"erc-8004-benchmarking-be/internal/config"
	classifier "erc-8004-benchmarking-be/internal/domain/classifier"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// modelSpec describes one LLM under test (driven via the Python AI service).
// The prompt version is owned by the Python service (default v4_xml in
// shared/prompts.py); we only pass the model name through.
type modelSpec struct {
	csvKey string // safe key used in CSV column names
	model  string // ollama model name, e.g. "qwen2.5:3b"
}

// modelsUnderTest is the panel of LLMs the bench sweeps in one run. Models
// execute sequentially so Ollama only loads one set of weights into VRAM at a
// time (no thrashing). Order matters: lighter models first so a partial run
// still produces useful baseline rows.
//
// Pull commands (run once, native Ollama on the host, NOT in Docker so the
// Metal GPU is available):
//
//	ollama pull qwen2.5:3b
//	ollama pull qwen2.5:7b-instruct
//	ollama pull qwen3:8b
//	ollama pull qwen2.5:14b-instruct
//	ollama pull phi4
//	ollama pull gemma3:12b
var modelsUnderTest = []modelSpec{
	{csvKey: "qwen2_5_3b", model: "qwen2.5:3b"},
	{csvKey: "qwen2_5_7b", model: "qwen2.5:7b-instruct"},
	{csvKey: "qwen3_8b", model: "qwen3:8b"},
	{csvKey: "gemma3_12b", model: "gemma3:12b"},
	{csvKey: "qwen2_5_14b", model: "qwen2.5:14b-instruct"},
	{csvKey: "phi4_14b", model: "phi4"},
}

// agentMeta is the slice of an AgentDocument we surface to the prompt builder.
// Cached per-agent so repeated samples for the same agent only touch Mongo once.
type agentMeta struct {
	description string
	services    string
	tags        []string
}

// rowAcc carries the per-feedback state through the bench: source record,
// rule output, and one LLMResult+latency per model under test.
type rowAcc struct {
	fb    feedbackrepo.FeedbackRecord
	rule  classifier.Result
	llm   map[string]classifier.LLMResult
	llmMs map[string]int64
}

func main() {
	var (
		source     = flag.String("source", "category", "category | others | labelled")
		perBucket  = flag.Int("per-bucket", 200, "[category mode] samples per non-others category bucket")
		perCluster = flag.Int("per-cluster", 3, "[others mode] max samples per tag-cluster")
		total      = flag.Int("total", 200, "[others mode] total samples ceiling")
		labels     = flag.String("labels", "", "[labelled mode] path to gold-label CSV")
		exportOnly = flag.Bool("export-only", false, "[others mode] write label template CSV and exit (no LLM calls)")
		seed       = flag.Int64("seed", 42, "RNG seed for stratified sampling")

		limit   = flag.Int("limit", 0, "hard cap on total samples after sampling (0 = no cap)")
		outPath = flag.String("out", "", "CSV output path")
		timeout = flag.Int("timeout", 120, "AI service request timeout seconds")
		baseURL = flag.String("base-url", "", "override AI_SERVICE_URL (default http://localhost:8000)")
		skipLLM = flag.Bool("skip-llm", false, "skip every AI service call (rule-only run)")

		llmConc = flag.Int("llm-concurrency", 1, "max concurrent LLM classify calls per model pass (1 = serial)")
	)
	flag.Parse()

	srcMode := strings.ToLower(strings.TrimSpace(*source))
	switch srcMode {
	case "category", "others", "labelled":
	default:
		log.Fatalf("invalid --source=%s (want category|others|labelled)", srcMode)
	}
	if srcMode == "labelled" && strings.TrimSpace(*labels) == "" {
		log.Fatalf("--source=labelled requires --labels=path/to/gold.csv")
	}
	if *exportOnly && srcMode != "others" {
		log.Fatalf("--export-only only supported with --source=others")
	}

	if *outPath == "" {
		stamp := time.Now().Format("20060102_150405")
		name := fmt.Sprintf("classifier_eval_%s_%s.csv", srcMode, stamp)
		*outPath = filepath.Join("scripts", "output", name)
	}
	if envURL := os.Getenv("AI_SERVICE_URL"); envURL != "" && *baseURL == "" {
		*baseURL = envURL
	}
	if *baseURL == "" {
		*baseURL = "http://localhost:8000"
	}

	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	analyzedDB := mc.Database(cfg.AnalyzedDatabase)
	feedbacks := feedbackrepo.NewRepository(analyzedDB, cfg.FeedbackHistColl)
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl, cfg.ScoreStatsColl)

	// ── 1. Pull samples per mode ──
	var samples []feedbackrepo.FeedbackRecord
	var goldLabels map[string]GoldLabel
	switch srcMode {
	case "category":
		buckets := []string{"junk", "service_feedback", "config_feedback", "app_specific"}
		for _, cat := range buckets {
			docs, err := sampleByCategory(ctx, feedbacks, cat, *perBucket)
			if err != nil {
				log.Fatalf("sample %s: %v", cat, err)
			}
			log.Printf("sampled %d / requested %d for category=%s", len(docs), *perBucket, cat)
			samples = append(samples, docs...)
		}

	case "others":
		docs, err := sampleOthersStratified(ctx, feedbacks, *perCluster, *total, *seed)
		if err != nil {
			log.Fatalf("sample others: %v", err)
		}
		log.Printf("stratified others sample: %d records across tag clusters (perCluster=%d, budget=%d)",
			len(docs), *perCluster, *total)
		samples = docs

	case "labelled":
		gold, err := loadGoldLabels(*labels)
		if err != nil {
			log.Fatalf("load gold labels: %v", err)
		}
		if len(gold) == 0 {
			log.Fatalf("no gold-labelled rows found in %s (gold_category column empty?)", *labels)
		}
		ids := make([]string, 0, len(gold))
		for id := range gold {
			ids = append(ids, id)
		}
		docs, err := feedbacks.FindByIDs(ctx, ids)
		if err != nil {
			log.Fatalf("fetch labelled feedbacks: %v", err)
		}
		samples = docs
		goldLabels = gold
		log.Printf("loaded %d gold labels, matched %d in DB", len(gold), len(samples))
	}

	if *limit > 0 && len(samples) > *limit {
		samples = samples[:*limit]
	}
	log.Printf("total samples to process: %d", len(samples))

	// ── 2. Pre-cache agent docs ──
	agentCache := buildAgentCache(ctx, agents, samples)

	// ── Export-only short circuit: write label template and exit ──
	if *exportOnly {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
			log.Fatalf("mkdir output: %v", err)
		}
		if err := exportOthersForLabelling(*outPath, samples, agentCache); err != nil {
			log.Fatalf("export labels: %v", err)
		}
		log.Printf("label template written: %s (%d rows) — fill gold_category, then re-run with --source=labelled",
			*outPath, len(samples))
		return
	}

	// ── 3. Build AI service clients (one per model — only the model field differs) ──
	aiClients := make([]*classifier.AIClient, len(modelsUnderTest))
	for i, m := range modelsUnderTest {
		aiClients[i] = classifier.NewAIClient(classifier.AIClientConfig{
			BaseURL:        *baseURL,
			Model:          m.model,
			TimeoutSeconds: *timeout,
		})
	}

	if !*skipLLM && len(aiClients) > 0 {
		hcCtx, hcCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := aiClients[0].HealthCheck(hcCtx); err != nil {
			hcCancel()
			log.Fatalf("AI service health check failed: %v", err)
		}
		hcCancel()
	}

	// ── 4. Run all models, accumulating into per-feedback rows ──
	rows := make([]*rowAcc, len(samples))
	for i, s := range samples {
		rows[i] = &rowAcc{
			fb:    s,
			rule:  classifier.Classify(s.Tag1, s.Tag2),
			llm:   make(map[string]classifier.LLMResult, len(modelsUnderTest)),
			llmMs: make(map[string]int64, len(modelsUnderTest)),
		}
	}

	if !*skipLLM {
		conc := *llmConc
		if conc < 1 {
			conc = 1
		}
		for mi, m := range modelsUnderTest {
			client := aiClients[mi]
			log.Printf("=== running model %s over %d samples (llm-concurrency=%d) ===",
				m.model, len(rows), conc)
			startPass := time.Now()

			sem := make(chan struct{}, conc)
			g, gctx := errgroup.WithContext(ctx)
			var done atomic.Int64

			for _, row := range rows {
				row := row
				g.Go(func() error {
					sem <- struct{}{}
					defer func() { <-sem }()

					fb := row.fb
					am := agentCache[fb.ID]
					off := offchainSummaryFromMap(fb.FeedbackParsed)
					valueNorm := classifier.NormalizeValue(fb.Value, int(fb.ValueDecimals))

					scale := ""
					if real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals)); ok {
						scale = classifier.AssignTier(real)
					}

					callCtx, cancel := context.WithTimeout(gctx, time.Duration(*timeout+5)*time.Second)
					t0 := time.Now()
					res := client.Classify(callCtx, fb.Tag1, fb.Tag2, valueNorm, off, am.description, am.services, am.tags, fb.Endpoint, scale)
					elapsed := time.Since(t0)
					cancel()

					row.llm[m.csvKey] = res
					row.llmMs[m.csvKey] = elapsed.Milliseconds()

					n := done.Add(1)
					if int(n)%50 == 0 {
						log.Printf("  [%s] %d/%d done — last=%dms", m.csvKey, n, len(rows), elapsed.Milliseconds())
					}
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				log.Fatalf("model %s: %v", m.model, err)
			}
			log.Printf("=== %s done in %s ===", m.model, time.Since(startPass).Truncate(time.Second))
		}
	}

	// ── 5. Write CSV ──
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir output: %v", err)
	}
	f, err := os.Create(*outPath)
	if err != nil {
		log.Fatalf("create csv: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header layout: every prompt-input column (shared with the labelling
	// template) + rule_category + has_empty_tags + optional gold + per-model
	// outputs. In labelled mode agreement columns compare each LLM to
	// gold_category (not rule_category — rule is always "others" here).
	// has_empty_tags flags records with empty tag1 AND tag2 — useful for
	// splitting F1 on the edge case where the LLM has only offchain to work with.
	header := append([]string{}, promptRowHeader...)
	header = append(header, "rule_category", "has_empty_tags")
	if srcMode == "labelled" {
		header = append(header, "gold_category", "gold_notes")
	}
	for _, m := range modelsUnderTest {
		header = append(header,
			m.csvKey+"_category",
			m.csvKey+"_confidence",
			m.csvKey+"_reason",
			m.csvKey+"_latency_ms",
			m.csvKey+"_source",
		)
		if srcMode == "labelled" {
			header = append(header, "agreement_"+m.csvKey+"_vs_gold")
		} else {
			header = append(header, "agreement_"+m.csvKey+"_vs_rule")
		}
	}
	if err := w.Write(header); err != nil {
		log.Fatalf("write header: %v", err)
	}

	for _, row := range rows {
		fb := row.fb
		am := agentCache[fb.ID]
		hasEmptyTags := strings.TrimSpace(fb.Tag1) == "" && strings.TrimSpace(fb.Tag2) == ""

		rec := append(promptRow(fb, am),
			string(row.rule.Category),
			strconv.FormatBool(hasEmptyTags),
		)
		if srcMode == "labelled" {
			gl := goldLabels[fb.ID]
			rec = append(rec, gl.GoldCategory, gl.Notes)
		}
		for _, m := range modelsUnderTest {
			res := row.llm[m.csvKey]
			rec = append(rec,
				string(res.Category),
				strconv.FormatFloat(res.Confidence, 'f', 2, 64),
				truncateRunes(res.Reason, 240),
				strconv.FormatInt(row.llmMs[m.csvKey], 10),
				res.Source,
			)
			if srcMode == "labelled" {
				gl := goldLabels[fb.ID]
				rec = append(rec, strconv.FormatBool(
					normCategory(string(res.Category)) == normCategory(gl.GoldCategory)))
			} else {
				rec = append(rec, strconv.FormatBool(
					normCategory(string(res.Category)) == normCategory(string(row.rule.Category))))
			}
		}
		if err := w.Write(rec); err != nil {
			log.Fatalf("write row: %v", err)
		}
	}

	log.Printf("CSV written: %s (%d rows)", *outPath, len(rows))

	// ── 6. Metrics ──
	// Rule vs gold is independent of LLM — always print when gold exists.
	if srcMode == "labelled" {
		gold := make([]string, len(rows))
		pred := make([]string, len(rows))
		for i, r := range rows {
			gold[i] = goldLabels[r.fb.ID].GoldCategory
			pred[i] = string(r.rule.Category)
		}
		fmt.Print(FormatReport("rule baseline vs gold", ComputeReport(gold, pred)))
	}
	// LLM-vs-gold reports require the LLM pass to have run.
	if !*skipLLM {
		if srcMode == "category" {
			// Rule output treated as gold proxy — measures rule-mimicking accuracy.
			printAllModelReports("rule (proxy gold)", rows, modelsUnderTest, func(r *rowAcc) string {
				return string(r.rule.Category)
			})
		}
		if srcMode == "labelled" {
			printAllModelReports("gold labels", rows, modelsUnderTest, func(r *rowAcc) string {
				return goldLabels[r.fb.ID].GoldCategory
			})
		}
	}
	if srcMode == "others" {
		log.Printf("others mode: no gold labels, F1 not computed. " +
			"Re-run with --source=others --export-only to dump a labelling " +
			"template, fill gold_category, then run --source=labelled.")
	}
}

// printAllModelReports computes a Report per model using goldFn to extract
// the per-row gold label and prints each one to stdout.
func printAllModelReports(
	source string,
	rows []*rowAcc,
	models []modelSpec,
	goldFn func(*rowAcc) string,
) {
	for _, m := range models {
		gold := make([]string, len(rows))
		pred := make([]string, len(rows))
		for i, r := range rows {
			gold[i] = goldFn(r)
			pred[i] = string(r.llm[m.csvKey].Category)
		}
		fmt.Print(FormatReport(fmt.Sprintf("%s vs %s", m.model, source), ComputeReport(gold, pred)))
	}
}

// buildAgentCache fetches agent docs once per (chainId, agentId) pair seen in samples
// and returns a map keyed by feedback _id for O(1) lookup during the bench loop.
func buildAgentCache(ctx context.Context, agents *agentrepo.Repository, samples []feedbackrepo.FeedbackRecord) map[string]agentMeta {
	cache := make(map[string]agentMeta, len(samples))
	keyToMeta := make(map[string]agentMeta)
	for _, s := range samples {
		k := fmt.Sprintf("%d:%s", s.ChainID, s.AgentID)
		if meta, seen := keyToMeta[k]; seen {
			cache[s.ID] = meta
			continue
		}
		doc, err := agents.FindByAgentID(ctx, s.ChainID, s.AgentID)
		if err != nil || doc == nil {
			keyToMeta[k] = agentMeta{}
			cache[s.ID] = agentMeta{}
			continue
		}
		meta := agentMeta{
			description: doc.Description,
			tags:        doc.Tags,
		}
		if len(doc.Services) > 0 {
			if b, err := json.Marshal(doc.Services); err == nil {
				meta.services = string(b)
			}
		}
		keyToMeta[k] = meta
		cache[s.ID] = meta
	}
	return cache
}

// offchainSummaryFromMap serializes feedbackParsed to a JSON string that the
// prompt builder (BuildUserMessage* / SummarizeJSONLikeSchema) will further
// compress into a compact schema-key=value summary. Truncation here is kept
// intentionally generous (2000 bytes) so the schema summarizer receives a
// valid, complete JSON object rather than a truncated fragment that fails
// to parse. The summarizer's own maxRunes cap controls final token budget.
func offchainSummaryFromMap(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	s := string(b)
	if len(s) > 2000 {
		s = s[:2000]
	}
	return s
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}
