package main

// classifier-bench-mongo — Benchmark 3 self-hosted LLM models against the
// rule-based classifier on real feedback_history records. Outputs a CSV for
// off-line analysis.
//
// Usage:
//
//	cd erc-8004-benchmarking-be
//	LLM_BASE_URL=http://localhost:11434 \
//   go run ./cmd/tools/classifier-bench-mongo \
//     --per-bucket=200 --out=./scripts/output/classifier_eval.csv
//
// Flags:
//
//	--per-bucket int   samples per category bucket (default 200)
//	--limit int        hard cap on total samples after sampling (0 = no cap)
//	--out string       CSV output path (default ./scripts/output/classifier_eval_<ts>.csv)
//	--timeout int      LLM request timeout seconds (default 120)
//	--mode string      ollama | llamacpp (default ollama)
//	--base-url string  override LLM_BASE_URL (default http://localhost:11434)
//
// Buckets sampled (excluding "others" per request):
//
//	spam, noise, service_feedback, config_feedback, app_specific
//
// Models / prompts run sequentially (one full pass per model) to keep Ollama
// from thrashing VRAM on a 4 GB GPU. Within a pass, calls are sequential to
// avoid contending with the model's CPU/GPU split.

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
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/config"
	classifier "erc-8004-benchmarking-be/internal/domain/classifier"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// modelSpec describes one LLM under test.
type modelSpec struct {
	csvKey        string // safe key used in CSV column names
	model         string // ollama model name, e.g. "qwen3:1.7b"
	promptVersion classifier.PromptVersion
}

var modelsUnderTest = []modelSpec{
	// {csvKey: "qwen3_1_7b", model: "qwen3:1.7b", promptVersion: classifier.PromptVersionQwen3Small},
	// {csvKey: "qwen3_8b", model: "qwen3:8b", promptVersion: classifier.PromptVersionFull6Cat},
	{csvKey: "qwen2_5_3b", model: "qwen2.5:3b", promptVersion: classifier.PromptVersionFull6CatV2},
}

func main() {
	var (
		perBucket = flag.Int("per-bucket", 200, "samples per category bucket")
		limit     = flag.Int("limit", 0, "hard cap on total samples (0 = no cap)")
		outPath   = flag.String("out", "", "CSV output path")
		timeout   = flag.Int("timeout", 120, "LLM request timeout seconds")
		mode      = flag.String("mode", "ollama", "ollama | llamacpp")
		baseURL   = flag.String("base-url", "", "override LLM_BASE_URL")
	)
	flag.Parse()

	if *outPath == "" {
		*outPath = filepath.Join("scripts", "output",
			fmt.Sprintf("classifier_eval_%s.csv", time.Now().Format("20060102_150405")))
	}
	if envURL := os.Getenv("LLM_BASE_URL"); envURL != "" && *baseURL == "" {
		*baseURL = envURL
	}
	if *baseURL == "" {
		*baseURL = "http://localhost:11434"
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
	agents := agentrepo.NewRepository(analyzedDB, cfg.AgentsColl)

	// ── 1. Sample feedback per bucket ──
	buckets := []string{"spam", "noise", "service_feedback", "config_feedback", "app_specific"}
	samples := make([]feedbackrepo.FeedbackRecord, 0, *perBucket*len(buckets))
	for _, cat := range buckets {
		docs, err := sampleByCategory(ctx, feedbacks, cat, *perBucket)
		if err != nil {
			log.Fatalf("sample %s: %v", cat, err)
		}
		log.Printf("sampled %d / requested %d for category=%s", len(docs), *perBucket, cat)
		samples = append(samples, docs...)
	}
	if *limit > 0 && len(samples) > *limit {
		samples = samples[:*limit]
	}
	log.Printf("total samples: %d", len(samples))

	// ── 2. Pre-cache agent docs to avoid repeated lookups ──
	agentCache := buildAgentCache(ctx, agents, samples)

	// ── 3. Build LLM clients ──
	llmMode := classifier.ModeOllama
	if strings.EqualFold(*mode, "llamacpp") {
		llmMode = classifier.ModeLlamaCpp
	}
	llmClients := make([]*classifier.LLMClient, len(modelsUnderTest))
	for i, m := range modelsUnderTest {
		llmClients[i] = classifier.NewLLMClient(classifier.LLMClientConfig{
			BaseURL:        *baseURL,
			Model:          m.model,
			Mode:           llmMode,
			TimeoutSeconds: *timeout,
			PromptVersion:  m.promptVersion,
		})
	}

	// Health check each model up-front so we fail fast.
	hcCtx, hcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer hcCancel()
	if err := llmClients[0].HealthCheck(hcCtx); err != nil {
		log.Fatalf("LLM health check failed: %v", err)
	}

	// ── 4. Run all models, accumulating into a per-feedback map ──
	type rowAcc struct {
		fb    feedbackrepo.FeedbackRecord
		rule  classifier.Result
		llm   map[string]classifier.LLMResult // csvKey → result
		llmMs map[string]int64
	}
	rows := make([]*rowAcc, len(samples))
	for i, s := range samples {
		rows[i] = &rowAcc{
			fb:    s,
			rule:  classifier.Classify(s.Tag1, s.Tag2),
			llm:   make(map[string]classifier.LLMResult, len(modelsUnderTest)),
			llmMs: make(map[string]int64, len(modelsUnderTest)),
		}
	}

	for mi, m := range modelsUnderTest {
		client := llmClients[mi]
		log.Printf("=== running model %s (prompt=%s) over %d samples ===",
			m.model, m.promptVersion, len(rows))
		startPass := time.Now()
		for ri, row := range rows {
			fb := row.fb
			ag := agentCache[fb.ID]
			desc, services, agTags := agentMetaStrings(ag)
			off := offchainSummary(fb)
			valueNorm := classifier.NormalizeValue(fb.Value, int(fb.ValueDecimals))

			callCtx, cancel := context.WithTimeout(ctx, time.Duration(*timeout+5)*time.Second)
			t0 := time.Now()
			res := client.Classify(callCtx, fb.Tag1, fb.Tag2, valueNorm, off, desc, services, agTags)
			elapsed := time.Since(t0)
			cancel()

			row.llm[m.csvKey] = res
			row.llmMs[m.csvKey] = elapsed.Milliseconds()

			if (ri+1)%50 == 0 {
				log.Printf("  [%s] %d/%d done — last=%dms", m.csvKey, ri+1, len(rows), elapsed.Milliseconds())
			}
		}
		log.Printf("=== %s done in %s ===", m.model, time.Since(startPass).Truncate(time.Second))
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

	header := []string{
		"feedback_id", "chain_id", "agent_id",
		"tag1", "tag2", "value_norm",
		"agent_description_short", "agent_services_short", "agent_tags",
		"rule_category", "rule_confidence", "rule_source",
	}
	for _, m := range modelsUnderTest {
		header = append(header,
			m.csvKey+"_category",
			m.csvKey+"_confidence",
			m.csvKey+"_reason",
			m.csvKey+"_latency_ms",
			m.csvKey+"_source",
			"agreement_"+m.csvKey+"_vs_rule",
		)
	}
	if err := w.Write(header); err != nil {
		log.Fatalf("write header: %v", err)
	}

	for _, row := range rows {
		fb := row.fb
		ag := agentCache[fb.ID]
		desc, services, agTags := agentMetaStrings(ag)
		valueNorm := classifier.NormalizeValue(fb.Value, int(fb.ValueDecimals))

		rec := []string{
			fb.ID,
			strconv.FormatInt(fb.ChainID, 10),
			fb.AgentID,
			fb.Tag1,
			fb.Tag2,
			strconv.FormatFloat(valueNorm, 'f', 4, 64),
			truncateRunes(desc, 120),
			truncateRunes(services, 120),
			strings.Join(agTags, "|"),
			string(row.rule.Category),
			strconv.FormatFloat(row.rule.Confidence, 'f', 2, 64),
			row.rule.Source,
		}
		for _, m := range modelsUnderTest {
			res := row.llm[m.csvKey]
			rec = append(rec,
				string(res.Category),
				strconv.FormatFloat(res.Confidence, 'f', 2, 64),
				truncateRunes(res.Reason, 200),
				strconv.FormatInt(row.llmMs[m.csvKey], 10),
				res.Source,
				strconv.FormatBool(string(res.Category) == string(row.rule.Category)),
			)
		}
		if err := w.Write(rec); err != nil {
			log.Fatalf("write row: %v", err)
		}
	}

	log.Printf("CSV written: %s (%d rows)", *outPath, len(rows))
}

// sampleByCategory uses $match + $sample to pull `n` random feedback records
// for the given classification.category.
func sampleByCategory(ctx context.Context, repo *feedbackrepo.Repository, category string, n int) ([]feedbackrepo.FeedbackRecord, error) {
	if n <= 0 {
		return nil, nil
	}
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.M{"classification.category": category}}},
		{{Key: "$sample", Value: bson.M{"size": int64(n)}}},
	}
	cur, err := repo.Coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}
	defer cur.Close(ctx)

	var docs []feedbackrepo.FeedbackRecord
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return docs, nil
}

// buildAgentCache fetches agent docs once per (chainId, agentId) pair seen in samples.
func buildAgentCache(ctx context.Context, agents *agentrepo.Repository, samples []feedbackrepo.FeedbackRecord) map[string]*agentrepo.AgentDocument {
	cache := make(map[string]*agentrepo.AgentDocument, len(samples))
	keyToAgent := make(map[string]*agentrepo.AgentDocument)
	for _, s := range samples {
		k := fmt.Sprintf("%d:%s", s.ChainID, s.AgentID)
		if _, seen := keyToAgent[k]; seen {
			cache[s.ID] = keyToAgent[k]
			continue
		}
		doc, err := agents.FindByAgentID(ctx, s.ChainID, s.AgentID)
		if err != nil {
			// Treat missing agent as nil context — LLM still runs without
			// description/services.
			keyToAgent[k] = nil
			cache[s.ID] = nil
			continue
		}
		keyToAgent[k] = doc
		cache[s.ID] = doc
	}
	return cache
}

func agentMetaStrings(ag *agentrepo.AgentDocument) (description, services string, tags []string) {
	if ag == nil {
		return "", "", nil
	}
	description = ag.Description
	if len(ag.Services) > 0 {
		b, err := json.Marshal(ag.Services)
		if err == nil {
			services = string(b)
		}
	}
	tags = ag.Tags
	return description, services, tags
}

// offchainSummary extracts a short readable summary from feedbackParsed when present.
func offchainSummary(fb feedbackrepo.FeedbackRecord) string {
	if len(fb.FeedbackParsed) == 0 {
		return ""
	}
	b, err := json.Marshal(fb.FeedbackParsed)
	if err != nil {
		return ""
	}
	s := string(b)
	if len(s) > 400 {
		s = s[:400]
	}
	return s
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
