package main

// classifier-bench — Offline + optional-online benchmark for the LLM classifier.
//
// Default (offline): reports prompt-size deltas between the v1 prompt and the
// new compact-3B prompt over a built-in eval set sampled from real ERC-8004
// fallback inputs.
//
// Online (when LLM_BASE_URL is set): also runs each eval row through the
// configured LLM under both prompts and reports JSON-validity rate, category
// distribution, others-rate, and average per-call latency.
//
// Usage:
//   go run ./cmd/tools/classifier-bench               # offline only
//   LLM_BASE_URL=http://localhost:11434 \
//     LLM_MODEL=qwen2.5:3b \
//     go run ./cmd/tools/classifier-bench             # offline + online
//
// Env vars (online mode):
//   LLM_BASE_URL          required (e.g. http://localhost:11434 for Ollama)
//   LLM_MODEL             required (e.g. qwen2.5:3b)
//   LLM_MODE              ollama | llamacpp        (default: ollama)
//   LLM_TIMEOUT_SECONDS   default 60

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	classifier "erc-8004-benchmarking-be/internal/domain/classifier"
)

// evalSample mirrors a fallback-bound feedback record sampled from the
// production `feedback_history` collection. Each archetype below was verified
// against real data — see memory note "Feedback tag distribution skew".
type evalSample struct {
	name             string
	tag1             string
	tag2             string
	valueNorm        float64
	offchainContent  string
	agentDescription string
	agentServices    string
	agentTags        []string            // denormalized onchain tags, e.g. ["defi","swap"]
	expectedCategory classifier.Category // best human guess for agreement metric
}

func evalSet() []evalSample {
	defi := "OASF: technology/blockchain/defi, technology/blockchain/cryptocurrency"
	bank := "OASF: finance_and_business/banking, finance_and_business/finance"
	mentoDesc := "Mento broker agent — multi-token Celo stablecoin swaps."
	celoFXDesc := "Celo FX trader agent — Mento spreads, cUSD/cEUR swaps."
	ichiDesc := "Yield optimization agent — Ichi vaults on Celo."
	return []evalSample{
		// ─── 1. token-symbol + transaction verb → app_specific (real samples) ───
		{name: "fx-trade buy", tag1: "fx-trade", tag2: "buy", valueNorm: 0.85, agentDescription: celoFXDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "CADm buy", tag1: "CADm", tag2: "buy", valueNorm: 0.80, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "JPYm buy", tag1: "JPYm", tag2: "buy", valueNorm: 0.75, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "COPm buy", tag1: "COPm", tag2: "buy", valueNorm: 1.0, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "BRLm buy", tag1: "BRLm", tag2: "buy", valueNorm: 1.0, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "USDm buy", tag1: "USDm", tag2: "buy", valueNorm: 1.0, agentDescription: mentoDesc, expectedCategory: classifier.CategoryApp},
		{name: "AUDm buy", tag1: "AUDm", tag2: "buy", valueNorm: 1.0, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "EURm buy", tag1: "EURm", tag2: "buy", valueNorm: 1.0, agentDescription: mentoDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "Ichi USDT-WBTC vault deposit", tag1: "Ichi USD₮-WBTC vault", tag2: "deposit", valueNorm: 1.0, agentDescription: ichiDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "Ichi USD-WETH vault deposit (smart-quote)", tag1: "Ichi USD’-WETH vault", tag2: "deposit", valueNorm: 1.0, agentDescription: ichiDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "Ichi USD-WBTC vault deposit (smart-quote)", tag1: "Ichi USD’-WBTC vault", tag2: "deposit", valueNorm: 1.0, agentDescription: ichiDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "Ichi USDT-WETH no-vault-suffix", tag1: "Ichi USD₮-WETH", tag2: "deposit", valueNorm: 1.0, agentDescription: ichiDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "Ichi USDT-WBTC no-vault-suffix", tag1: "Ichi USD₮-WBTC", tag2: "deposit", valueNorm: 1.0, agentDescription: ichiDesc, agentServices: defi, expectedCategory: classifier.CategoryApp},

		// ─── 2. sentence pasted into tag2 → service_feedback ───
		{name: "a2a praise sentence", tag1: "a2a", tag2: "A2A protocol handles rate analysis and portfolio queries cleanly. JSON-RPC responses are well-structured.", valueNorm: 0.88, agentDescription: "FX signal trader. A2A interface.", agentServices: defi, expectedCategory: classifier.CategoryService},
		{name: "execution praise sentence", tag1: "execution", tag2: "Agentic order execution is impressive — agent tracked momentum and executed before rate dropped.", valueNorm: 0.94, agentDescription: "Trading bot, executes Mento spreads.", agentServices: defi, expectedCategory: classifier.CategoryService},
		{name: "tee attestation praise", tag1: "tee", tag2: "Intel TDX attestation via Phala Cloud verifies correctly. Good to see verifiable execution.", valueNorm: 0.85, agentDescription: "Verified FX agent on TEE.", expectedCategory: classifier.CategoryService},
		{name: "speed sentence", tag1: "speed", tag2: "Signals arrive within seconds of oracle update. Faster than manual monitoring.", valueNorm: 0.82, agentDescription: celoFXDesc, expectedCategory: classifier.CategoryService},
		{name: "value sentence", tag1: "value", tag2: "Premium signals at $0.01 via x402 are a steal. Entry/exit prices are actionable.", valueNorm: 0.85, agentDescription: celoFXDesc, expectedCategory: classifier.CategoryService},

		// ─── 3. automated metrics / state flags → config_feedback ───
		{name: "reputation period id", tag1: "m1-mainnet-reputation", tag2: "M1-MAINNET-REPUTATION-2026-02-12-01", valueNorm: 1.0, agentDescription: "Reputation oracle.", expectedCategory: classifier.CategoryConfig},
		{name: "ownerVerified domain", tag1: "ownerVerified", tag2: "domain", valueNorm: 0.93, agentDescription: "DNS-based agent identity verifier.", expectedCategory: classifier.CategoryConfig},
		{name: "ownerVerified reliable", tag1: "ownerVerified", tag2: "reliable", valueNorm: 0.96, agentDescription: "Identity verifier", expectedCategory: classifier.CategoryConfig},
		{name: "active oasf", tag1: "active", tag2: "oasf", valueNorm: 0.87, agentDescription: "OASF schema validator.", expectedCategory: classifier.CategoryConfig},
		{name: "safety-score monitor", tag1: "safety-score", tag2: "agent-safety-monitor", valueNorm: 0.9, agentDescription: "Safety monitor.", expectedCategory: classifier.CategoryConfig},
		{name: "successRate api_calls", tag1: "successRate", tag2: "api_calls", valueNorm: 0.95, agentDescription: "API gateway.", expectedCategory: classifier.CategoryConfig},
		{name: "uptime bare", tag1: "uptime", tag2: "", valueNorm: 1.0, agentDescription: "Health probe.", expectedCategory: classifier.CategoryConfig},
		{name: "factCheck", tag1: "factCheck", tag2: "", valueNorm: 0.9, agentDescription: "Fact-check oracle.", expectedCategory: classifier.CategoryConfig},
		{name: "tokenVerification", tag1: "tokenVerification", tag2: "", valueNorm: 1.0, agentDescription: "Token verifier.", expectedCategory: classifier.CategoryConfig},

		// ─── 4. topic / hashtag pairs ───
		{name: "trading celo", tag1: "trading", tag2: "celo", valueNorm: 0.83, agentDescription: "FX trader on Celo.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "forex new", tag1: "forex", tag2: "new", valueNorm: 0.80, agentDescription: "FX agent.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "forex celo", tag1: "forex", tag2: "celo", valueNorm: 0.80, agentDescription: "FX agent on Celo.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "natillera celo (local)", tag1: "natillera", tag2: "celo", valueNorm: 0.99, agentDescription: "Esusu/Natillera community-savings rotational agent on Celo.", agentServices: bank, expectedCategory: classifier.CategoryApp},
		{name: "savings community", tag1: "savings", tag2: "community", valueNorm: 0.97, agentDescription: "Community savings.", agentServices: bank, expectedCategory: classifier.CategoryApp},
		{name: "web3 colombia", tag1: "web3", tag2: "colombia", valueNorm: 0.95, agentDescription: "Web3 onboarding agent for Colombia.", expectedCategory: classifier.CategoryOthers},
		{name: "financial-inclusion latam", tag1: "financial-inclusion", tag2: "latam", valueNorm: 0.96, agentDescription: "Community savings agent for Colombia.", expectedCategory: classifier.CategoryOthers},
		{name: "research defi", tag1: "research", tag2: "defi", valueNorm: 0.85, agentDescription: "DeFi research bot.", agentServices: defi, expectedCategory: classifier.CategoryOthers},
		{name: "memory helpful", tag1: "memory", tag2: "helpful", valueNorm: 0.9, agentDescription: "RAG memory layer.", expectedCategory: classifier.CategoryService},
		{name: "Skillful (capital)", tag1: "Skillful", tag2: "", valueNorm: 0.95, agentDescription: "Generalist agent.", expectedCategory: classifier.CategoryService},
		{name: "based casual", tag1: "based", tag2: "", valueNorm: 0.88, agentDescription: "Crypto Twitter agent.", expectedCategory: classifier.CategoryOthers},
		{name: "AI single tag", tag1: "AI", tag2: "", valueNorm: 0.9, agentDescription: "Generic AI agent.", expectedCategory: classifier.CategoryOthers},
		{name: "Defi single tag", tag1: "Defi", tag2: "", valueNorm: 0.92, agentDescription: "DeFi assistant.", agentServices: defi, expectedCategory: classifier.CategoryOthers},
		{name: "good1 good2 (singleton)", tag1: "good1", tag2: "good2", valueNorm: 0.85, agentDescription: "Generic agent.", expectedCategory: classifier.CategoryService},
		{name: "Helpful Exclusive (mixed case)", tag1: "Helpful", tag2: "Exclusive", valueNorm: 0.9, agentDescription: "Premium AI", expectedCategory: classifier.CategoryService},
		{name: "Reliable Secure (mixed case)", tag1: "Reliable", tag2: "Secure", valueNorm: 0.9, agentDescription: "Custodial agent.", expectedCategory: classifier.CategoryService},
		{name: "Creative + Cool pic", tag1: "Creative", tag2: "Cool pic", valueNorm: 0.9, agentDescription: "Image generator.", expectedCategory: classifier.CategoryService},
		{name: "ask_gigabrain", tag1: "ask_gigabrain", tag2: "", valueNorm: 0.85, agentDescription: "Q&A oracle for traders.", expectedCategory: classifier.CategoryApp},
		{name: "vibe trading x402", tag1: "vibe trading", tag2: "x402", valueNorm: 0.9, agentDescription: "x402-paid trading agent.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "token-audit audit", tag1: "token-audit", tag2: "audit", valueNorm: 0.85, agentDescription: "Smart-contract audit agent.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "risk-analysis inter-agent", tag1: "risk-analysis", tag2: "inter-agent", valueNorm: 0.85, agentDescription: "Risk scoring agent.", expectedCategory: classifier.CategoryConfig},
		{name: "x402 success", tag1: "x402", tag2: "success", valueNorm: 1.0, agentDescription: "x402 payment relay.", expectedCategory: classifier.CategoryConfig},
		{name: "agent_rating execution-market", tag1: "agent_rating", tag2: "execution-market", valueNorm: 0.85, agentDescription: "Reputation aggregator.", expectedCategory: classifier.CategoryConfig},

		// ─── 5. spam / noise / service rule-matched (sanity checks) ───
		{name: "spam telegram", tag1: "get top 1 rank -->", tag2: "https://t.me/agent_bldr", valueNorm: 1.0, agentDescription: "ranked agent", expectedCategory: classifier.CategorySpam},
		{name: "noise empty", tag1: "", tag2: "", valueNorm: 0.0, expectedCategory: classifier.CategoryNoise},
		{name: "noise test/asd", tag1: "test", tag2: "asd", valueNorm: 0.0, expectedCategory: classifier.CategoryNoise},
		{name: "noise xyz/abc", tag1: "xyz", tag2: "abc", valueNorm: 0.0, expectedCategory: classifier.CategoryOthers}, // not in noise list — model judgment
		{name: "service helpful fast", tag1: "helpful", tag2: "fast", valueNorm: 1.0, agentDescription: "DAO assistant.", expectedCategory: classifier.CategoryService},
		{name: "service quality green", tag1: "quality", tag2: "green", valueNorm: 0.9, agentDescription: "Generic", expectedCategory: classifier.CategoryService},
		{name: "service starred perfect", tag1: "starred", tag2: "perfect", valueNorm: 1.0, agentDescription: "Generic", expectedCategory: classifier.CategoryService},
		{name: "service trustless privacy", tag1: "trustless", tag2: "privacy", valueNorm: 0.9, agentDescription: "Privacy agent.", expectedCategory: classifier.CategoryService},
		{name: "config winRate date-range", tag1: "winRate", tag2: "2026-02-25/2026-02-28", valueNorm: 0.7, agentDescription: "Strategy bot.", expectedCategory: classifier.CategoryConfig},
		{name: "config worker_rating addr", tag1: "worker_rating", tag2: "0x52e05c8e", valueNorm: 0.9, agentDescription: "Worker mesh.", expectedCategory: classifier.CategoryConfig},
		{name: "config liveness", tag1: "liveness", tag2: "liveness-check", valueNorm: 1.0, agentDescription: "Live probe.", expectedCategory: classifier.CategoryConfig},
		{name: "app token mint", tag1: "token", tag2: "mint", valueNorm: 1.0, agentDescription: "Token factory.", agentServices: defi, expectedCategory: classifier.CategoryApp},
		{name: "app personality fragment", tag1: "personality", tag2: "fragment", valueNorm: 0.9, agentDescription: "Soul agent.", expectedCategory: classifier.CategoryApp},
		{name: "app generateImage", tag1: "generateImage", tag2: "", valueNorm: 1.0, agentDescription: "Image generator.", expectedCategory: classifier.CategoryApp},

		// ─── 6. offchain JSON content (exercises summarizer) ───
		{name: "json comment praises agent", tag1: "execution", tag2: "", valueNorm: 0.94, offchainContent: `{"comment":"Agent runs daily and swaps reliably","rating":100,"version":"2.0"}`, agentDescription: "Trading bot.", expectedCategory: classifier.CategoryService},
		{name: "json feedback recommend", tag1: "accuracy", tag2: "accurate", valueNorm: 1.0, offchainContent: `{"feedback":"I highly recommend this agent for all users","rating":100}`, agentDescription: "Esusu AI.", expectedCategory: classifier.CategoryService},
	}
}

func main() {
	samples := evalSet()
	fmt.Printf("classifier-bench — %d eval samples\n\n", len(samples))

	// ── Section 1: prompt-size comparison (offline; always runs) ──
	printPromptSizeReport(samples)

	// ── Section 2: optional online benchmark via Ollama / llama.cpp ──
	baseURL := os.Getenv("LLM_BASE_URL")
	model := os.Getenv("LLM_MODEL")
	if baseURL == "" || model == "" {
		fmt.Println("\nLLM_BASE_URL / LLM_MODEL not set — skipping online benchmark.")
		fmt.Println("Set both to compare valid-JSON rate and category distribution against a live model.")
		return
	}
	mode := classifier.ModeOllama
	if strings.EqualFold(os.Getenv("LLM_MODE"), "llamacpp") {
		mode = classifier.ModeLlamaCpp
	}
	timeout, _ := strconv.Atoi(os.Getenv("LLM_TIMEOUT_SECONDS"))
	if timeout <= 0 {
		timeout = 60
	}

	v1 := classifier.NewLLMClient(classifier.LLMClientConfig{
		BaseURL: baseURL, Model: model, Mode: mode,
		TimeoutSeconds: timeout, PromptVersion: classifier.PromptVersionV1,
	})
	v2 := classifier.NewLLMClient(classifier.LLMClientConfig{
		BaseURL: baseURL, Model: model, Mode: mode,
		TimeoutSeconds: timeout, PromptVersion: classifier.PromptVersionCompact3B,
	})

	ctx := context.Background()
	v1Stats := runOnline(ctx, v1, samples, "v1")
	v2Stats := runOnline(ctx, v2, samples, "compact-3b")
	printOnlineReport(v1Stats, v2Stats)
}

// ─── offline: prompt size ──────────────────────────────────────────────

func printPromptSizeReport(samples []evalSample) {
	fmt.Println("Prompt size — full system+user payload (chars; ~tokens at 4 chars/token)")
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "case\tv1 chars\tv1 ~tok\tcompact chars\tcompact ~tok\tdelta")
	var totalV1, totalV2 int
	for _, s := range samples {
		v1 := classifier.BuildPrompt(s.tag1, s.tag2, s.valueNorm, s.offchainContent, s.agentDescription, s.agentServices, s.agentTags, "", "")
		v2 := classifier.BuildPromptCompact3B(s.tag1, s.tag2, s.valueNorm, s.offchainContent, s.agentDescription, s.agentServices, s.agentTags, "", "")
		v1c := utf8.RuneCountInString(v1)
		v2c := utf8.RuneCountInString(v2)
		totalV1 += v1c
		totalV2 += v2c
		delta := 0.0
		if v1c > 0 {
			delta = float64(v2c-v1c) / float64(v1c) * 100
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%+.1f%%\n",
			truncate(s.name, 32), v1c, v1c/4, v2c, v2c/4, delta)
	}
	avgDelta := 0.0
	if totalV1 > 0 {
		avgDelta = float64(totalV2-totalV1) / float64(totalV1) * 100
	}
	fmt.Fprintf(tw, "TOTAL\t%d\t%d\t%d\t%d\t%+.1f%%\n",
		totalV1, totalV1/4, totalV2, totalV2/4, avgDelta)
	tw.Flush()

	// System prompt sizes alone.
	v1Sys := utf8.RuneCountInString(systemPromptV1Sample())
	v2Sys := utf8.RuneCountInString(systemPromptCompact3BSample())
	fmt.Printf("\nSystem prompt: v1=%d chars (~%d tok), compact-3b=%d chars (~%d tok), delta=%+.1f%%\n",
		v1Sys, v1Sys/4, v2Sys, v2Sys/4, float64(v2Sys-v1Sys)/float64(v1Sys)*100)
}

// systemPromptV1Sample / systemPromptCompact3BSample — extract just the system
// portion from BuildPrompt / BuildPromptCompact3B for the size report.
func systemPromptV1Sample() string {
	full := classifier.BuildPrompt("", "", 0, "", "", "", nil, "", "")
	return splitSystem(full)
}
func systemPromptCompact3BSample() string {
	full := classifier.BuildPromptCompact3B("", "", 0, "", "", "", nil, "", "")
	return splitSystem(full)
}
func splitSystem(s string) string {
	idx := strings.Index(s, "\n\n[USER]\n")
	if idx < 0 {
		return s
	}
	return strings.TrimPrefix(s[:idx], "[SYSTEM]\n")
}

// ─── online: live LLM run ──────────────────────────────────────────────

type onlineStats struct {
	label         string
	total         int
	validJSON     int
	others        int
	agreement     int
	totalLatency  time.Duration
	categoryCount map[classifier.Category]int
}

func runOnline(ctx context.Context, c *classifier.LLMClient, samples []evalSample, label string) onlineStats {
	stats := onlineStats{label: label, categoryCount: map[classifier.Category]int{}}
	fmt.Printf("\n--- online run: %s (model talks to %s) ---\n", label, os.Getenv("LLM_BASE_URL"))
	for _, s := range samples {
		stats.total++
		start := time.Now()
		res := c.Classify(ctx, s.tag1, s.tag2, s.valueNorm, s.offchainContent, s.agentDescription, s.agentServices, s.agentTags, "", "")
		stats.totalLatency += time.Since(start)
		if res.Source == "llm" {
			stats.validJSON++
		}
		stats.categoryCount[res.Category]++
		if res.Category == classifier.CategoryOthers {
			stats.others++
		}
		if res.Category == s.expectedCategory {
			stats.agreement++
		}
	}
	return stats
}

func printOnlineReport(rows ...onlineStats) {
	fmt.Println("\n=== online benchmark summary ===")
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "metric\t"+joinLabels(rows))
	fmt.Fprintf(tw, "valid-JSON rate\t%s\n", joinPct(rows, func(s onlineStats) (int, int) { return s.validJSON, s.total }))
	fmt.Fprintf(tw, "agreement vs expected\t%s\n", joinPct(rows, func(s onlineStats) (int, int) { return s.agreement, s.total }))
	fmt.Fprintf(tw, "others rate\t%s\n", joinPct(rows, func(s onlineStats) (int, int) { return s.others, s.total }))
	fmt.Fprintf(tw, "avg latency\t%s\n", joinAvg(rows))
	tw.Flush()

	for _, r := range rows {
		fmt.Printf("\n[%s] category distribution:\n", r.label)
		for _, cat := range []classifier.Category{
			classifier.CategorySpam, classifier.CategoryNoise,
			classifier.CategoryService, classifier.CategoryConfig,
			classifier.CategoryApp, classifier.CategoryOthers,
		} {
			fmt.Printf("  %-18s %d\n", cat, r.categoryCount[cat])
		}
	}
}

func joinLabels(rs []onlineStats) string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.label
	}
	return strings.Join(out, "\t")
}
func joinPct(rs []onlineStats, f func(onlineStats) (int, int)) string {
	out := make([]string, len(rs))
	for i, r := range rs {
		num, den := f(r)
		pct := 0.0
		if den > 0 {
			pct = float64(num) / float64(den) * 100
		}
		out[i] = fmt.Sprintf("%d/%d (%.1f%%)", num, den, pct)
	}
	return strings.Join(out, "\t")
}
func joinAvg(rs []onlineStats) string {
	out := make([]string, len(rs))
	for i, r := range rs {
		if r.total == 0 {
			out[i] = "n/a"
			continue
		}
		out[i] = (r.totalLatency / time.Duration(r.total)).String()
	}
	return strings.Join(out, "\t")
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n-1]) + "…"
}
