package classifier

// classifier.go — Rule-based feedback classifier for ERC-8004 NewFeedback events.
// Covers ~92% of on-chain records without LLM. Implements Stage 1–3 of the
// Feedback Classifier flow (flow_rule_only.md).

import (
	"math"
	"math/big"
	"regexp"
	"strings"
)

// ─── Output ──────────────────────────────────────────────────────────

// Category is the semantic classification of a feedback event.
type Category string

const (
	CategoryService Category = "service_feedback"
	CategoryConfig  Category = "config_feedback"
	CategoryApp     Category = "app_specific"
	CategoryOthers  Category = "others"
	CategorySpam    Category = "spam"
	CategoryNoise   Category = "noise"
)

// Result is the output of Classify.
type Result struct {
	Category      Category
	Confidence    float64
	Source        string // "rule" | "fallback"
	NormalizedTag string // canonical tag1 value
}

// ─── Patterns ────────────────────────────────────────────────────────

var (
	spamURLPattern  = regexp.MustCompile(`(?i)(t\.me/|telegram\.me|https?://|http://|t\.me)`)
	spamRankPattern = regexp.MustCompile(`(?i)(get\s+top|top\s*[0-9]|-{2,}>|#1\s+rank)`)
	workerAddrRe    = regexp.MustCompile(`^0x[0-9a-fA-F]{6,}$`)
	dateRangeRe     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}`)
	camelCaseRe     = regexp.MustCompile(`^[a-z][a-zA-Z0-9]{6,}[A-Z][a-zA-Z0-9]+$`)
	snakeCaseVerbRe = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z][a-z0-9]*){2,}$`)
	base58TokenRe   = regexp.MustCompile(`^base:\d+$`)
	emojiOnlyRe     = regexp.MustCompile(`^[\x{1F000}-\x{1FFFF}\x{2600}-\x{27FF}\s]+$`)
)

// ─── Lookup Sets ─────────────────────────────────────────────────────

// configTag1Set: tag1 values that always mean config_feedback.
var configTag1Set = map[string]bool{
	// ERC-8004 spec signals
	"reachable": true, "liveness": true, "successrate": true,
	"responsetime": true, "blocktimefreshness": true,
	"uptime": true, "revenues": true, "tradingyield": true,
	// Infra probes
	"liveness-check": true, "health-check": true, "ping": true,
	"bulkcheck": true, "trust-oracle": true, "trust": true,
	// Automated trust / guard systems (emit score dimensions, not app actions)
	"sentinel8004": true, "agentguard": true,
	// Protocol-level metrics used as tag1
	"a2a": true, "trust-score": true,
	// Verification and delegation protocols
	"human-verification": true, "agent-delegation": true,
}

// configTag2Set: tag2 values that indicate config_feedback when tag1 is unrecognized.
// Only consulted after explicit tag1 sets (config/app/service) all miss.
var configTag2Set = map[string]bool{
	"oracle-screening": true, "liveness-check": true,
	"win-rate": true, "coverage-rate": true, "exit-rate": true,
	"service_quality": true, "automated-screening": true,
	"a2a": true, "mcp": true, "web": true,
	// SentinelNet oracle protocol and versioned trust guard protocols
	"sentinelnet-v1": true, "trust-v2": true,
}

// appTag1Set: tag1 values that always mean app_specific.
var appTag1Set = map[string]bool{
	// Soul fragment dimensions
	"personality": true, "knowledge": true, "timeline": true,
	"relationship": true, "stance": true, "style": true,
	// DeFi / token operations
	"trade": true, "token": true, "swap_token": true, "swap": true,
	"open_position": true, "open_dca": true, "close_dca": true,
	"trade_perpetuals": true, "fund_startup": true, "lido": true,
	"mandate": true, "faucet-drip": true, "token_info": true,
	"transfer_token": true, "create_onetime_signal": true, "purchase": true,
	// Agent economy
	"tip": true, "watchorfight": true, "doppel": true,
	// Agent lifecycle operations (spawn appears as tag1 in practice)
	"spawn": true, "tycoon": true,
	// Utility / payment operations
	"airtime": true, "gift_card": true, "bill_payment": true,
	// Task / research operations
	"task-completion": true, "market-intelligence": true, "research-delivery": true,
	// Risk / audit operations (camelCase promoted from regex to explicit for higher confidence)
	"contractrisk": true,
	// Creative / generative operations
	"generateimage": true, "custom_song_creation": true,
	"image_to_ugc_video_generation": true,
	"curatenewsfeedbydigitaltwin":   true,
	"x402-scan":                     true, "token_ai_analysis_trade_suggestion": true,
}

// appTag2Set: tag2 values that indicate app_specific when tag1 is unrecognized.
var appTag2Set = map[string]bool{
	"fragment": true, "mint": true, "record": true,
	"create": true, "agent": true, "spawn": true,
	// Action type tags
	"agentaction": true,
}

// serviceTag1Set: known quality/review tag1 values → service_feedback.
var serviceTag1Set = map[string]bool{
	// ERC-8004 spec (rating type)
	"starred": true,
	// Quality adjectives
	"quality": true, "performance": true, "service": true,
	"helpful": true, "fast": true, "reliable": true,
	"reliability": true, "excellence": true, "excellent": true,
	"speed": true, "satisfaction": true, "experience": true,
	"value": true, "rating": true, "good": true,
	"efficient": true, "robust": true, "secure": true,
	"audited": true, "innovative": true, "transparent": true,
	"trustless": true, "composable": true, "interoperable": true,
	"open": true, "standards-compliant": true, "gas-efficient": true,
	"cost-effective": true, "low-latency": true, "scalable": true,
	"layer-2": true, "privacy": true, "defi": true, "defai": true,
	"x402": true, "accurate": true, "cool": true, "great": true,
	"general": true, "mcp": true, "security": true, "review": true,
	"useful": true, "smart": true, "nice": true, "overall": true,
	"accuracy": true, "infrastructure": true, "creative": true,
	"support": true, "intelligent": true, "analytical": true,
	"fast helpful": true,
	// Noun forms of adjectives already in set (scalable→scalability, etc.)
	"scalability": true, "innovation": true,
	// Evaluation dimensions observed in practice
	"usability": true, "compliance": true, "integration": true,
	"peer-review": true, "content-moderation": true,
	// Common typos — normalized here
	"helpfull": true, "powerfull": true, "usefull": true,
	"reliabel": true, "excelent": true,
}

// noiseTag1Set: always noise, drop silently.
var noiseTag1Set = map[string]bool{
	"test": true, "asd": true,
}

// serviceKeywords: fuzzy keywords for lower-confidence service detection.
var serviceKeywords = []string{
	"helpful", "fast", "reliable", "quality", "excellent", "good",
	"useful", "accurate", "great", "smart", "simple", "easy",
	"smooth", "solid", "stable", "best", "nice", "clean",
}

// ─── Main Classifier ─────────────────────────────────────────────────

// Classify is the single entry point for feedback classification.
// tag1, tag2 are the raw values from the on-chain NewFeedback event (any casing).
// It is safe to call concurrently.
func Classify(tag1, tag2 string) Result {
	t1raw := strings.TrimSpace(tag1)
	t2raw := strings.TrimSpace(tag2)
	t1 := strings.ToLower(t1raw)
	t2 := strings.ToLower(t2raw)

	// ── Stage 1A: Spam ────────────────────────────────────────────
	if isSpam(t1, t2) {
		return Result{Category: CategorySpam, Confidence: 0.99, Source: "rule"}
	}

	// ── Stage 1B: Noise ───────────────────────────────────────────
	if isNoise(t1, t2) {
		return Result{Category: CategoryNoise, Confidence: 0.99, Source: "rule"}
	}

	// ── Stage 2: Config — high-confidence specific patterns first ────────────
	// worker_rating + hex address (must precede generic configTag1Set match)
	if t1 == "worker_rating" && workerAddrRe.MatchString(t2) {
		return Result{
			Category:      CategoryConfig,
			Confidence:    0.99,
			Source:        "rule",
			NormalizedTag: "worker_rating",
		}
	}

	// winrate + date range (must precede generic configTag1Set match)
	if t1 == "winrate" && dateRangeRe.MatchString(t2raw) {
		return Result{
			Category:      CategoryConfig,
			Confidence:    0.99,
			Source:        "rule",
			NormalizedTag: "winrate",
		}
	}

	// Stage 2: Config — tag1 direct match
	if configTag1Set[t1] {
		return Result{
			Category:      CategoryConfig,
			Confidence:    0.95,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// ── Stage 2: App Specific — tag1 direct match ─────────────────
	// Checked before configTag2Set so an explicit app tag1 is never overridden
	// by an ambiguous tag2 discriminator.
	if appTag1Set[t1] {
		return Result{
			Category:      CategoryApp,
			Confidence:    0.95,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// Stage 2: App Specific — camelCase function name pattern
	// e.g. generateImage, publishPost, graduationEvaluation
	if camelCaseRe.MatchString(t1raw) {
		return Result{
			Category:      CategoryApp,
			Confidence:    0.80,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// Stage 2: App Specific — deep_snake_case function pattern (3+ parts)
	// e.g. earn_instant_usdc_reward, create_onetime_signal
	if snakeCaseVerbRe.MatchString(t1) {
		return Result{
			Category:      CategoryApp,
			Confidence:    0.80,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// ── Stage 2: Service Feedback — tag1 direct match ─────────────
	// Checked before configTag2Set so a clear service tag1 (e.g. "review") is
	// not overridden by a tag2 discriminator like "service_quality".
	if serviceTag1Set[t1] {
		return Result{
			Category:      CategoryService,
			Confidence:    0.88,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// Stage 2: Config — tag2 discriminator
	// Only reached when tag1 matched none of the explicit sets above.
	if configTag2Set[t2] {
		return Result{
			Category:      CategoryConfig,
			Confidence:    0.90,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// Stage 2: App Specific — tag2 discriminator
	if appTag2Set[t2] {
		return Result{
			Category:      CategoryApp,
			Confidence:    0.92,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// Stage 2: App Specific — base:NUMBER token creation pattern
	if base58TokenRe.MatchString(t2) {
		return Result{
			Category:      CategoryApp,
			Confidence:    0.98,
			Source:        "rule",
			NormalizedTag: "token-create",
		}
	}

	// Stage 2: Service — contains known service keywords (fuzzy)
	if containsServiceKeyword(t1) {
		return Result{
			Category:      CategoryService,
			Confidence:    0.72,
			Source:        "rule",
			NormalizedTag: t1,
		}
	}

	// ── Stage 2: Emoji only → others ──────────────────────────────
	if emojiOnlyRe.MatchString(t1raw) {
		return Result{Category: CategoryOthers, Confidence: 0.50, Source: "rule"}
	}

	// ── Fallback → others (awaiting LLM) ──────────────────────────
	return Result{Category: CategoryOthers, Confidence: 0.0, Source: "fallback"}
}

// ─── Helpers ─────────────────────────────────────────────────────────

func isSpam(t1, t2 string) bool {
	return spamURLPattern.MatchString(t1) ||
		spamURLPattern.MatchString(t2) ||
		spamRankPattern.MatchString(t1) ||
		spamRankPattern.MatchString(t2)
}

// isNoise matches only explicit junk tag1 values (test/asd-style) — records
// with empty tag1+tag2 are NOT classified as noise here; they fall through
// to the "others" fallback so the LLM can read the offchain content and
// classify based on real signal (comment/attachments/proofOfPayment).
func isNoise(t1, t2 string) bool {
	if noiseTag1Set[t1] && (t2 == "" || noiseTag1Set[t2]) {
		return true
	}
	return false
}

// IsAnomalousValue returns true when the raw on-chain value looks like an
// un-normalized uint256 (len > 20 chars with zero decimals).
// Call this before Classify, on the raw on-chain value string.
func IsAnomalousValue(rawValue string, valueDecimals int) bool {
	return len(rawValue) > 10 && valueDecimals == 0
}

// NormalizeValue converts an on-chain feedback value to a float64 in [-1, 1].
// Interpretation: value is a percentage in [-100, 100] stored with valueDecimals
// decimal places.  vi = value / (100 * 10^valueDecimals), clamped to [-1, 1].
// Negative values are valid per Value Decimals Patterns spec for decreasing metrics
// (e.g. latency reduction). int128 is signed for that reason.
// Uses math/big to handle the full int128 range safely.
func NormalizeValue(rawValue string, valueDecimals int) float64 {
	if rawValue == "" || rawValue == "0" {
		return 0.0
	}

	n := new(big.Int)
	if _, ok := n.SetString(rawValue, 10); !ok {
		return 0.0
	}

	// divisor = 100 * 10^valueDecimals
	exp := valueDecimals + 2 // +2 because percentage is 0-100
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil)

	nF := new(big.Float).SetPrec(64).SetInt(n)
	dF := new(big.Float).SetPrec(64).SetInt(divisor)
	result, _ := new(big.Float).Quo(nF, dF).Float64()

	if result > 1.0 {
		return 1.0
	}
	if result < -1.0 {
		return -1.0
	}
	return result
}

func containsServiceKeyword(t1 string) bool {
	for _, kw := range serviceKeywords {
		if strings.Contains(t1, kw) {
			return true
		}
	}
	return false
}

// ─── Tag-scale normalization ──────────────────────────────────────────────────

// RawValueToReal converts an on-chain value string to its real-world float:
// real = value / 10^valueDecimals.
// Returns (0.0, false) when rawValue is empty or non-numeric.
func RawValueToReal(rawValue string, valueDecimals int) (float64, bool) {
	if rawValue == "" {
		return 0.0, false
	}
	n := new(big.Int)
	if _, ok := n.SetString(rawValue, 10); !ok {
		return 0.0, false
	}
	if valueDecimals <= 0 {
		f, _ := new(big.Float).SetPrec(64).SetInt(n).Float64()
		return f, true
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(valueDecimals)), nil)
	nF := new(big.Float).SetPrec(64).SetInt(n)
	dF := new(big.Float).SetPrec(64).SetInt(divisor)
	result, _ := new(big.Float).Quo(nF, dF).Float64()
	return result, true
}

// AssignTier returns the scale tier for a real value (value / 10^valueDecimals).
//
//	binary:    |real| ≤ 1.0
//	star5:     1.0 < real ≤ 5.0
//	star10:    5.0 < real ≤ 10.0
//	pct100:    10.0 < |real| ≤ 100.0
//	unbounded: |real| > 100
func AssignTier(real float64) string {
	abs := math.Abs(real)
	switch {
	case abs <= 1.0:
		return "binary"
	case real <= 5.0:
		return "star5"
	case real <= 10.0:
		return "star10"
	case abs <= 100.0:
		return "pct100"
	default:
		return "unbounded"
	}
}

// NormalizeValueWithScale normalizes a pre-computed real value using a detected scale.
// Caller must compute real = RawValueToReal(rawValue, valueDecimals) before calling.
// Callers should resolve "" via AssignTier before calling; the default case handles explicit "pct100" or unknown strings.
func NormalizeValueWithScale(real float64, scale string) float64 {
	clamp := func(v float64) float64 { return math.Max(-1.0, math.Min(1.0, v)) }
	switch scale {
	case "binary":
		if real >= 0.5 {
			return 1.0
		}
		return 0.0
	case "star5":
		return clamp(real / 5.0)
	case "star10":
		return clamp(real / 10.0)
	case "unbounded":
		return 0.0
	default: // "pct100" or not yet detected
		return clamp(real / 100.0)
	}
}
