package classifier

// classifier.go — Rule-based feedback classifier for ERC-8004 NewFeedback events.
// Covers ~92% of on-chain records without LLM. Implements Stage 1–3 of the
// Feedback Classifier flow (flow_rule_only.md).

import (
	"math"
	"math/big"
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
	CategoryJunk    Category = "junk" // spam + noise merged (noise has only 33 records in the corpus)
)

// Result is the output of Classify.
type Result struct {
	Category      Category
	Confidence    float64
	Source        string // "rule" | "fallback"
	NormalizedTag string // canonical tag1 value
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

	// ── Stage 1A: Spam-shaped junk ────────────────────────────────
	if isSpam(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: 0.99, Source: "rule"}
	}

	// ── Stage 1B: Noise-shaped junk ───────────────────────────────
	if isNoise(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: 0.99, Source: "rule"}
	}

	// ── Stage 1C: All-digits junk ─────────────────────────────────
	// Both tags consisting purely of digits carries no semantic signal
	// (no domain, no quality adjective, no operation verb). Requiring BOTH
	// keeps false positives near zero — single numeric tags often pair with
	// a meaningful counterpart (e.g. tag2=block height + tag1=metric name).
	if isAllDigitsJunk(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: 0.95, Source: "rule"}
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

	// Stage 2: Config — trust with community/peer tag2 → service_feedback
	// "trust" is in configTag1Set for the oracle-screening bulk (54k records), but
	// when paired with human-evaluation tag2 values it is a quality signal, not infra.
	if t1 == "trust" && (t2 == "community-verified" || t2 == "peer-review" || t2 == "user-review") {
		return Result{Category: CategoryService, Confidence: 0.90, Source: "rule", NormalizedTag: "trust"}
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

	// Stage 2: Service Feedback — tag2 discriminator
	if serviceTag2Set[t2] {
		return Result{
			Category:      CategoryService,
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

// isAllDigitsJunk matches records where BOTH tags are non-empty and consist
// purely of digits — they carry no semantic signal at all. Single-side numeric
// tags often pair with meaningful counterparts (e.g. block height + metric
// name), so this requires both sides to qualify as junk.
func isAllDigitsJunk(t1, t2 string) bool {
	return t1 != "" && t2 != "" && allDigitsRe.MatchString(t1) && allDigitsRe.MatchString(t2)
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
