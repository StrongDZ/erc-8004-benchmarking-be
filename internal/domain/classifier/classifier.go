package classifier

// classifier.go — Two-axis rule classifier for ERC-8004 NewFeedback events.
// Cascade: junk → quantity → quality; unknown tags escalate to "others" (LLM).
// Second axis `feature` (infrastructure | agent_domain) is assigned for the
// quality/quantity categories. Covers the clear cases without an LLM.

import (
	"math"
	"math/big"
	"strings"
)

// ─── Output ──────────────────────────────────────────────────────────

// Category is the scoring-relevant axis. Only `quality` feeds the trust score.
type Category string

const (
	CategoryQuality  Category = "quality"   // subjective judgment / service quality — SCORES
	CategoryQuantity Category = "quantity"  // a measured metric (rate/score/speed/count/amount)
	CategoryJunk     Category = "junk"      // spam / noise / meaningless
	CategoryOthers   Category = "others"    // rule could not decide → escalate to LLM
)

// ruleConfidence is assigned to every whitelist rule hit. Tiered scores were removed
// once quantity/quality matching became exact-set only.
const ruleConfidence = 0.99

// Feature is the second axis: what the feedback is ABOUT (not scoring).
type Feature string

const (
	FeatureInfra  Feature = "infrastructure" // generic signal, applies to ANY agent
	FeatureDomain Feature = "agent_domain"   // specific to this agent's business
	FeatureBoth   Feature = "both"           // generic metric on a domain service (LLM only)
	FeatureNone   Feature = ""               // junk / undecided
)

// Result is the output of Classify.
type Result struct {
	Category      Category
	Feature       Feature
	Confidence    float64
	Source        string // "rule" | "fallback"
	NormalizedTag string // canonical tag1 value
}

// ─── Main Classifier ─────────────────────────────────────────────────

// Classify is the single entry point for rule-based feedback classification.
// tag1, tag2 are the raw on-chain NewFeedback tag values (any casing); scale is
// the detected value tier ("binary"/"star5"/…/"unbounded", may be "" if unknown).
// Safe to call concurrently.
func Classify(tag1, tag2, scale string) Result {
	t1raw := strings.TrimSpace(tag1)
	t2raw := strings.TrimSpace(tag2)
	t1 := strings.ToLower(t1raw)
	t2 := strings.ToLower(t2raw)

	// ── Layer 1: JUNK ─────────────────────────────────────────────
	if isSpam(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: ruleConfidence, Source: "rule"}
	}
	if isNoise(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: ruleConfidence, Source: "rule"}
	}
	if isAllDigitsJunk(t1, t2) {
		return Result{Category: CategoryJunk, Confidence: ruleConfidence, Source: "rule"}
	}
	if uuidRe.MatchString(t1raw) || (t1 == "" && uuidRe.MatchString(t2raw)) {
		return Result{Category: CategoryJunk, Confidence: ruleConfidence, Source: "rule"}
	}
	if t1 == "" && t2 == "" {
		// no tags → let the LLM read offchain content
		return Result{Category: CategoryOthers, Confidence: 0.0, Source: "fallback"}
	}
	if emojiOnlyRe.MatchString(t1raw) && t2 == "" {
		return Result{Category: CategoryJunk, Confidence: ruleConfidence, Source: "rule"}
	}

	// ── Layer 2: QUANTITY — whitelist metric tags only ───────────────
	if isMetric(t1) || isMetric(t2) {
		return Result{Category: CategoryQuantity, Feature: featureOf(t1, t2),
			Confidence: ruleConfidence, Source: "rule", NormalizedTag: t1}
	}

	// ── Layer 3: QUALITY — subjective sentiment / service judgment ───
	if qualityTag1Set[t1] {
		return Result{Category: CategoryQuality, Feature: featureOf(t1, t2),
			Confidence: ruleConfidence, Source: "rule", NormalizedTag: t1}
	}
	if containsQualityKeyword(t1) {
		return Result{Category: CategoryQuality, Feature: featureOf(t1, t2),
			Confidence: ruleConfidence, Source: "rule", NormalizedTag: t1}
	}

	// ── Unknown → escalate to LLM ────────────────────────────────
	return Result{Category: CategoryOthers, Confidence: 0.0, Source: "fallback"}
}

// ─── Helpers ─────────────────────────────────────────────────────────

func isSpam(t1, t2 string) bool {
	return spamURLPattern.MatchString(t1) || spamURLPattern.MatchString(t2) ||
		spamRankPattern.MatchString(t1) || spamRankPattern.MatchString(t2)
}

// isNoise matches explicit placeholder/gibberish tag1 values (test/asd/custom-style).
// Records with empty tag1+tag2 are handled separately (→ others) so the LLM can
// inspect offchain content.
func isNoise(t1, t2 string) bool {
	if noiseTag1Set[t1] && (t2 == "" || noiseTag1Set[t2] || allDigitsRe.MatchString(t2)) {
		return true
	}
	return false
}

// isAllDigitsJunk matches records where BOTH tags are non-empty pure digits.
func isAllDigitsJunk(t1, t2 string) bool {
	return t1 != "" && t2 != "" && allDigitsRe.MatchString(t1) && allDigitsRe.MatchString(t2)
}

// isMetric reports whether a tag names a measured metric (Layer 2 → quantity).
func isMetric(t string) bool {
	if t == "" {
		return false
	}
	return quantityTag1Set[t] || quantityTag2Set[t]
}

func containsQualityKeyword(t string) bool {
	for _, kw := range qualityKeywords {
		if strings.Contains(t, kw) {
			return true
		}
	}
	return false
}

// featureOf assigns the feature axis from tags alone: a generic infra/protocol
// signal → infrastructure; otherwise agent_domain (the default). The "both" value
// needs agent-domain context and is only produced by the LLM.
func featureOf(t1, t2 string) Feature {
	if infraTagSet[t1] || infraTagSet[t2] {
		return FeatureInfra
	}
	return FeatureDomain
}

// IsAnomalousValue returns true when the raw on-chain value looks like an
// un-normalized uint256 (len > 10 chars with zero decimals).
func IsAnomalousValue(rawValue string, valueDecimals int) bool {
	return len(rawValue) > 10 && valueDecimals == 0
}

// NormalizeValue converts an on-chain feedback value to a float64 in [-1, 1].
// Interpretation: value is a percentage in [-100, 100] stored with valueDecimals
// decimal places.  vi = value / (100 * 10^valueDecimals), clamped to [-1, 1].
func NormalizeValue(rawValue string, valueDecimals int) float64 {
	if rawValue == "" || rawValue == "0" {
		return 0.0
	}
	n := new(big.Int)
	if _, ok := n.SetString(rawValue, 10); !ok {
		return 0.0
	}
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

// ─── Tag-scale normalization ──────────────────────────────────────────────────

// RawValueToReal converts an on-chain value string to its real-world float:
// real = value / 10^valueDecimals.  Returns (0.0, false) when empty/non-numeric.
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
