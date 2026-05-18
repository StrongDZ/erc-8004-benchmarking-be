package classifier

// rule_patterns.go — Compiled regex patterns and lookup sets used by Classify().
// Keeping these in a separate file lets classifier.go stay focused on pipeline logic.

import "regexp"

// ─── Compiled Regex Patterns ──────────────────────────────────────────────────

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

// ─── Lookup Sets ──────────────────────────────────────────────────────────────

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
