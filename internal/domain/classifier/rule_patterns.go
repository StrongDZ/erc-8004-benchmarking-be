package classifier

// rule_patterns.go — patterns + lookup sets for the two-axis cascade classifier.
//   category: junk → quantity → quality  (unknown escalates to "others" for the LLM)
//   feature : infrastructure | agent_domain   (assigned for quality & quantity)
// Sets are intentionally CONSERVATIVE: the rule engine resolves only the clear
// cases; ambiguous tags fall through to "others" so the LLM (prompt V6) decides.

import "regexp"

// ─── Compiled Regex Patterns ──────────────────────────────────────────────────

var (
	spamURLPattern  = regexp.MustCompile(`(?i)(t\.me/|telegram\.me|https?://|http://)`)
	spamRankPattern = regexp.MustCompile(`(?i)(get\s+top|top\s*[0-9]|-{2,}>|#1\s+rank)`)
	uuidRe          = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	emojiOnlyRe     = regexp.MustCompile(`^[\x{1F000}-\x{1FFFF}\x{2600}-\x{27FF}\s]+$`)
	allDigitsRe     = regexp.MustCompile(`^[0-9]+$`)
)

// ─── Layer 1: JUNK ──────────────────────────────────────────────────────────

// noiseTag1Set: meaningless placeholder / gibberish tokens → junk.
var noiseTag1Set = map[string]bool{
	"test": true, "asd": true, "custom": true, "settled": true,
	"claudelance": true, "vibez": true,
}

// ─── Layer 2: QUANTITY (category) — tag names a measured metric ───────────────

// quantityTag1Set: exact tag values that are measured metrics (rate/score/count/
// speed/status). A SCORE counts as a metric (no quality-dimension exception).
var quantityTag1Set = map[string]bool{
	"reachable": true, "liveness": true, "successrate": true, "success-rate": true,
	"responsetime": true, "response-time": true, "blocktimefreshness": true,
	"blocktime-freshness": true, "blocktime freshness": true, "uptime": true, "creditscore": true,
	"attendance-rate": true, "completion-rate": true, "execution-speed": true,
	"payment-speed": true, "settlement-speed": true, "win-rate": true,
	"coverage-rate": true, "exit-rate": true, "active": true, "safety-score": true, "contractrisk": true,
	"counterparty": true, "longevity": true,
}

// quantityTag2Set: tag2 discriminators that mark the record as a measured metric.
var quantityTag2Set = map[string]bool{
	"oracle-screening": true, "liveness-check": true, "win-rate": true,
	"coverage-rate": true, "exit-rate": true, "automated-screening": true,
	"completion-rate": true, "scroll-stop-rate": true,
}

// ─── Layer 3: QUALITY (category) — subjective sentiment / service judgment ────

// qualityTag1Set: clear quality/sentiment/service-judgment tags → quality.
// (Metric-shaped terms like "score"/"accuracy"/"speed" are deliberately NOT here;
// they go to quantity or escalate to the LLM.)
var qualityTag1Set = map[string]bool{
	"trustscore": true, "trust-score": true,
	"starred": true, "quality": true, "performance": true, "service": true,
	"helpful": true, "fast": true, "reliable": true, "reliability": true,
	"excellence": true, "excellent": true, "satisfaction": true, "experience": true,
	"value": true, "rating": true, "good": true, "robust": true, "secure": true,
	"audited": true, "innovative": true, "transparent": true, "trustless": true,
	"composable": true, "interoperable": true, "cool": true, "great": true,
	"review": true, "useful": true, "smart": true, "nice": true, "overall": true,
	"creative": true, "support": true, "intelligent": true, "analytical": true,
	"usability": true, "compliance": true, "peer-review": true,
	"content-moderation": true, "amazing": true, "awesome": true, "beautiful": true,
	"professional": true, "impressive": true, "outstanding": true, "best": true,
	// common typos
	"helpfull": true, "powerfull": true, "usefull": true, "reliabel": true,
	"excelent": true,
}

// qualityKeywords: fuzzy contains for lower-confidence quality detection.
var qualityKeywords = []string{
	"helpful", "fast", "reliable", "quality", "excellent", "good",
	"useful", "great", "smart", "simple", "easy", "smooth", "solid",
	"stable", "best", "nice", "clean", "amazing", "awesome", "love",
}

// ─── Feature axis: INFRASTRUCTURE vs AGENT_DOMAIN ─────────────────────────────

// infraTagSet: generic infra/protocol signals that would apply to ANY agent
// regardless of business → feature = infrastructure. Everything else defaults to
// agent_domain. (Rule engine has no agent context, so "both" is only the LLM's.)
var infraTagSet = map[string]bool{
	"reachable": true, "liveness": true, "liveness-check": true, "uptime": true,
	"responsetime": true, "response-time": true, "ping": true, "health-check": true,
	"blocktimefreshness": true, "blocktime-freshness": true, "blocktime freshness": true, "a2a": true, "mcp": true,
	"web": true, "trust-oracle": true, "oracle-screening": true, "trust-score": true,
	"trustscore": true, "reputation": true, "sentinel8004": true, "agentguard": true,
	"safety-score": true, "ownerverified": true, "owner verified": true,
}
