package dto

// agents.go — Public DTOs for agent-facing endpoints.
// These mirror the JSON shapes in docs/API_SPEC_v1.md §2.1, §3.1–§3.11.

// ScoreBreakdown surfaces the 4 component scores that compose the trust score.
// Reputation is the raw accumulator value (unbounded). Services/Publisher/Compliance are normalized to [0, 100].
type ScoreBreakdown struct {
	Reputation float64 `json:"reputation"`
	Services   float64 `json:"services"`
	Publisher  float64 `json:"publisher"`
	Compliance float64 `json:"compliance"`
}

// AgentRow is one row of /leaderboard (§2.1) and /leaderboard/search (§2.2, trimmed via JSON tags).
// TrustScore carries the pre-computed compositeScore in range [0, 100].
type AgentRow struct {
	Rank             int            `json:"rank,omitempty"`
	ChainID          int64          `json:"chainId"`
	AgentID          string         `json:"agentId"`
	Name             string         `json:"name,omitempty"`
	Image            string         `json:"image,omitempty"`
	Owner            string         `json:"owner,omitempty"`
	// TrustScore is the composite trust score in [0, 100], pre-computed by the score-refresh worker.
	TrustScore       float64        `json:"trustScore"`
	// ReputationScore is the raw accumulated reputation value (unbounded, pre-normalization).
	ReputationScore float64        `json:"reputationScore"`
	ScoreBreakdown  ScoreBreakdown `json:"scoreBreakdown"`
	ScoreUpdateAt    int64          `json:"scoreUpdateAt"`
	ConsecutiveFails int64          `json:"consecutiveFails"`
	TotalTasks       int64          `json:"totalTasks"`
	TotalPassed      int64          `json:"totalPassed"`
	TotalFailed      int64          `json:"totalFailed"`
	SuccessRate      float64        `json:"successRate"`
	Active           bool           `json:"active"`
	X402Support      bool           `json:"x402Support"`
	HasOASF          bool           `json:"hasOASF"`
	Domains          []string       `json:"domains,omitempty"`
	OASFSkills       []string       `json:"oasfSkills,omitempty"`
	OASFDomains      []string       `json:"oasfDomains,omitempty"`
	Services         []AgentService `json:"services,omitempty"`
	Tags             []string       `json:"tags,omitempty"`
	CreatedAt        int64          `json:"createdAt,omitempty"`
}

// AgentSearchRow is the compact shape for /leaderboard/search (§2.2).
type AgentSearchRow struct {
	ChainID    int64   `json:"chainId"`
	AgentID    string  `json:"agentId"`
	Name       string  `json:"name,omitempty"`
	Image      string  `json:"image,omitempty"`
	TrustScore float64 `json:"trustScore"`
}

// LeaderboardStats is the payload for /leaderboard/stats (§2.4).
// ChainIDs carries the effective filter when multi-chain; ChainID is 0 in that case.
type LeaderboardStats struct {
	ChainID          int64   `json:"chainId"`
	ChainIDs         []int64 `json:"chainIds,omitempty"`
	TotalAgents      int64   `json:"totalAgents"`
	ActiveAgents     int64   `json:"activeAgents"`
	TotalFeedbacks   int64   `json:"totalFeedbacks"`
	AvgTrustScore    float64 `json:"avgTrustScore"`
	MedianTrustScore float64 `json:"medianTrustScore"`
	Top10ScoreAvg    float64 `json:"top10ScoreAvg"`
	LastBlockIndexed uint64  `json:"lastBlockIndexed"`
	LastIndexedAt    string  `json:"lastIndexedAt,omitempty"`
}

// TagCount is one row of /leaderboard/tags.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int64  `json:"count"`
}

// RisingStarRow is the shape for /leaderboard/rising-stars (§2.3).
type RisingStarRow struct {
	ChainID     int64   `json:"chainId"`
	AgentID     string  `json:"agentId"`
	Name        string  `json:"name,omitempty"`
	Image       string  `json:"image,omitempty"`
	ScoreNow    float64 `json:"scoreNow"`
	ScoreBefore float64 `json:"scoreBefore"`
	Delta       float64 `json:"delta"`
	Velocity    float64 `json:"velocity"`
	Period      string  `json:"period"`
}

// AgentScoring is the scoring sub-object on /agents/:id (§3.1).
// TrustScore carries the pre-computed compositeScore in range [0, 100].
type AgentScoring struct {
	// TrustScore is the composite trust score in [0, 100], pre-computed by the score-refresh worker.
	TrustScore        float64          `json:"trustScore"`
	// ReputationScore is the raw accumulated reputation value (unbounded, pre-normalization).
	// Distinct from ScoreBreakdown.Reputation which is the normalized [0, 100] reputation component.
	ReputationScore   float64          `json:"reputationScore"`
	ScoreBreakdown    ScoreBreakdown   `json:"scoreBreakdown"`
	ScoreUpdateAt     int64            `json:"scoreUpdateAt"`
	ConsecutiveFails  int64            `json:"consecutiveFails"`
	Penalty           float64          `json:"penalty"`
	TotalTasks        int64            `json:"totalTasks"`
	TotalPassed       int64            `json:"totalPassed"`
	TotalFailed       int64            `json:"totalFailed"`
	SuccessRate       float64          `json:"successRate"`
	ClassDistribution map[string]int64 `json:"classDistribution,omitempty"`
}

// OnchainMetadataValue mirrors the repo type but decoupled from BSON.
type OnchainMetadataValue struct {
	RawHex       string `json:"rawHex,omitempty"`
	Decoded      any    `json:"decoded,omitempty"`
	DetectedType string `json:"detectedType,omitempty"`
	Confidence   string `json:"confidence,omitempty"`
}

// AgentService mirrors one OASF/registration service entry.
type AgentService struct {
	Name     string   `json:"name"`
	Endpoint string   `json:"endpoint,omitempty"`
	Version  string   `json:"version,omitempty"`
	Skills   []string `json:"skills,omitempty"`
	Domains  []string `json:"domains,omitempty"`
}

// AgentProfile is the response for /agents/:chainId/:agentId (§3.1).
type AgentProfile struct {
	ChainID          int64                           `json:"chainId"`
	AgentID          string                          `json:"agentId"`
	Owner            string                          `json:"owner,omitempty"`
	AgentURI         string                          `json:"agentURI,omitempty"`
	Name             string                          `json:"name,omitempty"`
	Description      string                          `json:"description,omitempty"`
	Image            string                          `json:"image,omitempty"`
	Active           bool                            `json:"active"`
	X402Support      bool                            `json:"x402Support"`
	SupportedTrust   []string                        `json:"supportedTrust,omitempty"`
	Domains          []string                        `json:"domains,omitempty"`
	HasOASF          bool                            `json:"hasOASF"`
	OASFSkills       []string                        `json:"oasfSkills,omitempty"`
	OASFDomains      []string                        `json:"oasfDomains,omitempty"`
	Services         []AgentService                  `json:"services,omitempty"`
	OnchainMetadata  map[string]OnchainMetadataValue `json:"onchainMetadata,omitempty"`
	OffchainMetadata map[string]any                  `json:"offchainMetadata,omitempty"`
	Scoring          AgentScoring                    `json:"scoring"`
	CreatedAt        string                          `json:"createdAt,omitempty"`
}

// ServiceOverview is one service entry on /agents/:id/overview,
// augmented with a health status derived from the offchain_data cache.
type ServiceOverview struct {
	Name     string   `json:"name"`
	Endpoint string   `json:"endpoint,omitempty"`
	Version  string   `json:"version,omitempty"`
	Skills   []string `json:"skills,omitempty"`
	Domains  []string `json:"domains,omitempty"`
	// Health is "ok" when the endpoint has a cached successful fetch,
	// "warning" when the endpoint fetched but the body is not valid JSON for a
	// JSON-required protocol (oasf, a2a, mcp),
	// "fail" when the most recent fetch attempt recorded an error,
	// and "unknown" when the endpoint has never been probed.
	Health     string `json:"health"`
	HealthInfo string `json:"healthInfo,omitempty"`
}

// AgentOverview is the payload for GET /agents/:chainId/:agentId/overview.
// It powers the "Overview" tab on the agent profile page.
type AgentOverview struct {
	ChainID          int64                           `json:"chainId"`
	AgentID          string                          `json:"agentId"`
	Owner            string                          `json:"owner,omitempty"`
	AgentURI         string                          `json:"agentURI,omitempty"`
	Name             string                          `json:"name,omitempty"`
	Description      string                          `json:"description,omitempty"`
	Image            string                          `json:"image,omitempty"`
	Active           bool                            `json:"active"`
	CreatedAt        string                          `json:"createdAt,omitempty"`
	CreatedTx        string                          `json:"createdTx,omitempty"`
	AgentWallet      string                          `json:"agentWallet,omitempty"`
	Services         []ServiceOverview               `json:"services"`
	OnchainMetadata  map[string]OnchainMetadataValue `json:"onchainMetadata,omitempty"`
	OffchainMetadata map[string]any                  `json:"offchainMetadata,omitempty"`
}

// TrustScorePoint is one entry in /agents/:id/trust-score-history.
// Score is the composite trust score in [0, 100] at that point in time: the reputation
// component varies (replayed from feedback_history), while services/publisher/compliance
// are held at their current values (frozen S/P/C approximation, same as the delta logic).
// type="event" marks the value right after a feedback was applied (incl. penalty when vi<0.40).
// type="decay" marks a midnight UTC sample between events, plus a final "now" sample.
type TrustScorePoint struct {
	Timestamp string  `json:"timestamp"` // RFC3339 UTC
	Score     float64 `json:"score"`     // composite trust score [0, 100]
	Type      string  `json:"type"`      // "event" | "decay"
	TxHash    string  `json:"txHash,omitempty"`
}

// TrustScoreHistoryResult wraps the trust-score timeline response.
type TrustScoreHistoryResult struct {
	Points []TrustScorePoint `json:"points"`
}

// FeedbackRow is one item in /agents/:id/feedbacks (§3.3).
type FeedbackRow struct {
	ID             string                 `json:"_id"`
	FeedbackIndex  uint64                 `json:"feedbackIndex"`
	ClientAddress  string                 `json:"clientAddress"`
	Value          string                 `json:"value"`
	ValueDecimals  uint8                  `json:"valueDecimals"`
	ValueScale     string                 `json:"valueScale,omitempty"`
	Vi             float64                `json:"vi"`
	Wi             float64                `json:"wi"`
	PriceUSDC      float64                `json:"priceUSDC"`
	Tag1           string                 `json:"tag1,omitempty"`
	Tag2           string                 `json:"tag2,omitempty"`
	Endpoint       string                 `json:"endpoint,omitempty"`
	Unit           string                 `json:"unit,omitempty"`
	FeedbackURI    string                 `json:"feedbackURI,omitempty"`
	FeedbackHash   string                 `json:"feedbackHash,omitempty"`
	FeedbackParsed map[string]any         `json:"feedbackParsed,omitempty"`
	Classification FeedbackClassification `json:"classification"`
	TxHash         string                 `json:"txHash"`
	BlockNumber    uint64                 `json:"blockNumber"`
	Timestamp      string                 `json:"timestamp"`
	TimestampUnix  int64                  `json:"timestampUnix,omitempty"` // event time, Unix seconds (omitted when unknown)
	RevokeTxHash   string                 `json:"revokeTxHash,omitempty"`
	Responses      []FeedbackResponse     `json:"responses,omitempty"`
}

// RuleClassification mirrors the rule-engine verdict for API consumers.
type RuleClassification struct {
	Category string `json:"category"`
}

// FallbackClassification mirrors the LLM fallback verdict — present only when
// the LLM fallback actually ran.
type FallbackClassification struct {
	Category   string  `json:"category"`
	Reason     string  `json:"reason,omitempty"`
	Confidence float64 `json:"confidence"`
}

// FeedbackClassification is the JSON-facing view of classifier output.
type FeedbackClassification struct {
	Rule     RuleClassification      `json:"rule"`
	Fallback *FallbackClassification `json:"fallback,omitempty"`
}

// FeedbackResponse is one replay of a responseAppended event.
type FeedbackResponse struct {
	Responder      string         `json:"responder"`
	ResponseURI    string         `json:"responseURI,omitempty"`
	ResponseHash   string         `json:"responseHash,omitempty"`
	TxHash         string         `json:"txHash"`
	ResponseParsed map[string]any `json:"responseParsed,omitempty"`
}

// FeedbackDetail extends FeedbackRow with the fetched off-chain payload (§3.4).
type FeedbackDetail struct {
	FeedbackRow
	OffchainParsed map[string]any `json:"offchainParsed,omitempty"`
}

// IdentityEvent is one row for /agents/:id/identity-history (§3.5).
type IdentityEvent struct {
	EventName   string         `json:"eventName"`
	AgentURI    string         `json:"agentURI,omitempty"`
	Owner       string         `json:"owner,omitempty"`
	EventArgs   map[string]any `json:"eventArgs,omitempty"`
	URIParsed   map[string]any `json:"uriParsed,omitempty"`
	BlockNumber uint64         `json:"blockNumber"`
	TxHash      string         `json:"txHash"`
	Timestamp   string         `json:"timestamp"`
}

// PenaltyRow is one row for /agents/:id/penalties (§3.8).
type PenaltyRow struct {
	FeedbackIndex  uint64  `json:"feedbackIndex"`
	ClientAddress  string  `json:"clientAddress"`
	Timestamp      string  `json:"timestamp"`
	Vi             float64 `json:"vi"`
	Wi             float64 `json:"wi"`
	PenaltyApplied float64 `json:"penaltyApplied,omitempty"`
	Reason         string  `json:"reason"` // "fail" | "revoked"
	TxHash         string  `json:"txHash"`
	RevokeTxHash   string  `json:"revokeTxHash,omitempty"`
}

// WalletFeedbackRow extends FeedbackRow with cross-agent context for GET /wallet/:address/feedbacks.
// AgentID and ChainID identify which agent the feedback was submitted to.
type WalletFeedbackRow struct {
	FeedbackRow
	AgentID   string `json:"agentId"`
	ChainID   int64  `json:"chainId"`
	AgentName string `json:"agentName,omitempty"`
}

// ProofResponse is the /agents/:id/proof/:txHash payload (§3.10).
type ProofResponse struct {
	ChainID      int64          `json:"chainId"`
	TxHash       string         `json:"txHash"`
	BlockNumber  uint64         `json:"blockNumber"`
	EventName    string         `json:"eventName"`
	ContractType string         `json:"contractType"`
	Args         map[string]any `json:"args,omitempty"`
	FeedbackURI  string         `json:"feedbackURI,omitempty"`
	ResponseURIs []string       `json:"responseURIs,omitempty"`
}

// AgentRegistration is one row in the cross-chain registrations list returned by
// GET /agents/:chainId/:agentId/registrations.
type AgentRegistration struct {
	ChainID   int64  `json:"chainId"`
	AgentID   string `json:"agentId"`
	Name      string `json:"name,omitempty"`
	Active    bool   `json:"active"`
	IsCurrent bool   `json:"isCurrent"`
}

// AgentRegistrationList is the response payload for
// GET /agents/:chainId/:agentId/registrations. MatchedBy values:
//   - "agentWallet"        : grouped by a non-empty agentWallet
//   - "owner+contentHash"  : grouped by owner with identical identity content hash
//   - "self"               : agentWallet and owner both empty; only the requested registration is returned
type AgentRegistrationList struct {
	AgentWallet   string              `json:"agentWallet,omitempty"`
	MatchedBy     string              `json:"matchedBy"`
	Registrations []AgentRegistration `json:"registrations"`
}
