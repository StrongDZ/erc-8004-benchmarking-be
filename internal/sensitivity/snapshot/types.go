package snapshot

// types.go — Snapshot domain types shared by builder, writer, reader.

// SnapshotMeta is the document stored in bachelor_sensitivity_index.snapshots.
// One record per snapshot; _id is the snapshot ID (e.g. "snap_20260531_143025").
type SnapshotMeta struct {
	ID                string         `bson:"_id"`
	Label             string         `bson:"label"`
	CreatedAt         int64          `bson:"createdAt"` // unix seconds
	ChainID           int64          `bson:"chainId"`
	Filters           FilterConfig   `bson:"filters"`
	Stats             SnapshotStats  `bson:"stats"`
	SourceDB          string         `bson:"sourceDb"`
	SourceLastEventTs int64          `bson:"sourceLastEventTs"` // unix seconds of most recent feedback in source
	GitCommitSHA      string         `bson:"gitCommitSha,omitempty"`
}

// FilterConfig records the filters applied during snapshot creation, so re-runs
// with the same filters are reproducible.
type FilterConfig struct {
	Category        string `bson:"category"`        // default "quality" (only quality feeds the trust score)
	IncludeRevoked  bool   `bson:"includeRevoked"`  // default false
	IncludeSelf     bool   `bson:"includeSelf"`     // default false
	MinFeedbacks    int    `bson:"minFeedbacks"`    // default 1 — agents must have ≥ this many
	IncludeSpam     bool   `bson:"includeSpam"`     // default false — when true, also include junk/quantity/others
}

// SnapshotStats records counts inserted during snapshot creation.
type SnapshotStats struct {
	Agents    int64 `bson:"agents"`
	Feedbacks int64 `bson:"feedbacks"`
	Edges     int64 `bson:"edges"`
}

// AgentSnapshot is one document in <snapshot_db>.agents.
// Denormalized: contains list of FeedbackIDs for fast per-agent iteration.
type AgentSnapshot struct {
	ID            string   `bson:"_id"`            // agentId (chain-agnostic — chainId is stored separately)
	ChainID       int64    `bson:"chainId"`
	Owner         string   `bson:"owner"`
	AgentWallet   string   `bson:"agentWallet,omitempty"`
	FeedbackIDs   []string `bson:"feedbackIds"`    // list of feedback document _id's
	FeedbackCount int      `bson:"feedbackCount"`  // len(FeedbackIDs)
	Verified      bool     `bson:"verified"`       // computed flag for "good" label heuristic
}

// FeedbackSnapshot is one document in <snapshot_db>.feedbacks.
// Schema mirrors feedback.FeedbackRecord but only retains fields needed for SA.
type FeedbackSnapshot struct {
	ID                   string  `bson:"_id"`
	AgentID              string  `bson:"agentId"`
	ChainID              int64   `bson:"chainId"`
	ClientAddress        string  `bson:"clientAddress"`
	FeedbackIndex        uint64  `bson:"feedbackIndex"`
	ValueNormalized      float64 `bson:"valueNormalized"`        // vi ∈ [-1,1] computed via classifier.NormalizeValueWithScale
	ValueScale           string  `bson:"valueScale,omitempty"`
	Wi                   float64 `bson:"wi"`                     // copied from source for cross-check
	ClassifierConfidence float64 `bson:"classifierConfidence"`
	Timestamp            int64   `bson:"timestamp"`              // unix seconds
	IsRevoked            bool    `bson:"isRevoked"`
	IsSelfFeedback       bool    `bson:"isSelfFeedback"`
	Category             string  `bson:"category"`               // effective top-level category: junk|quality|quantity|others
	// Quality signals (for Cụm C inputs)
	ReasoningLen      int  `bson:"reasoningLen"`
	AttachmentCount   int  `bson:"attachmentCount"`
	HasRatingBreakdown bool `bson:"hasRatingBreakdown"`
	HasProofOfPayment  bool `bson:"hasProofOfPayment"`
}

// GraphEdge is one document in <snapshot_db>.graph_edges.
// Directed: from FromWallet (client/feedback giver) → ToWallet (agent owner).
type GraphEdge struct {
	ID               string  `bson:"_id"`               // "{from}:{to}"
	FromWallet       string  `bson:"fromWallet"`        // client address
	ToWallet         string  `bson:"toWallet"`          // agent owner (or agentWallet)
	FeedbackCount    int     `bson:"feedbackCount"`     // number of feedbacks from→to
	AvgQualityScore  float64 `bson:"avgQualityScore"`   // Q ∈ [0,1] avg across feedbacks
}

// BaselineScore is one document in <snapshot_db>.baseline_scores.
// Computed with default formula parameters at snapshot creation time.
// Mirrors the v2 agent_score_stats component set so Cụm B can re-blend the
// 5-component composite and Cụm D can compute WalletTrust per owner.
type BaselineScore struct {
	AgentID          string  `bson:"_id"`           // agentId
	ChainID          int64   `bson:"chainId"`
	ReputationScore  float64 `bson:"reputationScore"`
	AdoptionScore    float64 `bson:"adoptionScore"`
	ServicesScore    float64 `bson:"servicesScore"`
	PublisherScore   float64 `bson:"publisherScore"`
	PublisherPresent bool    `bson:"publisherPresent"`
	ComplianceScore  float64 `bson:"complianceScore"`
	CompositeScore   float64 `bson:"compositeScore"`
	WeightMass       float64 `bson:"weightMass"`    // B = Σ wᵢ·dᵢ — evidence mass for Cụm D O-component (owned-agent quality)
	ExternalScore    float64 `bson:"externalScore"` // owner wallet on-chain score [0,100], 0 when unenriched (Cụm D E-component)
}
