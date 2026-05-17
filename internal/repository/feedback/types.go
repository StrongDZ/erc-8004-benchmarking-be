package feedback

// types.go — Types for the feedback_history collection.

import (
	"go.mongodb.org/mongo-driver/bson"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// FeedbackResponse represents one ResponseAppended event.
type FeedbackResponse struct {
	Responder      string         `bson:"responder"`
	ResponseURI    string         `bson:"responseURI"`
	ResponseHash   string         `bson:"responseHash"`
	TxHash         string         `bson:"txHash"`
	ResponseParsed map[string]any `bson:"responseParsed,omitempty"`
}

// RuleClassification holds the rule-engine verdict — always present.
type RuleClassification struct {
	Category string `bson:"category"` // service_feedback | config_feedback | app_specific | spam | noise | others
}

// FallbackClassification holds the LLM verdict — populated only when the LLM
// fallback actually ran (i.e. rule returned "others" AND an LLM client was wired).
type FallbackClassification struct {
	Category   string  `bson:"category"`
	Reason     string  `bson:"reason,omitempty"`
	Confidence float64 `bson:"confidence"`
}

// FeedbackClassification persists both the rule verdict and (when LLM ran)
// the fallback verdict so downstream consumers can separately query each.
type FeedbackClassification struct {
	Rule     RuleClassification      `bson:"rule"`
	Fallback *FallbackClassification `bson:"fallback,omitempty"`
}

// FeedbackRecord stores a single feedback event and its scoring impact.
// Schema follows ERC-8004 feedback_history collection spec.
type FeedbackRecord struct {
	ID             string                 `bson:"_id"` // {chainId}:{agentId}:{clientAddress}:{feedbackIndex}
	AgentID        string                 `bson:"agentId"`
	ChainID        int64                  `bson:"chainId"`
	ClientAddress  string                 `bson:"clientAddress"`
	FeedbackIndex  uint64                 `bson:"feedbackIndex"`
	Value          string                 `bson:"value"` // int128 as string
	ValueDecimals  uint8                  `bson:"valueDecimals"`
	Tag1           string                 `bson:"tag1,omitempty"`
	Tag2           string                 `bson:"tag2,omitempty"`
	Endpoint       string                 `bson:"endpoint,omitempty"`
	FeedbackURI    string                 `bson:"feedbackURI,omitempty"`
	FeedbackHash   string                 `bson:"feedbackHash,omitempty"`
	FeedbackParsed map[string]any         `bson:"feedbackParsed,omitempty"`
	BlockNumber    uint64                 `bson:"blockNumber"`
	TxHash         string                 `bson:"txHash"`
	LogIndex       uint                   `bson:"logIndex"`
	Timestamp      int64                  `bson:"timestamp"`
	Type           string                 `bson:"type"` // reputation_feedback, etc.
	PriceUSDC      float64                `bson:"priceUSDC"`
	Wi             float64                `bson:"wi"`                     // difficulty weight at time of scoring
	ValueScale     string                 `bson:"valueScale,omitempty"`   // scale used to compute vi: "binary"|"star5"|"star10"|"pct100"|"unbounded"|""
	Classification FeedbackClassification `bson:"classification"`          // set by classifier
	Unit           string                 `bson:"unit,omitempty"`           // display unit: "ms", "s", "%", "blocks", "USDC", "none", etc.
	IsSelfFeedback bool                   `bson:"isSelfFeedback,omitempty"` // clientAddress == owner or agentWallet
	Responses      []FeedbackResponse     `bson:"responses,omitempty"`     // ResponseAppended history
	RevokeTxHash   string                 `bson:"revokeTxHash,omitempty"`  // set when FeedbackRevoked
	IsRevoked      bool                   `bson:"isRevoked,omitempty"`     // true after FeedbackRevoked
	RevokedAt      int64                  `bson:"revokedAt,omitempty"`     // Unix seconds of revocation event
}

// FeedbackUpdate holds a partial update for a single feedback document.
type FeedbackUpdate struct {
	ID     string // document _id
	Update bson.M // full update document (e.g. {"$set": {...}, "$push": {...}})
}

// Repository wraps the feedback_history collection.
type Repository struct {
	mongorepo.MongoRepoImpl[FeedbackRecord]
}
