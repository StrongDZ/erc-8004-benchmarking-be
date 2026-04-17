package identity

// types.go — Types for the identity_history collection.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// IdentityChange records a single identity event (Registered, URIUpdated, MetadataSet).
type IdentityChange struct {
	ID          string         `bson:"_id"` // {chainId}:{txHash}:{logIndex}
	AgentID     string         `bson:"agentId"`
	ChainID     int64          `bson:"chainId"`
	EventName   string         `bson:"eventName"` // Registered | URIUpdated | MetadataSet
	AgentURI    string         `bson:"agentURI,omitempty"`
	Owner       string         `bson:"owner,omitempty"`
	EventArgs   map[string]any `bson:"eventArgs,omitempty"`
	URIParsed   map[string]any `bson:"uriParsed,omitempty"`
	BlockNumber uint64         `bson:"blockNumber"`
	TxHash      string         `bson:"txHash"`
	LogIndex    uint           `bson:"logIndex"`
	Timestamp   int64          `bson:"timestamp"`
}

// Repository wraps the identity_history collection.
type Repository struct {
	mongorepo.MongoRepoImpl[IdentityChange]
}
