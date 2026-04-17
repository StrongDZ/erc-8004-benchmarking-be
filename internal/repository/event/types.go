package event

// types.go — Types for decoded on-chain events.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// DecodedEvent is the document stored in the "events" collection after ABI decoding.
type DecodedEvent struct {
	ID              string         `bson:"_id"` // EventDocumentID(chainId, txHash, logIndex)
	ChainID         int64          `bson:"chainId"`
	ContractAddress string         `bson:"contractAddress"`
	ContractType    string         `bson:"contractType"` // identity | reputation | validation
	EventName       string         `bson:"eventName"`    // Registered, NewFeedback, …
	BlockNumber     uint64         `bson:"blockNumber"`
	BlockHash       string         `bson:"blockHash"`
	TxHash          string         `bson:"txHash"`
	LogIndex        uint           `bson:"logIndex"`
	Args            map[string]any `bson:"args"` // decoded parameter map
	Removed         bool           `bson:"removed"`
	Timestamp       int64          `bson:"timestamp"`  // block time, Unix seconds UTC
	IngestedAt      int64          `bson:"ingestedAt"` // crawler write time, Unix seconds UTC
	DecodedAt       int64          `bson:"decodedAt"`  // consumer decode time, Unix seconds UTC
}

// Repository wraps the events collection.
type Repository struct {
	mongorepo.MongoRepoImpl[DecodedEvent]
}

// ChainEventCursor is an exclusive lower bound for pagination on one chain:
// next page contains events strictly after (BlockNumber, LogIndex).
type ChainEventCursor struct {
	BlockNumber uint64
	LogIndex    uint
}
