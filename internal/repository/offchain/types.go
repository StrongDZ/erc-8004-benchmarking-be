package offchain

// types.go — Types for offchain_data documents.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// Fetch status constants stored in OffchainData.Status.
const (
	StatusFetchFailed    = -1 // HTTP/network error — content not available
	StatusFetchedNotJSON = 1  // fetched successfully but content is not valid JSON
	StatusFetchedJSON    = 5  // fetched successfully and content is valid JSON
)

// OffchainData stores resolved content (or fetch error) keyed by stable hash of the original URI.
type OffchainData struct {
	ID           string `bson:"_id"`                    // OffchainDocumentID(uri)
	URI          string `bson:"uri"`
	Status       int    `bson:"status"`                 // -1 | 1 | 5 — see Status* constants
	Content      string `bson:"content,omitempty"`
	SourceType   string `bson:"sourceType,omitempty"`   // data | ipfs | https | arweave | plain
	EventType    string `bson:"eventType,omitempty"`    // Registered | URIUpdated | NewFeedback | ResponseAppended
	ContractType string `bson:"contractType,omitempty"` // identity | reputation | validation
	FetchError   string `bson:"fetchError,omitempty"`
	ContentSize  int    `bson:"contentSize,omitempty"` // len(Content) in bytes (UTF-8)
}

// Repository wraps the offchain_data collection.
type Repository struct {
	mongorepo.MongoRepoImpl[OffchainData]
}
