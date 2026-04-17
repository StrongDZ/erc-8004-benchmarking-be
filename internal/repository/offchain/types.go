package offchain

// types.go — Types for offchain_data documents.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// OffchainData stores resolved JSON (or fetch error) keyed by stable hash of the original URI.
type OffchainData struct {
	ID           string `bson:"_id"` // OffchainDocumentID(uri)
	URI          string `bson:"uri"`
	Content      string `bson:"content,omitempty"`
	SourceType   string `bson:"sourceType,omitempty"`   // data | ipfs | https
	EventType    string `bson:"eventType,omitempty"`    // Registered | URIUpdated | NewFeedback | ResponseAppended
	ContractType string `bson:"contractType,omitempty"` // identity | reputation | validation
	FetchError   string `bson:"fetchError,omitempty"`
	ContentSize  int    `bson:"contentSize,omitempty"` // len(Content) in bytes (UTF-8)
}

// Repository wraps the offchain_data collection.
type Repository struct {
	mongorepo.MongoRepoImpl[OffchainData]
}
