package crawler

// types.go — Types for crawler progress tracking.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// CrawlMode identifies the crawling strategy for a contract.
type CrawlMode string

const (
	CrawlModeEvent CrawlMode = "event"
)

// CrawlerState tracks crawl progress for one (chainId, contractAddress, mode) tuple.
type CrawlerState struct {
	ID                 string    `bson:"_id"` // CrawlerDocumentID(chainId, contractAddress, mode)
	ChainID            int64     `bson:"chainId"`
	ContractAddress    string    `bson:"contractAddress"`
	Mode               CrawlMode `bson:"mode"`
	LastProcessedBlock uint64    `bson:"lastProcessedBlock"`
	Status             string    `bson:"status"`
	LastError          string    `bson:"lastError,omitempty"`
	ActiveRPC          string    `bson:"activeRpc,omitempty"` // RPC URL of last successful crawl step
	UpdatedAt          int64     `bson:"updatedAt"`           // Unix seconds UTC
}

// Repository wraps the crawlers collection.
type Repository struct {
	mongorepo.MongoRepoImpl[CrawlerState]
}
