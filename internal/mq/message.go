package mq

// message.go — Queue names and payload types for the ERC-8004 message bus.

import "erc-8004-benchmarking-be/internal/repository/config"

// Queue name constants.
const (
	QueueRawLogs = "erc8004.raw_logs"
	QueueAgentURI = "erc8004.agent_uri"
)

// RawLogMessage is published by the indexer and consumed by the decoder.
// It carries the full log plus the contract's ABI so the consumer can decode
// args without additional DB lookups.
type RawLogMessage struct {
	ChainID         int64                      `json:"chainId"`
	ContractAddress string                     `json:"contractAddress"`
	ContractType    string                     `json:"contractType"`
	EventABI        []config.ABIEventFragment  `json:"eventABI"`
	BlockNumber     uint64                     `json:"blockNumber"`
	BlockHash       string                     `json:"blockHash"`
	TxHash          string                     `json:"txHash"`
	LogIndex        uint                       `json:"logIndex"`
	Topics          []string                   `json:"topics"` // hex strings, topics[0] = event signature hash
	Data            string                     `json:"data"`   // ABI-encoded non-indexed args, hex without 0x prefix
	Removed         bool                       `json:"removed"`
	Timestamp       int64                      `json:"timestamp"`  // block time, Unix seconds UTC
	IngestedAt      int64                      `json:"ingestedAt"` // crawler write time, Unix seconds UTC
}

// AgentURIMessage is published by both:
// - bootstrap URI producer (historical events <= decodedAt watermark)
// - decode consumer live stream (new identity URI events)
type AgentURIMessage struct {
	URI       string `json:"uri"`
	ChainID   int64  `json:"chainId"`
	EventID   string `json:"eventId"` // EventDocumentID(chainId, txHash, logIndex)
	DecodedAt int64  `json:"decodedAt"`
	EventName string `json:"eventName,omitempty"`
	Source    string `json:"source"` // bootstrap | live
}
