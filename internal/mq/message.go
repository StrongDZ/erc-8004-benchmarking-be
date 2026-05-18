package mq

// message.go — Queue names and payload types for the ERC-8004 message bus.

import (
	"context"
	"fmt"

	"erc-8004-benchmarking-be/internal/repository/contracts"
)

// Queue name constants.
const (
	QueueRawLogs            = "erc8004.raw_logs"
	QueueAgentURI           = "erc8004.agent_uri"
	QueueFeedbackClassified = "erc8004.feedback.classified"
)

// EventURIQueueName returns the per-chain queue for event-sourced URI messages.
// Format: uri.{chainID}.event_uri
func EventURIQueueName(chainID int64) string {
	return fmt.Sprintf("uri.%d.event_uri", chainID)
}

// ServiceURIQueueName returns the per-chain queue for service-endpoint URI messages.
// Format: uri.{chainID}.service_uri
func ServiceURIQueueName(chainID int64) string {
	return fmt.Sprintf("uri.%d.service_uri", chainID)
}

// Publisher publishes JSON messages to arbitrary named queues.
// Implemented by rabbitmq.MultiPublisher; defined here at the consumer site.
type Publisher interface {
	Publish(ctx context.Context, queueName string, msg any) error
}

// RawLogMessage is published by the indexer and consumed by the decoder.
// It carries the full log plus the contract's ABI so the consumer can decode
// args without additional DB lookups.
type RawLogMessage struct {
	ChainID         int64                        `json:"chainId"`
	ContractAddress string                       `json:"contractAddress"`
	ContractType    string                       `json:"contractType"`
	EventABI        []contracts.ABIEventFragment `json:"eventABI"`
	BlockNumber     uint64                       `json:"blockNumber"`
	BlockHash       string                       `json:"blockHash"`
	TxHash          string                       `json:"txHash"`
	LogIndex        uint                         `json:"logIndex"`
	Topics          []string                     `json:"topics"` // hex strings, topics[0] = event signature hash
	Data            string                       `json:"data"`   // ABI-encoded non-indexed args, hex without 0x prefix
	Removed         bool                         `json:"removed"`
	Timestamp       int64                        `json:"timestamp"`  // block time, Unix seconds UTC
	IngestedAt      int64                        `json:"ingestedAt"` // crawler write time, Unix seconds UTC
}

// AgentURIMessage is published to uri.{chainID}.event_uri by the uri-bootstrap
// producer (historical scan). BlockNumber and LogIndex identify the source event
// position for the consumer-driven uri_fetched cursor.
type AgentURIMessage struct {
	URI          string `json:"uri"`
	ChainID      int64  `json:"chainId"`
	EventID      string `json:"eventId"` // EventDocumentID(chainId, txHash, logIndex)
	BlockNumber  uint64 `json:"blockNumber"`
	LogIndex     uint   `json:"logIndex"`
	DecodedAt    int64  `json:"decodedAt"`
	EventName    string `json:"eventName,omitempty"`
	ContractType string `json:"contractType,omitempty"` // identity | reputation | validation (from decoded event)
	Source       string `json:"source"`                 // bootstrap | live
}

// ServiceURIMessage is published to uri.{chainID}.service_uri by the TrustRank processor
// when it encounters service endpoints extracted from identity registry agent cards.
type ServiceURIMessage struct {
	URI         string `json:"uri"`
	ChainID     int64  `json:"chainId"`
	AgentID     string `json:"agentId,omitempty"`
	ServiceName string `json:"serviceName,omitempty"`
	PublishedAt int64  `json:"publishedAt"`
}

// FeedbackClassifiedMessage is published to QueueFeedbackClassified by the trustrank
// processor after a feedback record has been classified and written to feedback_history.
type FeedbackClassifiedMessage struct {
	FeedbackID string `json:"feedbackId"`
	ChainID    int64  `json:"chainId"`
}
