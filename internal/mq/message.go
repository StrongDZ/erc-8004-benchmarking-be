package mq

// message.go — Queue names and payload types for the ERC-8004 message bus.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"erc-8004-benchmarking-be/internal/repository/contracts"
)

// Queue name constants.
const (
	QueueRawLogs             = "erc8004.raw_logs"
	QueueAgentURI            = "erc8004.agent_uri"
	QueueFeedbackOthers      = "erc8004.feedback.others"
	QueueAgentDescSummary    = "erc8004.agent.desc.summary"
	QueueWalletEnrich        = "erc8004.wallet_enrich"
	QueueScoreRefreshTrigger = "erc8004.scorerefresh.trigger"
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

// FeedbackOthersMessage is published to QueueFeedbackOthers by the trustrank
// processor for feedback the rule classifier could not decide (category "others").
// These rows are written ungraded; the async others-grader consumes them, runs the
// LLM fallback, and persists the verdict/weighting. Rule-decided feedback is graded
// inline at ingest and is never published here.
type FeedbackOthersMessage struct {
	FeedbackID string `json:"feedbackId"`
	ChainID    int64  `json:"chainId"`
}

// AgentDescSummaryMessage is published to QueueAgentDescSummary by the trustrank
// identity processor whenever an agent's description becomes non-empty or changes.
// The desc-summarizer worker consumes it, calls the AI service /summarize endpoint,
// and writes the result to agents.summarizedDescription.
// DescHash is sha256[:16] of strings.TrimSpace(Description); the consumer uses it
// as an idempotency key against agents.summarizedDescriptionHash.
type AgentDescSummaryMessage struct {
	ChainID     int64  `json:"chainId"`
	AgentID     string `json:"agentId"`
	Description string `json:"description"`
	DescHash    string `json:"descHash"`
	PublishedAt int64  `json:"publishedAt"`
	Attempt     int    `json:"attempt,omitempty"` // retry counter; 0 = first delivery
}

// WalletEnrichMessage is published to QueueWalletEnrich whenever UpsertCold
// (directly or via ReconcileOwnership) inserts a brand-new wallet document.
// The wallet-enrich worker consumes it and runs the cache-first external
// trust enrichment pass (RPC balance+nonce, then Etherscan age+counterparties)
// for this wallet.
type WalletEnrichMessage struct {
	ChainID int64  `json:"chainId"`
	Address string `json:"address"` // lowercase, per utils.NormalizeAddress
}

// PublishWalletEnrich publishes a WalletEnrichMessage to QueueWalletEnrich when
// wasNew is true and publisher is non-nil; otherwise it is a no-op. Callers pass
// through the wasNew flag returned by UpsertCold/ReconcileOwnership so the
// wallet-enrich worker only ever sees brand-new wallets.
func PublishWalletEnrich(ctx context.Context, publisher Publisher, wasNew bool, chainID int64, address string) error {
	if !wasNew || publisher == nil {
		return nil
	}
	return publisher.Publish(ctx, QueueWalletEnrich, WalletEnrichMessage{ChainID: chainID, Address: address})
}

// RecomputeTriggerMessage is published to QueueScoreRefreshTrigger by the API
// process to request an out-of-band authoritative score replay. ChainID == 0
// means replay every chain (matches runCycle's existing full-corpus behaviour);
// a non-zero value scopes the replay to one chain (used by the simulator to
// replay only its sandbox chain).
type RecomputeTriggerMessage struct {
	ChainID   int64  `json:"chainId"`
	RequestID string `json:"requestId"`
}

// DescHash returns the sha256[:16] hex digest used as the description idempotency key.
// Callers should normalise (TrimSpace) the description before hashing; this helper enforces it.
func DescHash(description string) string {
	trimmed := strings.TrimSpace(description)
	if trimmed == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:8]) // 8 bytes -> 16 hex chars
}
