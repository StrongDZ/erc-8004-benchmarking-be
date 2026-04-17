package uri

// uri.go — Pure domain logic for extracting and normalising agent URIs from decoded events.
// This package avoids infrastructure imports (no mongo, mq, repository).

import (
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/utils"
)

// Event is the minimal representation of a decoded on-chain event needed by this package.
// App layer is responsible for mapping from repository types to this struct.
type Event struct {
	EventName string
	Args      map[string]any
	ChainID   int64
	TxHash    string
	LogIndex  uint
	DecodedAt int64
}

// ExtractURIFromEvent returns the URI carried by identity or reputation events that include URI payloads.
func ExtractURIFromEvent(event Event) (string, bool) {
	switch event.EventName {
	case "Registered":
		return utils.GetStringArg(event.Args, "agentURI")
	case "URIUpdated":
		return utils.GetStringArg(event.Args, "newURI")
	case "NewFeedback":
		return utils.GetStringArg(event.Args, "feedbackURI")
	case "ResponseAppended":
		return utils.GetStringArg(event.Args, "responseURI")
	default:
		return "", false
	}
}

// BuildAgentURIMessage creates an AgentURIMessage from a decoded event and an extracted URI.
// eventID must be pre-computed by the caller (e.g. repository.EventDocumentID).
func BuildAgentURIMessage(event Event, uri, source, eventID string) mq.AgentURIMessage {
	return mq.AgentURIMessage{
		URI:       uri,
		ChainID:   event.ChainID,
		EventID:   eventID,
		DecodedAt: event.DecodedAt,
		EventName: event.EventName,
		Source:    source,
	}
}
