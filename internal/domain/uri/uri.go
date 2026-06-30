package uri

// uri.go — Pure domain logic for extracting and normalising agent URIs from decoded events.
// This package avoids infrastructure imports (no mongo, mq, repository).

import (
	"erc-8004-benchmarking-be/internal/utils"
)

// Event is the minimal representation of a decoded on-chain event needed by this package.
// App layer is responsible for mapping from repository types to this struct.
type Event struct {
	EventName    string
	Args         map[string]any
	ChainID      int64
	TxHash       string
	BlockNumber  uint64
	LogIndex     uint
	DecodedAt    int64
	ContractType string
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
