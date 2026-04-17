package websocket

// message.go — Wire format for realtime events broadcast over WebSocket.
// Published by consumer on Redis Pub/Sub, forwarded by API to connected clients.

// ChannelRealtimeEvents is the default Redis Pub/Sub channel for decoded events.
// Keep this in sync with config.Config.RedisEventsChannel default.
const ChannelRealtimeEvents = "erc8004:events:realtime"

// Message types.
const (
	TypeEventDecoded = "event.decoded"
	TypePing         = "ping"
	TypePong         = "pong"
)

// RealtimeEvent is the JSON frame pushed to FE clients.
// One-shot envelope: clients can switch on `type`.
//
// For "event.decoded", the minimal fields the dashboard needs are:
//   eventName, contractType, chainId, agentId, txHash, blockNumber, timestamp.
// `args` carries the full decoded arg map for callers that want richer detail.
type RealtimeEvent struct {
	Type         string         `json:"type"`
	EventName    string         `json:"eventName,omitempty"`
	ContractType string         `json:"contractType,omitempty"`
	ChainID      int64          `json:"chainId,omitempty"`
	AgentID      string         `json:"agentId,omitempty"`
	TxHash       string         `json:"txHash,omitempty"`
	BlockNumber  uint64         `json:"blockNumber,omitempty"`
	LogIndex     uint           `json:"logIndex,omitempty"`
	Timestamp    int64          `json:"timestamp,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
}
