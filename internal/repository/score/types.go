package score

// types.go — Types for the score_history collection (agent-centric design).

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// ScoreSnapshotItem represents a single score snapshot entry in the history array.
type ScoreSnapshotItem struct {
	AgentScore float64 `bson:"agent_score"` // displayed score S clamped [0, 1000]
	Type       string  `bson:"type"`        // "decay" | "event"
	TxHash     string  `bson:"txHash,omitempty"`
	EventScore float64 `bson:"event_score,omitempty"` // only present when type="event"
	Timestamp  int64   `bson:"timestamp"`             // Unix seconds
}

// ScoreHistory stores the full score timeline for a single agent on a single chain.
// Each document = one agent. The score_snapshot array grows with each event/decay cycle.
type ScoreHistory struct {
	ID             string              `bson:"_id,omitempty"`
	ChainID        int64               `bson:"chainId"`
	AgentID        string              `bson:"agentId"`
	ScoreSnapshots []ScoreSnapshotItem `bson:"score_snapshot"` // ordered by timestamp ascending
}

// Repository wraps the score_history collection.
type Repository struct {
	mongorepo.MongoRepoImpl[ScoreHistory]
}
