package scorestats

import mongorepo "erc-8004-benchmarking-be/internal/repository"

// AgentScoreStats is a materialized-view document in the agent_score_stats collection.
// One document per (chainId, agentId), updated every ~30 min by the score-refresh worker.
// This is the single source of truth for all scoring and accumulator fields.
type AgentScoreStats struct {
	ChainID     int64   `bson:"chainId"`
	AgentID     string  `bson:"agentId"`
	ReputationScore float64 `bson:"reputationScore"` // current reputationScore (penalty baked in, no display decay)
	Delta24h    float64 `bson:"delta24h"`         // score now − score 24 h ago (0 if no checkpoint before 24 h)
	Delta7d     float64 `bson:"delta7d"`          // score now − score 7 d ago
	Delta30d    float64 `bson:"delta30d"`         // score now − score 30 d ago
	Consistency float64 `bson:"consistency"`      // [0, 1]: 1 = perfectly consistent event scores

	// O(1) write-path accumulators (replicated from trustrank write path each cycle).
	ScoreUpdateAt    int64 `bson:"scoreUpdateAt"`    // Unix seconds of last reputation update
	ConsecutiveFails int64 `bson:"consecutiveFails"` // current consecutive failure streak
	TotalTasks       int64 `bson:"totalTasks"`       // total scored tasks (non-revoked, non-self)
	TotalPassed      int64 `bson:"totalPassed"`      // tasks with vi >= 0.40
	TotalFailed      int64 `bson:"totalFailed"`      // tasks with vi < 0.40

	// Composite trust score [0, 100] and component breakdown.
	// Populated by score-refresh worker each cycle.
	CompositeScore  float64  `bson:"compositeScore"`
	ReputationNorm  float64  `bson:"reputationNorm"`   // normalized reputation [0,100]
	ServicesScore   float64  `bson:"servicesScore"`
	PublisherScore  float64  `bson:"publisherScore"`
	ComplianceScore float64  `bson:"complianceScore"`
	// ServiceWarnings lists service names that need attention
	// (e.g., JSON-required endpoint fetched but content was not valid JSON).
	ServiceWarnings []string `bson:"serviceWarnings,omitempty"`

	ComputedAt int64 `bson:"computedAt"` // Unix seconds of last computation
}

// Repository wraps the agent_score_stats collection.
type Repository struct {
	mongorepo.MongoRepoImpl[AgentScoreStats]
}
