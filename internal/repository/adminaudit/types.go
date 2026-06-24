package adminaudit

// types.go — Types for the admin_audit_log collection.
// Append-only record of every /admin/* access (indexer monitoring, scoring
// recompute, simulator control), independent of whether the request was
// authorized — unauthorized attempts are recorded too.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// Entry is one /admin/* access record.
type Entry struct {
	Timestamp  int64  `bson:"timestamp"` // Unix seconds UTC
	Action     string `bson:"action"`    // e.g. "indexer-status", "scoring-recompute", "simulator-start"
	Method     string `bson:"method"`
	Path       string `bson:"path"`
	RemoteIP   string `bson:"remoteIp"`
	Authorized bool   `bson:"authorized"`
	StatusCode int    `bson:"statusCode"`
	RequestID  string `bson:"requestId,omitempty"`
}

// Repository wraps the admin_audit_log collection.
type Repository struct {
	mongorepo.MongoRepoImpl[Entry]
}
