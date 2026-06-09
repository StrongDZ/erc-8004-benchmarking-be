package wallet

// types.go — Types for the wallets collection.
// A Wallet is a unified node representing any Ethereum address that either owns
// agents (kind="owner") or has submitted feedback (kind="user"). The kind is
// derived from OwnedAgentIDs (non-empty → owner).

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// WalletKind enumerates the role of a wallet node in the trust graph.
type WalletKind string

const (
	WalletKindUser  WalletKind = "user"
	WalletKindOwner WalletKind = "owner"
)

// WalletDocument stores a wallet node in the trust propagation graph.
type WalletDocument struct {
	ID      string `bson:"_id"`     // {chainId}:{address}, address lowercased
	Address string `bson:"address"` // lowercased hex
	ChainID int64  `bson:"chainId"`
	Kind    string `bson:"kind"`    // "user" | "owner" (derived)

	// Base score (write-time, incrementally updated by trust-graph-updater).
	TrustScore float64 `bson:"trustScore"` // [0, 100]

	// Post-propagation score timestamp (batch pass writes this).
	PropagationUpdatedAt int64 `bson:"propagationUpdatedAt,omitempty"`

	// TrustRated is true when this wallet received trust inflow (owns ≥1 agent with weightMass>0).
	// False means no trust evidence; trustScore is nil/unset for unrated wallets.
	TrustRated bool `bson:"trustRated"`

	// Aggregates.
	OwnedAgentIDs       []string `bson:"ownedAgentIds,omitempty"`
	FeedbackTotalCount  int64    `bson:"feedbackTotalCount"`
	FeedbackValidCount  int64    `bson:"feedbackValidCount"`
	FeedbackJunkCount   int64    `bson:"feedbackJunkCount"`
	JunkRatio           float64  `bson:"junkRatio"`

	CreatedAt int64 `bson:"createdAt"`
	UpdatedAt int64 `bson:"updatedAt"`
}

// Repository wraps the wallets collection.
type Repository struct {
	mongorepo.MongoRepoImpl[WalletDocument]
}
