package wallet

// types.go — Types for the wallets collection.
// A Wallet is a unified node representing any Ethereum address that either owns
// agents (kind="owner") or has submitted feedback (kind="user"). The kind is
// derived from OwnedAgentIDs (non-empty → owner).

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// WalletKind enumerates the role of a wallet node in the trust scoring system.
type WalletKind string

const (
	WalletKindUser  WalletKind = "user"
	WalletKindOwner WalletKind = "owner"
)

// WalletDocument stores a wallet node in the trust scoring system.
type WalletDocument struct {
	ID      string `bson:"_id"`     // {chainId}:{address}, address lowercased
	Address string `bson:"address"` // lowercased hex
	ChainID int64  `bson:"chainId"`
	Kind    string `bson:"kind"` // "user" | "owner" (derived)

	// Base score (write-time, incrementally updated by the trustrank worker).
	TrustScore float64 `bson:"trustScore"` // [0, 100]

	// Timestamp of the last trustScore write (rewritten each score-refresh cycle).
	TrustScoreAt int64 `bson:"trustScoreAt,omitempty"`

	// TrustRated is true when this wallet received trust inflow (owns ≥1 agent with weightMass>0).
	// False means no trust evidence; trustScore is nil/unset for unrated wallets.
	TrustRated bool `bson:"trustRated"`

	// Aggregates.
	OwnedAgentIDs      []string `bson:"ownedAgentIds,omitempty"`
	FeedbackTotalCount int64    `bson:"feedbackTotalCount"`
	FeedbackValidCount int64    `bson:"feedbackValidCount"`
	FeedbackJunkCount  int64    `bson:"feedbackJunkCount"`
	JunkRatio          float64  `bson:"junkRatio"`

	CreatedAt int64 `bson:"createdAt"`
	UpdatedAt int64 `bson:"updatedAt"`

	External ExternalDoc `bson:"external"`
}

// ExternalDoc is the external on-chain trust enrichment for a wallet (1:1).
// Score is partial-renormed until Complete. Present is true once any feature
// has been written (gates the teleport blend).
type ExternalDoc struct {
	Score           float64 `bson:"score"`
	Complete        bool    `bson:"complete"`
	Present         bool    `bson:"present"`
	BalanceUSD      float64 `bson:"balanceUSD"`
	Nonce           uint64  `bson:"nonce"`
	AgeDays         float64 `bson:"ageDays"`
	Counterparties  int     `bson:"counterparties"`
	ENS             string  `bson:"ens,omitempty"`
	ENSAvatar       string  `bson:"ensAvatar,omitempty"`
	CheapAt         int64   `bson:"cheapAt,omitempty"`
	ExplorerAt      int64   `bson:"explorerAt,omitempty"`
	ExplorerSkipped bool    `bson:"explorerSkipped,omitempty"` // true when explorer API permanently unavailable for this chain
	ENSAt           int64   `bson:"ensAt,omitempty"`
	CheapFetched    bool    `bson:"cheapFetched"`
	RichFetched     bool    `bson:"richFetched"`
	ENSFetched      bool    `bson:"ensFetched"`
}

// Repository wraps the wallets collection.
type Repository struct {
	mongorepo.MongoRepoImpl[WalletDocument]
}
