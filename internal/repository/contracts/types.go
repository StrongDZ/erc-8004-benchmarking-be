package contracts

// types.go — Types for contracts/registry documents.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// ContractType identifies the ERC-8004 registry type.
type ContractType string

const (
	ContractTypeIdentity   ContractType = "identity"
	ContractTypeReputation ContractType = "reputation"
)

// ABIEventInput matches a Solidity JSON ABI event input object.
type ABIEventInput struct {
	Indexed      bool   `bson:"indexed,omitempty"      json:"indexed,omitempty"`
	InternalType string `bson:"internalType,omitempty" json:"internalType,omitempty"`
	Name         string `bson:"name,omitempty"         json:"name,omitempty"`
	Type         string `bson:"type,omitempty"         json:"type,omitempty"`
}

// ABIEventFragment matches a Solidity JSON ABI event object (type "event").
type ABIEventFragment struct {
	Anonymous bool            `bson:"anonymous,omitempty" json:"anonymous,omitempty"`
	Inputs    []ABIEventInput `bson:"inputs,omitempty"    json:"inputs,omitempty"`
	Name      string          `bson:"name,omitempty"      json:"name,omitempty"`
	Type      string          `bson:"type,omitempty"      json:"type,omitempty"`
}

// ContractTarget describes one monitored contract within a chain config.
type ContractTarget struct {
	Type       ContractType       `bson:"type"`
	Address    string             `bson:"address"`
	EventABI   []ABIEventFragment `bson:"eventABI,omitempty"`
	StartBlock uint64             `bson:"startBlock,omitempty"`
}

// ContractsConfig is the per-chain crawler configuration document.
type ContractsConfig struct {
	ID        int64            `bson:"_id"` // equals chainId
	ChainID   int64            `bson:"chainId"`
	RPCs      []string         `bson:"rpcs"`
	Contracts []ContractTarget `bson:"contracts"`
	Active    bool             `bson:"active"`
}

// ContractsRepository wraps the contracts collection.
type ContractsRepository struct {
	mongorepo.MongoRepoImpl[ContractsConfig]
}
