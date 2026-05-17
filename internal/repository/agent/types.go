package agent

// types.go — Types for the agents collection.

import (
	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// RegistrationService is one element of the ERC-8004 registration file "services" array.
type RegistrationService struct {
	Name     string   `bson:"name,omitempty" json:"name"`
	Endpoint string   `bson:"endpoint,omitempty" json:"endpoint"`
	Version  string   `bson:"version,omitempty" json:"version"`
	Skills   []string `bson:"skills,omitempty" json:"skills,omitempty"`
	Domains  []string `bson:"domains,omitempty" json:"domains,omitempty"`
}

// IdentityFields holds denormalized registration file fields stored on AgentDocument.
type IdentityFields struct {
	AgentURI       string
	Name           string
	Type           string
	Image          string
	Domains        []string
	Description    string
	Services       []RegistrationService
	Active         bool
	SupportedTrust []string
	X402Support    bool
}

// OnchainMetadataValue stores decoded metadata with provenance confidence.
// Decoded holds JSON-native values when applicable (e.g. []any for tag arrays, map for objects);
// scalars remain string or number as decoded from the chain bytes.
type OnchainMetadataValue struct {
	RawHex       string `bson:"rawHex,omitempty" json:"rawHex,omitempty"`
	Decoded      any    `bson:"decoded,omitempty" json:"decoded,omitempty"`
	DetectedType string `bson:"detectedType,omitempty" json:"detectedType,omitempty"`
	Confidence   string `bson:"confidence,omitempty" json:"confidence,omitempty"`
}

// AgentDocument stores the current snapshot of an agent: on-chain identity + scoring state.
type AgentDocument struct {
	ID               string   `bson:"_id"`     // {chainId}:{agentId}
	AgentID          string   `bson:"agentId"` // decimal string of uint256
	ChainID          int64    `bson:"chainId"`
	Owner            string   `bson:"owner"`
	AgentWallet      string   `bson:"agentWallet,omitempty"` // EIP-712-verified wallet from setAgentWallet(); first-class field
	AgentURI         string   `bson:"agentURI"`
	Name             string   `bson:"name,omitempty"`
	Type             string   `bson:"type,omitempty"`
	Domains          []string `bson:"domains,omitempty"`
	Image            string   `bson:"image,omitempty"`
	Description      string   `bson:"description,omitempty"`
	Registrations    []string `bson:"registrations,omitempty"`  // CAIP-10 multi-chain identity array (AgentURI Profile v1.3)
	CardUpdatedAt    string   `bson:"cardUpdatedAt,omitempty"`  // ISO-8601 updatedAt from agent card JSON
	Services         []RegistrationService `bson:"services,omitempty"`
	Active           bool     `bson:"active"`      // do not use omitempty — must persist false
	X402Support      bool     `bson:"x402Support"` // do not use omitempty — must persist false
	SupportedTrust   []string `bson:"supportedTrust,omitempty"`
	LegacyWarnings   []string `bson:"legacyWarnings,omitempty"` // WA031 and other spec deprecation warnings
	OASFSkills       []string `bson:"oasfSkills,omitempty"`   // normalized OASF skill paths for querying
	OASFDomains      []string `bson:"oasfDomains,omitempty"`  // normalized OASF domain paths for querying
	HasOASF          bool     `bson:"hasOASF"`                 // quick filter for OASF-supporting agents
	Tags             []string `bson:"tags,omitempty"`          // denormalized from onchainMetadata.tags for filtering
	OnchainMetadata  map[string]OnchainMetadataValue `bson:"onchainMetadata,omitempty"` // from MetadataSet events
	OffchainMetadata map[string]any    `bson:"offchainMetadata,omitempty"` // from agentURI JSON (non-fixed fields)
	AccumulatedScore float64  `bson:"accumulatedScore"`
	ScoreUpdateAt    int64    `bson:"scoreUpdateAt"` // Unix seconds of last score update
	ConsecutiveFails int64    `bson:"consecutiveFails"`
	TotalTasks       int64    `bson:"totalTasks"`
	TotalPassed      int64    `bson:"totalPassed"`
	TotalFailed      int64    `bson:"totalFailed"`
	CreatedAt        int64    `bson:"createdAt,omitempty"` // omit zero from upsert $set; insert uses $setOnInsert in repo
}

// Repository wraps the agents collection.
type Repository struct {
	mongorepo.MongoRepoImpl[AgentDocument]
}
