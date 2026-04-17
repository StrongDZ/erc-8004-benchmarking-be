package trustrank

import (
	"context"
	"encoding/json"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/score"
	"strings"
	"time"
)

// EventProcessor processes a batch of decoded events for scoring/identity/feedback.
type EventProcessor interface {
	ProcessBatch(ctx context.Context, chainID int64, events []eventrepo.DecodedEvent) error
}

// App orchestrates Stream 2: parallel per-chain event processing each cron tick.
type App struct {
	Contracts  *configrepo.ContractsRepository
	Events     *eventrepo.Repository
	ConfigRepo *configrepo.ConfigRepository
	Interval   time.Duration
	BatchSize  int64
	Proc       EventProcessor
}

// x402SupportJSON accepts bool or object in registration JSON (some cards use a config object).
type x402SupportJSON bool

func (x *x402SupportJSON) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*x = false
		return nil
	}
	switch b[0] {
	case 't', 'f':
		var v bool
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*x = x402SupportJSON(v)
		return nil
	case '{', '[':
		*x = true
		return nil
	default:
		var v bool
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*x = x402SupportJSON(v)
		return nil
	}
}

// agentCard unmarshals the ERC-8004 agent registration file JSON (identity URI).
// The "services" field is captured as raw JSON and parsed via ParseServices()
// because real-world cards use many incompatible shapes (object, array-of-objects,
// array-of-strings) and field types (skills/domains as numbers).
// Unknown fields are captured in Extra for offchain_metadata.
type agentCard struct {
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Domains        []string        `json:"domains"`
	Image          string          `json:"image"`
	ImageURI       string          `json:"imageURI"`
	RawServices    json.RawMessage `json:"services"`
	Active         bool            `json:"active"`
	SupportedTrust []string        `json:"supportedTrust"`
	X402Support    x402SupportJSON `json:"x402Support"`
	Extra          map[string]any  `json:"-"` // populated after unmarshal
}

// ParseServices parses the raw "services" JSON value tolerantly.
// Accepted shapes:
//   - null / absent                       → nil
//   - single object {"name":"..."}        → []RegistrationService{...}
//   - array of objects [{"name":"..."}, ] → []RegistrationService{...}
//   - array of strings ["svc"]            → nil (no useful data)
//
// Within each service object, skills and domains may contain non-string
// elements (numbers, nested objects); those elements are coerced to string
// or skipped, so the overall parse never fails.
func (c *agentCard) ParseServices() []agent.RegistrationService {
	return parseServicesJSON(c.RawServices)
}

// rawService is used only during JSON parsing; uses []any to tolerate
// heterogeneous element types in skills and domains.
type rawService struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Version  string `json:"version"`
	Skills   []any  `json:"skills"`
	Domains  []any  `json:"domains"`
}

func (r rawService) toRegistrationService() agent.RegistrationService {
	return agent.RegistrationService{
		Name:     r.Name,
		Endpoint: r.Endpoint,
		Version:  r.Version,
		Skills:   anySliceToStrings(r.Skills),
		Domains:  anySliceToStrings(r.Domains),
	}
}

func anySliceToStrings(items []any) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		switch v := item.(type) {
		case string:
			s := strings.TrimSpace(v)
			if s != "" {
				out = append(out, s)
			}
		case map[string]any:
			// Some cards model skills/domains as objects ({id,name,...}).
			// We only persist the canonical path-like "name" value.
			if name, ok := v["name"].(string); ok {
				s := strings.TrimSpace(name)
				if s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func parseServicesJSON(raw json.RawMessage) []agent.RegistrationService {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	// Try as array of raw elements.
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err == nil {
		result := make([]agent.RegistrationService, 0, len(elems))
		for _, elem := range elems {
			var svc rawService
			if err := json.Unmarshal(elem, &svc); err == nil && svc.Name != "" {
				result = append(result, svc.toRegistrationService())
			}
			// String elements or unparseable objects are silently skipped.
		}
		return result
	}

	// Try as single object.
	var svc rawService
	if err := json.Unmarshal(raw, &svc); err == nil && svc.Name != "" {
		return []agent.RegistrationService{svc.toRegistrationService()}
	}

	return nil
}

// Processor implements EventProcessor with real domain logic.
type Processor struct {
	agentRepo    *agent.Repository
	identityRepo *identityrepo.Repository
	feedbackRepo *feedback.Repository
	scoreRepo    *score.Repository
	offchainRepo *offchain.Repository
	formulaCfg   scoring.FormulaConfig
}

// batchState holds all in-memory maps and write buffers for a single batch.
type batchState struct {
	chainID    int64
	formulaCfg scoring.FormulaConfig

	// prefetched data
	agentMap map[string]*agent.AgentDocument     // agentID -> doc (mutated in-place)
	uriMap   map[string]string                   // URI -> JSON content
	fbMap    map[string]*feedback.FeedbackRecord // feedback doc _id -> record

	// write buffers
	pendingIdentity  []identityrepo.IdentityChange
	pendingFeedbacks []feedback.FeedbackRecord
	pendingSnapshots []struct {
		ChainID  int64
		AgentID  string
		Snapshot score.ScoreSnapshotItem
	}
	pendingFBUpdates []feedback.FeedbackUpdate
	dirtyAgents      map[string]bool // agentIDs that were created or mutated
}
