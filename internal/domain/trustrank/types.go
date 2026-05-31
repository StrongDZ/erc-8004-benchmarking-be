package trustrank

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"erc-8004-benchmarking-be/internal/domain/scoring"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	"erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/repository/offchain"
	"erc-8004-benchmarking-be/internal/repository/scorestats"
	"erc-8004-benchmarking-be/internal/repository/tagstats"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

// EventProcessor processes a batch of decoded events for scoring/identity/feedback.
type EventProcessor interface {
	ProcessBatch(ctx context.Context, chainID int64, events []eventrepo.DecodedEvent) error
}

// URIPublisher publishes service-endpoint URI messages to the service_uri queue.
// Implemented by an infrastructure adapter; defined here at the consumer site.
type URIPublisher interface {
	Publish(ctx context.Context, queueName string, msg any) error
}

// FeedbackPublisher publishes classified feedback IDs to the trust-graph worker queue.
// May be nil; when nil, publishing is skipped.
type FeedbackPublisher interface {
	Publish(ctx context.Context, queueName string, msg any) error
}

// DescSummaryPublisher publishes AgentDescSummaryMessage events whenever an agent's
// description becomes non-empty or changes. May be nil; when nil, publishing is skipped.
type DescSummaryPublisher interface {
	Publish(ctx context.Context, queueName string, msg any) error
}

// App orchestrates Stream 2: parallel per-chain event processing each cron tick.
type App struct {
	Contracts  *contractsrepo.ContractsRepository
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

// registrationsJSON accepts multiple JSON shapes and extracts CAIP-10 entries.
// Some cards send registrations as object instead of []string.
type registrationsJSON []string

func (r *registrationsJSON) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*r = nil
		return nil
	}

	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	out := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	collectRegistrations(raw, &out, seen)
	if len(out) == 0 {
		*r = nil
		return nil
	}
	*r = registrationsJSON(out)
	return nil
}

func collectRegistrations(v any, out *[]string, seen map[string]struct{}) {
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" || !isCaip10(s) {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		*out = append(*out, s)
	case []any:
		for _, item := range t {
			collectRegistrations(item, out, seen)
		}
	case map[string]any:
		// Best-effort: extract any CAIP-10 strings nested in object payloads.
		for _, item := range t {
			collectRegistrations(item, out, seen)
		}
	}
}

// updatedAtJSON accepts string or number and stores a normalized string form.
type updatedAtJSON string

func (u *updatedAtJSON) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*u = ""
		return nil
	}

	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	switch v := raw.(type) {
	case string:
		*u = updatedAtJSON(strings.TrimSpace(v))
	case float64:
		if v == float64(int64(v)) {
			*u = updatedAtJSON(strconv.FormatInt(int64(v), 10))
		} else {
			*u = updatedAtJSON(strconv.FormatFloat(v, 'f', -1, 64))
		}
	default:
		*u = ""
	}
	return nil
}

// agentCard unmarshals the ERC-8004 agent registration file JSON (identity URI).
// The "services" field is captured as raw JSON and parsed via ParseServices()
// because real-world cards use many incompatible shapes (object, array-of-objects,
// array-of-strings) and field types (skills/domains as numbers).
// Unknown fields are captured in Extra for offchain_metadata.
type agentCard struct {
	Name           string            `json:"name"`
	Type           string            `json:"type"`
	Description    string            `json:"description"`
	Domains        []string          `json:"domains"`
	Image          string            `json:"image"`
	ImageURI       string            `json:"imageURI"`
	RawServices    json.RawMessage   `json:"services"`
	Active         bool              `json:"active"`
	SupportedTrust []string          `json:"supportedTrust"`
	X402Support    x402SupportJSON   `json:"x402Support"`
	Registrations  registrationsJSON `json:"registrations"` // CAIP-10 identifiers (AgentURI Profile v1.3)
	UpdatedAt      updatedAtJSON     `json:"updatedAt"`     // ISO-8601 or unix timestamp
	Extra          map[string]any    `json:"-"`             // populated after unmarshal
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

// cachedScale holds a detected scale with an expiry for the in-memory cache.
type cachedScale struct {
	Scale     string
	ExpiresAt int64 // Unix seconds
}

// Processor implements EventProcessor with real domain logic.
type Processor struct {
	agentRepo        *agent.Repository
	statsRepo        *scorestats.Repository
	identityRepo     *identityrepo.Repository
	feedbackRepo     *feedback.Repository
	offchainRepo     *offchain.Repository
	formulaCfg       scoring.FormulaConfig
	compositeWeights scoring.CompositeWeights
	uriPublisher     URIPublisher    // may be nil; publishes service endpoint URIs asynchronously
	fbPublisher      FeedbackPublisher // may be nil; publishes classified feedback IDs
	descPublisher    DescSummaryPublisher // may be nil; publishes agent description-summary jobs
	walletRepo       *wallet.Repository // may be nil; syncs owner → wallets for trust graph
	coldStartT0      float64            // initial trustScore for new owner wallets
	tagStatsRepo     *tagstats.StatsRepository
	tagCorrsRepo     *tagstats.CorrectionRepository
	tagScaleCache    sync.Map // tag1 -> cachedScale
	minSamples       int      // minimum feedbacks before scale is locked
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
	pendingIdentity    []identityrepo.IdentityChange
	pendingFeedbacks   []feedback.FeedbackRecord
	pendingFBUpdates   []feedback.FeedbackUpdate
	pendingServiceURIs  []mq.ServiceURIMessage       // service endpoint URIs to publish asynchronously
	pendingDescSummary  []mq.AgentDescSummaryMessage // description-summary jobs to publish asynchronously
	pendingTierUpdates []tagstats.TierUpdate  // tag scale votes to flush in Phase 3
	dirtyAgents        map[string]bool        // agentIDs that were created or mutated
}
