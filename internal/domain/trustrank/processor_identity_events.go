package trustrank

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	decoder "erc-8004-benchmarking-be/internal/decoder/evm"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/agent"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	"erc-8004-benchmarking-be/internal/utils"
)

func (p *Processor) processIdentityEvent(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	switch ev.EventName {
	case "Registered":
		p.handleRegistered(bs, agentID, ev)
	case "URIUpdated":
		p.handleURIUpdated(bs, agentID, ev)
	case "MetadataSet":
		p.handleMetadataSet(bs, agentID, ev)
	case "Transfer":
		p.handleTransfer(bs, agentID, ev)
	}
}

func (p *Processor) handleRegistered(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	owner, _ := utils.GetStringArg(ev.Args, "owner")
	agentURI, _ := utils.GetStringArg(ev.Args, "agentURI")

	doc := bs.agentMap[agentID]
	if doc == nil {
		doc = &agent.AgentDocument{
			AgentID: agentID,
			ChainID: bs.chainID,
		}
		bs.agentMap[agentID] = doc
	}
	doc.Owner = owner
	doc.AgentURI = agentURI
	doc.CreatedAt = ev.Timestamp
	bs.dirtyAgents[agentID] = true

	p.appendIdentityHistory(bs, agentID, ev, agentURI, owner)

	if agentURI != "" {
		p.applyIdentityFromURI(bs, agentID, agentURI)
	}
}

func (p *Processor) handleURIUpdated(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	newURI, _ := utils.GetStringArg(ev.Args, "newURI")

	p.appendIdentityHistory(bs, agentID, ev, newURI, "")

	if newURI != "" {
		doc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)
		doc.AgentURI = newURI
		bs.dirtyAgents[agentID] = true
		p.applyIdentityFromURI(bs, agentID, newURI)
	}
}

func (p *Processor) handleMetadataSet(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	metadataKey, _ := utils.GetStringArg(ev.Args, "metadataKey")
	metadataValue, _ := utils.GetStringArg(ev.Args, "metadataValue")

	p.appendIdentityHistory(bs, agentID, ev, "", "")

	if metadataKey == "" {
		return
	}

	doc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)

	if doc.OnchainMetadata == nil {
		doc.OnchainMetadata = make(map[string]agent.OnchainMetadataValue)
	}

	decoded := decoder.DecodeMetadataValue(metadataKey, metadataValue)

	if decoded.DetectedType == "empty" {
		delete(doc.OnchainMetadata, metadataKey)
		if strings.EqualFold(metadataKey, "tags") {
			doc.Tags = nil
		}
		if strings.EqualFold(metadataKey, "agentWallet") {
			doc.AgentWallet = ""
		}
	} else {
		doc.OnchainMetadata[metadataKey] = agent.OnchainMetadataValue{
			RawHex:       decoded.RawHex,
			Decoded:      decoded.Decoded,
			DetectedType: decoded.DetectedType,
			Confidence:   decoded.Confidence,
		}
		if strings.EqualFold(metadataKey, "tags") {
			doc.Tags = decoder.ParseTagsFromDecoded(decoded.Decoded)
		}
		// agentWallet is a reserved EIP-712-bound key — also promoted to first-class field.
		if strings.EqualFold(metadataKey, "agentWallet") {
			if addr, ok := decoded.Decoded.(string); ok && addr != "" {
				doc.AgentWallet = addr
			}
		}
	}

	bs.dirtyAgents[agentID] = true
}

func (p *Processor) handleTransfer(bs *batchState, agentID string, ev eventrepo.DecodedEvent) {
	to, ok := utils.GetStringArg(ev.Args, "to")
	if !ok || to == "" {
		log.Printf("processor: Transfer missing 'to' address: chain=%d agent=%s", bs.chainID, agentID)
		return
	}
	doc := p.getOrCreateAgent(bs, agentID, ev.Timestamp)
	doc.Owner = to
	bs.dirtyAgents[agentID] = true
	p.appendIdentityHistory(bs, agentID, ev, "", to)
}

func (p *Processor) appendIdentityHistory(bs *batchState, agentID string, ev eventrepo.DecodedEvent, uri, owner string) {
	var uriParsed map[string]any
	if strings.TrimSpace(uri) != "" {
		uriParsed = parseJSONObject(bs.uriMap[uri])
	}
	bs.pendingIdentity = append(bs.pendingIdentity, identityrepo.IdentityChange{
		AgentID:     agentID,
		ChainID:     bs.chainID,
		EventName:   ev.EventName,
		AgentURI:    uri,
		Owner:       owner,
		EventArgs:   cloneArgs(ev.Args),
		URIParsed:   uriParsed,
		BlockNumber: ev.BlockNumber,
		TxHash:      ev.TxHash,
		LogIndex:    ev.LogIndex,
		Timestamp:   ev.Timestamp,
	})
}

func (p *Processor) applyIdentityFromURI(bs *batchState, agentID, uri string) {
	content, ok := bs.uriMap[uri]
	if !ok {
		return
	}

	var card agentCard
	if err := json.Unmarshal([]byte(content), &card); err != nil {
		log.Printf("processor: parse agent card JSON for %s: %v", agentID, err)
		return
	}

	var rawMap map[string]any
	if err := json.Unmarshal([]byte(content), &rawMap); err == nil {
		knownFields := []string{
			"name", "description", "domains", "image", "imageURI",
			"services", "active", "supportedTrust", "x402Support",
			"registrations", "updatedAt",
		}
		card.Extra = make(map[string]any)
		for k, v := range rawMap {
			isKnown := false
			for _, known := range knownFields {
				if strings.EqualFold(k, known) {
					isKnown = true
					break
				}
			}
			if !isKnown && v != nil {
				card.Extra[k] = v
			}
		}
	}

	doc := bs.agentMap[agentID]
	if doc == nil {
		return
	}

	img := strings.TrimSpace(card.Image)
	if img == "" {
		img = strings.TrimSpace(card.ImageURI)
	}

	var warnings []string
	merged, hasLegacyEndpoints := mergeServicesWithEndpoints(card.ParseServices(), card.Extra["endpoints"])
	if hasLegacyEndpoints {
		// WA031: legacy endpoints[] field detected; agents should migrate to services[].
		warnings = append(warnings, "WA031: legacy endpoints[] field used; migrate to services[]")
		log.Printf("processor: WA031 legacy endpoints detected: chain=%d agent=%s uri=%s", bs.chainID, agentID, uri)
	}

	doc.AgentURI = uri
	doc.Name = card.Name
	doc.Type = strings.TrimSpace(card.Type)
	doc.Image = img
	doc.Domains = card.Domains
	doc.Description = card.Description
	doc.Services = merged
	doc.Active = card.Active
	doc.SupportedTrust = card.SupportedTrust
	doc.X402Support = bool(card.X402Support)
	doc.Registrations = filterCaip10([]string(card.Registrations))
	doc.CardUpdatedAt = strings.TrimSpace(string(card.UpdatedAt))
	if len(warnings) > 0 {
		doc.LegacyWarnings = warnings
	}

	oasfSkills, oasfDomains, hasOASF := ExtractOASFCapabilities(doc.Services)
	doc.OASFSkills = oasfSkills
	doc.OASFDomains = oasfDomains
	doc.HasOASF = hasOASF

	if len(card.Extra) > 0 {
		doc.OffchainMetadata = card.Extra
	}

	bs.dirtyAgents[agentID] = true

	// Enqueue service endpoint URIs for async fetch by the service_uri consumer.
	if p.uriPublisher != nil {
		now := time.Now().Unix()
		for _, svc := range merged {
			ep := strings.TrimSpace(svc.Endpoint)
			if ep == "" {
				continue
			}
			bs.pendingServiceURIs = append(bs.pendingServiceURIs, mq.ServiceURIMessage{
				URI:         ep,
				ChainID:     bs.chainID,
				AgentID:     agentID,
				ServiceName: svc.Name,
				PublishedAt: now,
			})
		}
	}
}

// filterCaip10 returns only strings that match the CAIP-10 format eip155:{chainId}:{address}.
func filterCaip10(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, s := range items {
		s = strings.TrimSpace(s)
		if isCaip10(s) {
			out = append(out, s)
		} else if s != "" {
			log.Printf("processor: skipping invalid CAIP-10 registration entry: %q", s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// isCaip10 validates the basic CAIP-10 format: eip155:{chainId}:{address}.
func isCaip10(s string) bool {
	if !strings.HasPrefix(s, "eip155:") {
		return false
	}
	rest := s[len("eip155:"):]
	colon := strings.Index(rest, ":")
	if colon < 1 {
		return false
	}
	addr := rest[colon+1:]
	return len(addr) >= 40 && strings.HasPrefix(addr, "0x")
}

// mergeServicesWithEndpoints deduplicates services[] and merges legacy endpoints[].
// Returns the merged list and a boolean indicating whether legacy endpoints were present (WA031).
func mergeServicesWithEndpoints(
	services []agent.RegistrationService,
	endpointsRaw any,
) ([]agent.RegistrationService, bool) {
	merged := make([]agent.RegistrationService, 0, len(services))
	seen := make(map[string]struct{}, len(services))

	for _, svc := range services {
		name := strings.TrimSpace(svc.Name)
		endpoint := strings.TrimSpace(svc.Endpoint)
		if name == "" || endpoint == "" {
			continue
		}
		key := strings.ToLower(name) + "|" + endpoint
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, svc)
	}

	endpoints, ok := endpointsRaw.([]any)
	if !ok {
		if len(merged) == 0 {
			return nil, false
		}
		return merged, false
	}

	hasLegacy := len(endpoints) > 0
	for _, item := range endpoints {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name, _ := m["name"].(string)
		endpoint, _ := m["endpoint"].(string)
		name = strings.TrimSpace(name)
		endpoint = strings.TrimSpace(endpoint)
		if name == "" || endpoint == "" {
			continue
		}

		key := strings.ToLower(name) + "|" + endpoint
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, agent.RegistrationService{
			Name:     name,
			Endpoint: endpoint,
		})
	}

	if len(merged) == 0 {
		return nil, hasLegacy
	}
	return merged, hasLegacy
}
