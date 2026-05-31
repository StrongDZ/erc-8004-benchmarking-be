package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

type serviceCanon struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Version  string `json:"version"`
}

type identityCanon struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Image       string         `json:"image"`
	AgentURI    string         `json:"agentURI"`
	Domains     []string       `json:"domains"`
	OASFSkills  []string       `json:"oasfSkills"`
	OASFDomains []string       `json:"oasfDomains"`
	Services    []serviceCanon `json:"services"`
}

// ContentHash returns the lowercase hex SHA-256 of an agent's identity canonical form.
// Inputs are trimmed and slices sorted before hashing so that ordering and whitespace
// do not change the result.
func ContentHash(d *agentrepo.AgentDocument) string {
	if d == nil {
		return ""
	}

	c := identityCanon{
		Name:        strings.TrimSpace(d.Name),
		Description: strings.TrimSpace(d.Description),
		Image:       strings.TrimSpace(d.Image),
		AgentURI:    strings.TrimSpace(d.AgentURI),
		Domains:     sortedTrimmed(d.Domains),
		OASFSkills:  sortedTrimmed(d.OASFSkills),
		OASFDomains: sortedTrimmed(d.OASFDomains),
		Services:    canonServices(d.Services),
	}

	raw, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func sortedTrimmed(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, strings.TrimSpace(s))
	}
	sort.Strings(out)
	return out
}

func canonServices(in []agentrepo.RegistrationService) []serviceCanon {
	if len(in) == 0 {
		return []serviceCanon{}
	}
	out := make([]serviceCanon, 0, len(in))
	for _, s := range in {
		out = append(out, serviceCanon{
			Name:     strings.TrimSpace(s.Name),
			Endpoint: strings.TrimSpace(s.Endpoint),
			Version:  strings.TrimSpace(s.Version),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Endpoint < out[j].Endpoint
	})
	return out
}
