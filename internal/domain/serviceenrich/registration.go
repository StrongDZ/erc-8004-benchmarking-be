package serviceenrich

import (
	"encoding/json"
	"strings"
)

// RegistrationMeta holds fields merged from agentURI registration.json services[].
type RegistrationMeta struct {
	Description     string
	Method          string
	PaymentRequired *bool
	Protocol        string
	Tools           []string
	Prompts         []string
	SkillPaths      []string
	DomainPaths     []string
	Version         string
}

type registrationDoc struct {
	Services []registrationService `json:"services"`
}

type registrationService struct {
	Endpoint        string   `json:"endpoint"`
	Description     string   `json:"description"`
	Method          string   `json:"method"`
	PaymentRequired *bool    `json:"paymentRequired"`
	Protocol        string   `json:"protocol"`
	Version         string   `json:"version"`
	MCPTools        []string `json:"mcpTools"`
	MCPPrompts      []string `json:"mcpPrompts"`
	A2ASkills       []string `json:"a2aSkills"`
	Skills          []string `json:"skills"`
	Domains         []string `json:"domains"`
}

// ParseRegistrationServices indexes registration.json services by exact endpoint URL.
func ParseRegistrationServices(jsonText string) map[string]RegistrationMeta {
	jsonText = strings.TrimSpace(jsonText)
	if jsonText == "" {
		return nil
	}
	var doc registrationDoc
	if err := json.Unmarshal([]byte(jsonText), &doc); err != nil {
		return nil
	}
	out := make(map[string]RegistrationMeta, len(doc.Services))
	for _, s := range doc.Services {
		ep := strings.TrimSpace(s.Endpoint)
		if ep == "" {
			continue
		}
		tools := s.MCPTools
		skills := s.A2ASkills
		if len(skills) == 0 {
			skills = s.Skills
		}
		out[ep] = RegistrationMeta{
			Description:     strings.TrimSpace(s.Description),
			Method:          strings.TrimSpace(s.Method),
			PaymentRequired: s.PaymentRequired,
			Protocol:        strings.TrimSpace(s.Protocol),
			Tools:           tools,
			Prompts:         s.MCPPrompts,
			SkillPaths:      skills,
			DomainPaths:     s.Domains,
			Version:         strings.TrimSpace(s.Version),
		}
	}
	return out
}
