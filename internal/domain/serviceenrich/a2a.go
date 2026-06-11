package serviceenrich

import (
	"encoding/json"
	"fmt"
	"strings"
)

// A2AMeta holds A2A agent-card fields.
type A2AMeta struct {
	Provider     string
	Capabilities []string
	InputModes   []string
	OutputModes  []string
	AuthSchemes  []string
	SkillNames   []string
	X402         *X402Meta
}

// X402Meta holds x402 payment extension fields.
type X402Meta struct {
	Enabled  bool
	Chain    string
	Currency string
	Fee      string
	PayTo    string
}

type a2aDoc struct {
	Provider struct {
		Organization string `json:"organization"`
	} `json:"provider"`
	Capabilities struct {
		Streaming         bool `json:"streaming"`
		PushNotifications bool `json:"pushNotifications"`
		StateTransition   bool `json:"stateTransitionHistory"`
	} `json:"capabilities"`
	DefaultInputModes  []string `json:"defaultInputModes"`
	DefaultOutputModes []string `json:"defaultOutputModes"`
	Authentication     struct {
		Schemes []string `json:"schemes"`
	} `json:"authentication"`
	Skills []struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	} `json:"skills"`
	Extensions map[string]json.RawMessage `json:"extensions"`
	X402       *x402Block                 `json:"x402"`
}

type x402Block struct {
	Enabled  bool   `json:"enabled"`
	Chain    string `json:"chain"`
	Currency string `json:"currency"`
	Fee      any    `json:"fee"`
	PayTo    string `json:"payTo"`
}

// EnrichA2A parses an A2A agent card JSON body.
func EnrichA2A(jsonText string) A2AMeta {
	jsonText = strings.TrimSpace(jsonText)
	if jsonText == "" {
		return A2AMeta{}
	}
	var doc a2aDoc
	if err := json.Unmarshal([]byte(jsonText), &doc); err != nil {
		return A2AMeta{}
	}

	var caps []string
	if doc.Capabilities.Streaming {
		caps = append(caps, "streaming")
	}
	if doc.Capabilities.PushNotifications {
		caps = append(caps, "pushNotifications")
	}
	if doc.Capabilities.StateTransition {
		caps = append(caps, "stateTransitionHistory")
	}

	var skillNames []string
	for _, sk := range doc.Skills {
		name := strings.TrimSpace(sk.Name)
		if name == "" {
			name = strings.TrimSpace(sk.ID)
		}
		if name != "" {
			skillNames = append(skillNames, name)
		}
	}

	meta := A2AMeta{
		Provider:     strings.TrimSpace(doc.Provider.Organization),
		Capabilities: caps,
		InputModes:   doc.DefaultInputModes,
		OutputModes:  doc.DefaultOutputModes,
		AuthSchemes:  doc.Authentication.Schemes,
		SkillNames:   skillNames,
	}

	if doc.X402 != nil {
		meta.X402 = parseX402Block(doc.X402)
	} else if raw, ok := doc.Extensions["x402"]; ok {
		var blk x402Block
		if err := json.Unmarshal(raw, &blk); err == nil {
			meta.X402 = parseX402Block(&blk)
		}
	}
	return meta
}

func parseX402Block(b *x402Block) *X402Meta {
	if b == nil {
		return nil
	}
	fee := ""
	if b.Fee != nil {
		switch t := b.Fee.(type) {
		case string:
			fee = strings.TrimSpace(t)
		case float64:
			fee = fmt.Sprintf("%g", t)
		default:
			raw, _ := json.Marshal(t)
			fee = strings.TrimSpace(string(raw))
		}
	}
	return &X402Meta{
		Enabled:  b.Enabled || b.PayTo != "",
		Chain:    strings.TrimSpace(b.Chain),
		Currency: strings.TrimSpace(b.Currency),
		Fee:      fee,
		PayTo:    strings.ToLower(strings.TrimSpace(b.PayTo)),
	}
}
