package serviceenrich

import (
	"encoding/json"
	"strings"
)

// MCPMeta holds MCP manifest fields.
type MCPMeta struct {
	Tools       []string
	ToolCount   int
	AuthSchemes []string
	Provider    string
}

type mcpDoc struct {
	Tools []struct {
		Name string `json:"name"`
	} `json:"tools"`
	ServerInfo struct {
		Title string `json:"title"`
		Name  string `json:"name"`
	} `json:"serverInfo"`
	Authentication struct {
		Schemes []string `json:"schemes"`
	} `json:"authentication"`
}

// EnrichMCP parses an MCP manifest JSON body.
func EnrichMCP(jsonText string) MCPMeta {
	jsonText = strings.TrimSpace(jsonText)
	if jsonText == "" {
		return MCPMeta{}
	}
	var doc mcpDoc
	if err := json.Unmarshal([]byte(jsonText), &doc); err != nil {
		return MCPMeta{}
	}
	var tools []string
	for _, t := range doc.Tools {
		name := strings.TrimSpace(t.Name)
		if name != "" {
			tools = append(tools, name)
		}
	}
	provider := strings.TrimSpace(doc.ServerInfo.Title)
	if provider == "" {
		provider = strings.TrimSpace(doc.ServerInfo.Name)
	}
	return MCPMeta{
		Tools:       tools,
		ToolCount:   len(tools),
		AuthSchemes: doc.Authentication.Schemes,
		Provider:    provider,
	}
}
