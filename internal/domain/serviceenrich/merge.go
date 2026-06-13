package serviceenrich

import (
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

const maxToolsListed = 12

// BuildEnrichment assembles a UI-ready enrichment object for one service.
func BuildEnrichment(
	serviceName, endpoint string,
	reg RegistrationMeta,
	hasReg bool,
	row *offchainrepo.OffchainData,
) *dto.ServiceEnrichment {
	e := &dto.ServiceEnrichment{}

	if hasReg {
		applyRegistration(e, reg)
	}

	if row != nil {
		e.Probe = probeMeta(row)
		applyOffchainContent(e, serviceName, row)
	}

	if isEmptyEnrichment(e) {
		return nil
	}
	return e
}

func applyRegistration(e *dto.ServiceEnrichment, reg RegistrationMeta) {
	if reg.Description != "" {
		e.Description = reg.Description
	}
	if reg.Method != "" {
		e.Method = reg.Method
	}
	if reg.PaymentRequired != nil {
		e.PaymentRequired = reg.PaymentRequired
	}
	if reg.Protocol != "" {
		e.Protocol = reg.Protocol
	}
	if len(reg.Tools) > 0 {
		setTools(e, reg.Tools)
	}
	if len(reg.Prompts) > 0 {
		e.Prompts = reg.Prompts
	}
	if len(reg.SkillPaths) > 0 && len(e.SkillPaths) == 0 {
		e.SkillPaths = reg.SkillPaths
	}
	if len(reg.DomainPaths) > 0 && len(e.DomainPaths) == 0 {
		e.DomainPaths = reg.DomainPaths
	}
}

func applyOffchainContent(e *dto.ServiceEnrichment, serviceName string, row *offchainrepo.OffchainData) {
	content := strings.TrimSpace(row.Content)
	switch row.Status {
	case offchainrepo.StatusFetchedJSON:
		applyJSONEnrichment(e, serviceName, content)
	case offchainrepo.StatusFetchedNotJSON:
		html := EnrichHTML(content)
		if html.PageTitle != "" {
			e.PageTitle = html.PageTitle
		}
		if html.PageDescription != "" {
			e.PageDescription = html.PageDescription
		}
		if html.PageImage != "" {
			e.PageImage = html.PageImage
		}
	}
}

func applyJSONEnrichment(e *dto.ServiceEnrichment, serviceName string, content string) {
	name := strings.ToLower(strings.TrimSpace(serviceName))
	switch {
	case name == "mcp" || strings.Contains(name, "mcp"):
		mcp := EnrichMCP(content)
		if mcp.Provider != "" {
			e.Provider = mcp.Provider
		}
		if len(mcp.AuthSchemes) > 0 {
			e.AuthSchemes = mcp.AuthSchemes
		}
		if len(mcp.Tools) > 0 {
			setTools(e, mcp.Tools)
		}
	case name == "a2a" || strings.Contains(name, "a2a"):
		a2a := EnrichA2A(content)
		if a2a.Provider != "" {
			e.Provider = a2a.Provider
		}
		if len(a2a.Capabilities) > 0 {
			e.Capabilities = a2a.Capabilities
		}
		if len(a2a.InputModes) > 0 {
			e.InputModes = a2a.InputModes
		}
		if len(a2a.OutputModes) > 0 {
			e.OutputModes = a2a.OutputModes
		}
		if len(a2a.AuthSchemes) > 0 {
			e.AuthSchemes = a2a.AuthSchemes
		}
		if len(a2a.SkillNames) > 0 {
			e.SkillPaths = a2a.SkillNames
		}
		if a2a.X402 != nil {
			e.X402 = &dto.ServiceX402Enrichment{
				Enabled:  a2a.X402.Enabled,
				Chain:    a2a.X402.Chain,
				Currency: a2a.X402.Currency,
				Fee:      a2a.X402.Fee,
				PayTo:    a2a.X402.PayTo,
			}
		}
	default:
		// agent cards and registration-like JSON may still carry useful A2A fields.
		a2a := EnrichA2A(content)
		if a2a.Provider != "" && e.Provider == "" {
			e.Provider = a2a.Provider
		}
		if len(a2a.Capabilities) > 0 && len(e.Capabilities) == 0 {
			e.Capabilities = a2a.Capabilities
		}
		if a2a.X402 != nil && e.X402 == nil {
			e.X402 = &dto.ServiceX402Enrichment{
				Enabled:  a2a.X402.Enabled,
				Chain:    a2a.X402.Chain,
				Currency: a2a.X402.Currency,
				Fee:      a2a.X402.Fee,
				PayTo:    a2a.X402.PayTo,
			}
		}
	}
}

func setTools(e *dto.ServiceEnrichment, tools []string) {
	e.ToolCount = len(tools)
	if len(tools) <= maxToolsListed {
		e.Tools = tools
		return
	}
	e.Tools = append([]string(nil), tools[:maxToolsListed]...)
}

func probeMeta(row *offchainrepo.OffchainData) *dto.ServiceProbeMeta {
	pm := &dto.ServiceProbeMeta{
		Status:      row.Status,
		ContentSize: row.ContentSize,
		SourceType:  row.SourceType,
	}
	if row.Status == offchainrepo.StatusFetchFailed {
		pm.ErrorSummary = shortenError(row.FetchError)
	}
	return pm
}

func shortenError(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline") {
		return "Probe timed out"
	}
	if strings.Contains(lower, "unexpected http status") {
		return msg
	}
	if len(msg) > 120 {
		return msg[:120] + "…"
	}
	return msg
}

func isEmptyEnrichment(e *dto.ServiceEnrichment) bool {
	return e.Description == "" &&
		e.Method == "" &&
		e.PaymentRequired == nil &&
		e.Protocol == "" &&
		e.ToolCount == 0 &&
		len(e.Tools) == 0 &&
		len(e.Prompts) == 0 &&
		len(e.SkillPaths) == 0 &&
		len(e.DomainPaths) == 0 &&
		e.Provider == "" &&
		len(e.Capabilities) == 0 &&
		len(e.InputModes) == 0 &&
		len(e.OutputModes) == 0 &&
		len(e.AuthSchemes) == 0 &&
		e.PageTitle == "" &&
		e.PageDescription == "" &&
		e.PageImage == "" &&
		e.X402 == nil &&
		(e.Probe == nil || (e.Probe.ErrorSummary == "" && e.Probe.ContentSize == 0 && e.Probe.SourceType == ""))
}
