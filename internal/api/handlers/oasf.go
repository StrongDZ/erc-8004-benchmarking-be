package handlers

// oasf.go — HTTP handlers for /oasf/*.

import (
	"net/http"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// OASFHandler wires the OASF service to HTTP.
type OASFHandler struct {
	svc *service.OASF
}

// NewOASFHandler returns a new OASFHandler.
func NewOASFHandler(svc *service.OASF) *OASFHandler { return &OASFHandler{svc: svc} }

// Facets handles GET /oasf/facets (§4.1).
// @Summary OASF facets tree
// @Tags oasf
// @Produce json
// @Param chainId query int false "Chain ID (optional; omit to query all chains)"
// @Param skill query []string false "Skill filters"
// @Param domain query []string false "Domain filters"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /oasf/facets [get]
func (h *OASFHandler) Facets(w http.ResponseWriter, r *http.Request) {
	chainID := dto.ParseInt64(r, "chainId", 0)
	tree, err := h.svc.Facets(
		r.Context(),
		chainID,
		dto.ParseStringSlice(r, "skill"),
		dto.ParseStringSlice(r, "domain"),
	)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, tree)
}

// Skills handles GET /oasf/skills (§4.2) — browse top agents by skill filter.
// @Summary Browse agents by skills/domains
// @Tags oasf
// @Produce json
// @Param chainId query int false "Chain ID (optional; omit to query all chains)"
// @Param skill query []string false "Skill filters"
// @Param domain query []string false "Domain filters"
// @Param page query int false "Page number"
// @Param limit query int false "Page size"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /oasf/skills [get]
func (h *OASFHandler) Skills(w http.ResponseWriter, r *http.Request) {
	h.browse(w, r, "skill")
}

// Domains handles GET /oasf/domains (§4.3).
// @Summary Browse agents by domains/skills
// @Tags oasf
// @Produce json
// @Param chainId query int false "Chain ID (optional; omit to query all chains)"
// @Param domain query []string false "Domain filters"
// @Param skill query []string false "Skill filters"
// @Param page query int false "Page number"
// @Param limit query int false "Page size"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /oasf/domains [get]
func (h *OASFHandler) Domains(w http.ResponseWriter, r *http.Request) {
	h.browse(w, r, "domain")
}

func (h *OASFHandler) browse(w http.ResponseWriter, r *http.Request, kind string) {
	chainID := dto.ParseInt64(r, "chainId", 0)
	page, limit, skip := dto.ParsePagination(r, 50, 100)
	params := service.BrowseParams{
		ChainID: chainID,
		Page:    page,
		Limit:   limit,
		Skip:    skip,
	}
	if kind == "skill" {
		params.Skills = dto.ParseStringSlice(r, "skill")
		params.Domains = dto.ParseStringSlice(r, "domain")
	} else {
		params.Domains = dto.ParseStringSlice(r, "domain")
		params.Skills = dto.ParseStringSlice(r, "skill")
	}
	out, err := h.svc.Browse(r.Context(), params)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.SuccessMeta(w, r, out.Rows, &dto.Meta{Page: out.Page, Limit: out.Limit, Total: out.Total})
}

// SchemaSkills handles GET /oasf/schema/skills — full OASF skill taxonomy from DB.
// @Summary OASF skill taxonomy
// @Tags oasf
// @Produce json
// @Success 200 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /oasf/schema/skills [get]
func (h *OASFHandler) SchemaSkills(w http.ResponseWriter, r *http.Request) {
	entries, err := h.svc.SchemaSkills(r.Context())
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, entries)
}

// SchemaDomains handles GET /oasf/schema/domains — full OASF domain taxonomy from DB.
// @Summary OASF domain taxonomy
// @Tags oasf
// @Produce json
// @Success 200 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /oasf/schema/domains [get]
func (h *OASFHandler) SchemaDomains(w http.ResponseWriter, r *http.Request) {
	entries, err := h.svc.SchemaDomains(r.Context())
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, entries)
}
