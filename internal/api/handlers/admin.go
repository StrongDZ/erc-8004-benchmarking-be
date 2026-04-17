package handlers

// admin.go — HTTP handlers for /admin/*.

import (
	"net/http"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// AdminHandler wires admin/observability endpoints to HTTP.
type AdminHandler struct {
	svc *service.Admin
}

// NewAdminHandler returns a new AdminHandler.
func NewAdminHandler(svc *service.Admin) *AdminHandler { return &AdminHandler{svc: svc} }

// IndexerStatus handles GET /admin/indexer-status (§6.1).
// @Summary Indexer status
// @Tags admin
// @Security ApiKeyAuth
// @Produce json
// @Success 200 {object} dto.Response
// @Failure 401 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /admin/indexer-status [get]
func (h *AdminHandler) IndexerStatus(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.IndexerStatus(r.Context())
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, out)
}

// RecomputeScoring handles POST /admin/scoring/recompute (§6.2) — v2 stub.
// @Summary Recompute scoring (v2 placeholder)
// @Tags admin
// @Security ApiKeyAuth
// @Produce json
// @Failure 401 {object} dto.Response
// @Failure 501 {object} dto.Response
// @Router /admin/scoring/recompute [post]
func (h *AdminHandler) RecomputeScoring(w http.ResponseWriter, r *http.Request) {
	dto.Fail(w, r, http.StatusNotImplemented, dto.CodeNotImplemented, "scoring recompute is not implemented in v1")
}

// SimulatorStart is a placeholder for §6.3.
// @Summary Start simulator (v2 placeholder)
// @Tags admin
// @Security ApiKeyAuth
// @Produce json
// @Failure 401 {object} dto.Response
// @Failure 501 {object} dto.Response
// @Router /admin/simulator/start [post]
func (h *AdminHandler) SimulatorStart(w http.ResponseWriter, r *http.Request) {
	dto.Fail(w, r, http.StatusNotImplemented, dto.CodeNotImplemented, "simulator is not implemented in v1")
}
