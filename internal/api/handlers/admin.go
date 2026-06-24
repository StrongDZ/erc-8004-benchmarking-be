package handlers

// admin.go — HTTP handlers for /admin/*.

import (
	"encoding/json"
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

// RecomputeScoringRequest is the JSON body for POST /admin/scoring/recompute.
type RecomputeScoringRequest struct {
	ChainID int64 `json:"chainId"` // 0 = all chains
}

// RecomputeScoring handles POST /admin/scoring/recompute.
// Returns 202 Accepted with a requestId; the actual replay runs asynchronously
// in the score-refresh worker (typically completes within seconds to minutes
// depending on corpus size — see Table tab:latency).
// @Summary Trigger on-demand score recompute
// @Tags admin
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param body body RecomputeScoringRequest false "Chain scope (omit or 0 for all chains)"
// @Success 202 {object} dto.Response
// @Failure 401 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /admin/scoring/recompute [post]
func (h *AdminHandler) RecomputeScoring(w http.ResponseWriter, r *http.Request) {
	var req RecomputeScoringRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	requestID, err := h.svc.TriggerRecompute(r.Context(), req.ChainID)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.WriteEnvelope(w, r, http.StatusAccepted, dto.Response{
		Success: true,
		Data:    map[string]any{"requestId": requestID, "status": "queued"},
	})
}

// SimulatorStartRequest is the JSON body for POST /admin/simulator/start.
type SimulatorStartRequest struct {
	AgentID string `json:"agentId"` // empty = generate a new sandbox agent
	Count   int    `json:"count"`   // number of synthetic feedback records (1–100)
}

// SimulatorStart handles POST /admin/simulator/start.
// Inserts count synthetic feedback records on the reserved sandbox chain
// (chainId=999999001) then triggers a scoped score-refresh replay for that
// chain so composite scores are updated. Returns 202 Accepted.
// @Summary Start sandbox scoring simulator
// @Tags admin
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param body body SimulatorStartRequest true "Simulator parameters"
// @Success 202 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 401 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /admin/simulator/start [post]
func (h *AdminHandler) SimulatorStart(w http.ResponseWriter, r *http.Request) {
	var req SimulatorStartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "invalid JSON body")
		return
	}
	if req.Count <= 0 {
		req.Count = 10 // default
	}

	res, requestID, err := h.svc.StartSimulator(r.Context(), req.AgentID, req.Count)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.WriteEnvelope(w, r, http.StatusAccepted, dto.Response{
		Success: true,
		Data: map[string]any{
			"agentId":   res.AgentID,
			"count":     res.Count,
			"requestId": requestID,
			"status":    "queued",
		},
	})
}
