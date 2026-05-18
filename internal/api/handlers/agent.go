package handlers

// agent.go âÿÿ HTTP handlers for /agents/:chainId/:agentId/* endpoints.

import (
	"net/http"
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// AgentHandler wires the Agent service to HTTP.
type AgentHandler struct {
	svc *service.Agent
}

// NewAgentHandler returns a new AgentHandler.
func NewAgentHandler(svc *service.Agent) *AgentHandler { return &AgentHandler{svc: svc} }

// Profile handles GET /agents/:chainId/:agentId (Â§3.1).
// @Summary Agent profile
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId} [get]
func (h *AgentHandler) Profile(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	out, err := h.svc.Profile(r.Context(), chainID, agentID)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// Overview handles GET /agents/:chainId/:agentId/overview.
// @Summary Agent overview tab (basic info + services health + metadata)
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/overview [get]
func (h *AgentHandler) Overview(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	out, err := h.svc.Overview(r.Context(), chainID, agentID)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// Feedbacks handles GET /agents/:chainId/:agentId/feedbacks (Â§3.3).
// Response rows use dto.FeedbackRow: classification.rule is always set; classification.fallback
// is present only when the LLM fallback ran (see internal/api/service/mapper.go toFeedbackRow).
// @Summary Agent feedback list
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param category query string false "Feedback category"
// @Param status query string false "passed|failed|revoked"
// @Param from query string false "RFC3339 start time"
// @Param to query string false "RFC3339 end time"
// @Param page query int false "Page number"
// @Param limit query int false "Page size"
// @Param sort query string false "newest|oldest"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/feedbacks [get]
func (h *AgentHandler) Feedbacks(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	from, to, err := dto.ParseTimeRange(r)
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return
	}
	page, limit, skip := dto.ParsePagination(r, 50, 100)
	sortDesc := true
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort"))) {
	case "oldest", "asc", "block_asc":
		sortDesc = false
	}
	out, err := h.svc.Feedbacks(r.Context(), service.FeedbacksParams{
		ChainID:  chainID,
		AgentID:  agentID,
		Category: strings.TrimSpace(r.URL.Query().Get("category")),
		Status:   strings.TrimSpace(r.URL.Query().Get("status")),
		From:     from,
		To:       to,
		SortDesc: sortDesc,
		Page:     page,
		Limit:    limit,
		Skip:     skip,
	})
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.SuccessMeta(w, r, out.Rows, &dto.Meta{Page: out.Page, Limit: out.Limit, Total: out.Total})
}

// FeedbackDetail handles GET /agents/:chainId/:agentId/feedbacks/:feedbackId (Â§3.4).
// @Summary Agent feedback detail
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param feedbackId path string true "Feedback ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/feedbacks/{feedbackId} [get]
func (h *AgentHandler) FeedbackDetail(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	fid, err := dto.RequireStringPath(r, "feedbackId")
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return
	}
	out, err := h.svc.FeedbackDetail(r.Context(), chainID, agentID, fid)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// IdentityHistory handles GET /agents/:chainId/:agentId/identity-history (Â§3.5).
// @Summary Agent identity history
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/identity-history [get]
func (h *AgentHandler) IdentityHistory(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	out, err := h.svc.IdentityHistory(r.Context(), chainID, agentID)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// ActivityHeatmap handles GET /agents/:chainId/:agentId/activity-heatmap (Â§3.7).
// @Summary Agent activity heatmap
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param days query int false "Window in days"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/activity-heatmap [get]
func (h *AgentHandler) ActivityHeatmap(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	days := dto.ParseInt(r, "days", 365)
	out, err := h.svc.ActivityHeatmap(r.Context(), chainID, agentID, days)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// Penalties handles GET /agents/:chainId/:agentId/penalties (Â§3.8).
// @Summary Agent penalties
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param type query string false "fail|revoked|both"
// @Param page query int false "Page number"
// @Param limit query int false "Page size"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/penalties [get]
func (h *AgentHandler) Penalties(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	page, limit, skip := dto.ParsePagination(r, 50, 100)
	typ := strings.TrimSpace(r.URL.Query().Get("type"))
	if typ == "" {
		typ = "both"
	}
	out, err := h.svc.Penalties(r.Context(), service.PenaltiesParams{
		ChainID: chainID,
		AgentID: agentID,
		Type:    typ,
		Page:    page,
		Limit:   limit,
		Skip:    skip,
	})
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.SuccessMeta(w, r, out.Rows, &dto.Meta{Page: out.Page, Limit: out.Limit, Total: out.Total})
}

// Related handles GET /agents/:chainId/:agentId/related (Â§3.11).
// @Summary Related agents
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param by query string false "skill|domain|both"
// @Param limit query int false "Max results"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/related [get]
func (h *AgentHandler) Related(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	limit := dto.ParseInt64(r, "limit", 10)
	if limit < 1 {
		limit = 1
	}
	if limit > 50 {
		limit = 50
	}
	by := strings.TrimSpace(r.URL.Query().Get("by"))
	if by == "" {
		by = "both"
	}
	out, err := h.svc.Related(r.Context(), chainID, agentID, by, limit)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// Radar handles GET /agents/:chainId/:agentId/radar (Â§3.6).
// @Summary Agent radar
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/radar [get]
func (h *AgentHandler) Radar(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	out, err := h.svc.Radar(r.Context(), chainID, agentID)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// Proof handles GET /agents/:chainId/:agentId/proof/:txHash (Â§3.10).
// @Summary Agent proof by transaction
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Param txHash path string true "Transaction hash"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/proof/{txHash} [get]
func (h *AgentHandler) Proof(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	tx, err := dto.RequireStringPath(r, "txHash")
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return
	}
	out, err := h.svc.Proof(r.Context(), chainID, agentID, tx)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// AntiSpam is a v2 placeholder (Â§3.9).
// @Summary Agent anti-spam (v2 placeholder)
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Failure 501 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/anti-spam [get]
func (h *AgentHandler) AntiSpam(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := h.parseAgentPath(w, r); !ok {
		return
	}
	dto.Fail(w, r, http.StatusNotImplemented, dto.CodeNotImplemented, "anti-spam endpoint is not implemented in v1")
}

// OffchainByURI handles GET /offchain-by-uri?uri=… (offchain_data cache for a feedback / URI string).
// @Summary Off-chain URI cache
// @Tags agents
// @Produce json
// @Param uri query string true "Logical URI (ipfs://, https://, …)"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /offchain-by-uri [get]
func (h *AgentHandler) OffchainByURI(w http.ResponseWriter, r *http.Request) {
	uri := strings.TrimSpace(r.URL.Query().Get("uri"))
	if uri == "" {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "query parameter uri is required")
		return
	}
	out, err := h.svc.OffchainDataByURI(r.Context(), uri)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// ReputationScoreHistory handles GET /agents/:chainId/:agentId/reputation-score-history.
// Returns the agent's reputation timeline derived from feedback_history (event + daily decay points).
// @Summary Reputation score history
// @Tags agents
// @Produce json
// @Param chainId path int true "Chain ID"
// @Param agentId path string true "Agent ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 404 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /agents/{chainId}/{agentId}/reputation-score-history [get]
func (h *AgentHandler) ReputationScoreHistory(w http.ResponseWriter, r *http.Request) {
	chainID, agentID, ok := h.parseAgentPath(w, r)
	if !ok {
		return
	}
	out, err := h.svc.ReputationScoreHistory(r.Context(), chainID, agentID)
	if err != nil {
		writeServiceErr(w, r, err)
		return
	}
	dto.Success(w, r, out)
}

// parseAgentPath extracts (chainId, agentId) or writes a 400 and returns ok=false.
func (h *AgentHandler) parseAgentPath(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	chainID, err := dto.RequireInt64Path(r, "chainId")
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return 0, "", false
	}
	agentID, err := dto.RequireStringPath(r, "agentId")
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return 0, "", false
	}
	return chainID, agentID, true
}
