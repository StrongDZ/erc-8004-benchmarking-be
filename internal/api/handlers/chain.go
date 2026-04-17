package handlers

// chain.go — HTTP handlers for /chains and /chains/:chainId/contracts.

import (
	"net/http"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// ChainHandler wires the Chain service to HTTP.
type ChainHandler struct {
	svc *service.Chain
}

// NewChainHandler returns a new ChainHandler.
func NewChainHandler(svc *service.Chain) *ChainHandler { return &ChainHandler{svc: svc} }

// List handles GET /chains (§5.1).
// @Summary List supported chains
// @Tags chains
// @Produce json
// @Success 200 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /chains [get]
func (h *ChainHandler) List(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.List(r.Context())
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, out)
}

// Contracts handles GET /chains/:chainId/contracts (§5.2).
// @Summary List chain contract addresses
// @Tags chains
// @Produce json
// @Param chainId path int true "Chain ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /chains/{chainId}/contracts [get]
func (h *ChainHandler) Contracts(w http.ResponseWriter, r *http.Request) {
	chainID, err := dto.RequireInt64Path(r, "chainId")
	if err != nil {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
		return
	}
	out, err := h.svc.Contracts(r.Context(), chainID)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, out)
}
