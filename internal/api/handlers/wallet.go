package handlers

// wallet.go — HTTP handlers for /wallet/:address/* endpoints.

import (
	"net/http"
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// WalletHandler wires the Wallet service to HTTP.
type WalletHandler struct {
	svc *service.Wallet
}

// NewWalletHandler returns a new WalletHandler.
func NewWalletHandler(svc *service.Wallet) *WalletHandler { return &WalletHandler{svc: svc} }

// FeedbackGiven handles GET /wallet/{address}/feedbacks.
// Returns paginated feedback submitted by the given wallet address, enriched with agent name.
// Each row matches dto.FeedbackRow (classification.rule + optional classification.fallback), same as GET /agents/.../feedbacks.
// @Summary Wallet feedback given
// @Description Returns all feedback submitted by a wallet address across all agents.
// @Tags wallet
// @Produce json
// @Param address path string true "Wallet address (hex)"
// @Param page query int false "Page number (default 1)"
// @Param limit query int false "Page size (max 100, default 20)"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /wallet/{address}/feedbacks [get]
func (h *WalletHandler) FeedbackGiven(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimSpace(r.PathValue("address"))
	if address == "" {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "address is required")
		return
	}

	page, limit, skip := dto.ParsePagination(r, 20, 100)

	out, err := h.svc.FeedbackGiven(r.Context(), service.WalletFeedbacksParams{
		Address: address,
		Page:    page,
		Limit:   limit,
		Skip:    skip,
	})
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.SuccessMeta(w, r, out.Rows, &dto.Meta{Page: out.Page, Limit: out.Limit, Total: out.Total})
}
