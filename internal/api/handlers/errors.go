package handlers

// errors.go — Centralised service-error → HTTP-response mapping.
// All handlers delegate to writeServiceErr instead of duplicating switch/case.

import (
	"errors"
	"net/http"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
)

// writeServiceErr converts well-known service sentinel errors to the appropriate
// HTTP status code. Unknown errors become 500 Internal Server Error.
func writeServiceErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, service.ErrAgentNotFound):
		dto.Fail(w, r, http.StatusNotFound, dto.CodeNotFound, "agent not found")
	case errors.Is(err, service.ErrFeedbackNotFound):
		dto.Fail(w, r, http.StatusNotFound, dto.CodeNotFound, "feedback not found")
	case errors.Is(err, service.ErrInvalidInput):
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
	default:
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, "internal server error")
	}
}
