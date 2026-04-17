package dto

// response.go — Standard response envelope used by every handler (§1.2).

import (
	"encoding/json"
	"net/http"
)

// Meta carries pagination metadata (page, limit, total).
type Meta struct {
	Page  int   `json:"page,omitempty"`
	Limit int   `json:"limit,omitempty"`
	Total int64 `json:"total"`
}

// Error is the body of an error response (§1.2).
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Response is the top-level JSON envelope. `Data` is rendered as-is and may be nil.
type Response struct {
	Success   bool   `json:"success"`
	Data      any    `json:"data,omitempty"`
	Meta      *Meta  `json:"meta,omitempty"`
	Error     *Error `json:"error,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

// Standard error codes (§1.2).
const (
	CodeBadRequest     = "BAD_REQUEST"
	CodeNotFound       = "NOT_FOUND"
	CodeRateLimited    = "RATE_LIMITED"
	CodeUnauthorized   = "UNAUTHORIZED"
	CodeInternalError  = "INTERNAL_ERROR"
	CodeUpstreamTimeout = "UPSTREAM_TIMEOUT"
	CodeNotImplemented = "NOT_IMPLEMENTED"
)

// Success writes a 200 JSON envelope with `data`.
func Success(w http.ResponseWriter, r *http.Request, data any) {
	WriteEnvelope(w, r, http.StatusOK, Response{Success: true, Data: data})
}

// SuccessMeta writes a 200 JSON envelope with `data` and pagination meta.
func SuccessMeta(w http.ResponseWriter, r *http.Request, data any, meta *Meta) {
	WriteEnvelope(w, r, http.StatusOK, Response{Success: true, Data: data, Meta: meta})
}

// Fail writes an error envelope with the given HTTP status.
func Fail(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	WriteEnvelope(w, r, status, Response{Success: false, Error: &Error{Code: code, Message: msg}})
}

// WriteEnvelope marshals and writes the envelope, stamping RequestID from the
// request context (populated by the requestid middleware).
func WriteEnvelope(w http.ResponseWriter, r *http.Request, status int, env Response) {
	if id, ok := r.Context().Value(RequestIDKey).(string); ok && id != "" {
		env.RequestID = id
	}
	b, err := json.Marshal(env)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"success":false,"error":{"code":"INTERNAL_ERROR","message":"encode response"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// contextKey is an unexported type used to key values in request contexts.
type contextKey string

// RequestIDKey is the context key under which the per-request ULID is stored.
const RequestIDKey contextKey = "requestId"
