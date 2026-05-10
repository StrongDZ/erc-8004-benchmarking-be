package middleware

// middleware.go — Minimal HTTP middleware stack (stdlib only).
//   - RequestID: stamps an opaque request ID on every request
//   - Logger:    structured access log to stderr
//   - Recover:   panic → 500 without crashing the server
//   - CORS:      permissive preflight/main-request handling for browser clients
//   - AdminAuth: X-API-Key gate for /admin/*

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
)

// Chain composes middlewares left-to-right: Chain(a, b, c)(h) == a(b(c(h))).
func Chain(mws ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			h = mws[i](h)
		}
		return h
	}
}

// RequestID stamps a random 16-byte hex ID on each request context + response header.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf [16]byte
		_, _ = rand.Read(buf[:])
		id := hex.EncodeToString(buf[:])
		ctx := context.WithValue(r.Context(), dto.RequestIDKey, id)
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// statusRecorder captures the final status code for access logs.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (sr *statusRecorder) WriteHeader(status int) {
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}

func (sr *statusRecorder) Write(b []byte) (int, error) {
	if sr.status == 0 {
		sr.status = http.StatusOK
	}
	n, err := sr.ResponseWriter.Write(b)
	sr.bytes += n
	return n, err
}

// Hijack preserves websocket/http1 upgrade compatibility when ResponseWriter
// has been wrapped by middleware.
func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := sr.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
}

// Flush preserves streaming semantics when the underlying writer supports it.
func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Push delegates HTTP/2 server push when supported by the underlying writer.
func (sr *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := sr.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

// Logger writes a single-line access log per request.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(sr, r)
		status := sr.status
		if status == 0 {
			status = http.StatusOK
		}
		log.Printf(
			"api method=%s path=%s status=%d bytes=%d dur=%s req_id=%s",
			r.Method, r.URL.Path, status, sr.bytes, time.Since(start), sr.ResponseWriter.Header().Get("X-Request-Id"),
		)
	})
}

// Recover converts panics in downstream handlers into INTERNAL_ERROR responses.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rv := recover(); rv != nil {
				log.Printf("api panic recovered: %v\n%s", rv, debug.Stack())
				dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// CORS returns middleware that allows the listed origins. `*` allows all origins.
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	allow := map[string]bool{}
	wildcard := false
	for _, o := range allowedOrigins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			wildcard = true
			continue
		}
		allow[o] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && (wildcard || allow[origin]) {
				if wildcard {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Request-Id")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// AdminAuth gates a handler on the X-API-Key header matching the configured admin key.
// When the configured key is empty, the endpoint is disabled entirely.
func AdminAuth(adminKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.TrimSpace(adminKey) == "" {
				dto.Fail(w, r, http.StatusUnauthorized, dto.CodeUnauthorized, "admin api is disabled (ADMIN_API_KEY unset)")
				return
			}
			provided := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if provided == "" || provided != adminKey {
				dto.Fail(w, r, http.StatusUnauthorized, dto.CodeUnauthorized, "invalid or missing X-API-Key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
