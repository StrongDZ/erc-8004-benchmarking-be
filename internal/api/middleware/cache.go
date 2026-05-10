package middleware

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"net/http"
	"time"

	apicache "erc-8004-benchmarking-be/internal/api/cache"
)

// captureWriter intercepts the response body and status so we can cache them.
// Status defaults to 200 — handlers that never call WriteHeader implicitly return 200.
type captureWriter struct {
	http.ResponseWriter
	buf    bytes.Buffer
	status int
}

func (c *captureWriter) WriteHeader(code int) {
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	c.buf.Write(b)
	return c.ResponseWriter.Write(b)
}

// ResponseCache wraps a handler and caches 200-OK JSON GET responses in mem.
// Cache key is an FNV-64a hash of the full request URL (path + query string).
// Cached responses are served with an X-Cache: HIT header.
func ResponseCache(mem *apicache.Memory, ttl time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			h := fnv.New64a()
			_, _ = h.Write([]byte(r.URL.String()))
			key := fmt.Sprintf("http:%x", h.Sum64())

			if b, ok := mem.Get(key); ok {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				_, _ = w.Write(b)
				return
			}

			cw := &captureWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(cw, r)
			if cw.status == http.StatusOK && cw.buf.Len() > 0 {
				mem.Set(key, cw.buf.Bytes(), ttl)
			}
		})
	}
}
