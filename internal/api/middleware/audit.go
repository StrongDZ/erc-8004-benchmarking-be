package middleware

// audit.go — Append-only audit logging for /admin/* requests.

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	"erc-8004-benchmarking-be/internal/repository/adminaudit"
)

// AuditAdmin records one adminaudit.Entry per request to action. It runs
// after the rest of the chain (including AdminAuth, if present) so the
// recorded status code reflects the real outcome — unauthorized attempts are
// audited too, not just successful ones.
func AuditAdmin(repo *adminaudit.Repository, action string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sr := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(sr, r)
			status := sr.status
			if status == 0 {
				status = http.StatusOK
			}

			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				host = r.RemoteAddr
			}

			entry := adminaudit.Entry{
				Timestamp:  time.Now().Unix(),
				Action:     action,
				Method:     r.Method,
				Path:       r.URL.Path,
				RemoteIP:   host,
				Authorized: status != http.StatusUnauthorized,
				StatusCode: status,
				RequestID:  sr.ResponseWriter.Header().Get("X-Request-Id"),
			}
			// Detached from the request context (which is cancelled once the
			// response above has been written) and fire-and-forget: an
			// audit-log write failure must never affect the admin action's
			// own response.
			go func() {
				if err := repo.Append(context.Background(), entry); err != nil {
					log.Printf("admin audit: append failed: %v", err)
				}
			}()
		})
	}
}
