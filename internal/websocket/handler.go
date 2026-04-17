package websocket

// handler.go — HTTP handler that upgrades a request to a WebSocket connection
// and registers the client with the Hub.

import (
	"net/http"
	"strings"

	gws "github.com/gorilla/websocket"
)

// ServeWSOptions customises the upgrader. Zero value is a permissive default
// (allows all origins) suitable for API_ALLOWED_ORIGINS="*".
type ServeWSOptions struct {
	// AllowedOrigins is a list of exact Origin values allowed to upgrade.
	// An entry of "*" (or an empty/nil slice) allows all origins.
	AllowedOrigins []string
}

// ServeWS returns an http.HandlerFunc that upgrades GET /ws requests to a
// WebSocket connection and registers the resulting client with h.
func (h *Hub) ServeWS(opts ServeWSOptions) http.HandlerFunc {
	wildcard := false
	allow := make(map[string]struct{}, len(opts.AllowedOrigins))
	for _, o := range opts.AllowedOrigins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			wildcard = true
			continue
		}
		allow[o] = struct{}{}
	}
	if len(allow) == 0 && !wildcard {
		// Default: allow everything. Callers that want to restrict must pass explicit origins.
		wildcard = true
	}

	upgrader := gws.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			if wildcard {
				return true
			}
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // non-browser clients (curl/ws CLI) have no Origin
			}
			_, ok := allow[origin]
			return ok
		},
	}

	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logf("upgrade: %v", err)
			return
		}
		client := NewClient(h, conn)
		h.register <- client

		go client.writePump()
		go client.readPump()
	}
}
