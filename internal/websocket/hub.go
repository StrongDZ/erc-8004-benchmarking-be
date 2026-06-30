package websocket

// hub.go — Central broadcaster for WebSocket clients.
// Single goroutine owns the clients map to avoid locking; clients are registered
// and removed via channels. Slow clients are dropped to protect the hub.

import (
	"context"
	"log"
	"sync/atomic"
)

// Hub fans out messages to all connected clients.
type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
	clients    map[*Client]struct{}

	clientCount int64 // atomic snapshot for observability
}

// NewHub builds a Hub with buffered channels sized for moderate event rates.
func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client, 64),
		unregister: make(chan *Client, 64),
		broadcast:  make(chan []byte, 1024),
		clients:    make(map[*Client]struct{}),
	}
}

// Broadcast enqueues a message for fan-out. Non-blocking: drops on full buffer.
func (h *Hub) Broadcast(payload []byte) {
	if h == nil || len(payload) == 0 {
		return
	}
	select {
	case h.broadcast <- payload:
	default:
		// Drop when the hub buffer is saturated; realtime stream is best-effort.
	}
}

// Run owns the clients map and processes register/unregister/broadcast events.
// Returns when ctx is cancelled. Safe to call exactly once per Hub.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for c := range h.clients {
				close(c.send)
				delete(h.clients, c)
			}
			atomic.StoreInt64(&h.clientCount, 0)
			return

		case c := <-h.register:
			h.clients[c] = struct{}{}
			atomic.StoreInt64(&h.clientCount, int64(len(h.clients)))

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
				atomic.StoreInt64(&h.clientCount, int64(len(h.clients)))
			}

		case payload := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- payload:
				default:
					// Slow client: drop it to protect the hub.
					delete(h.clients, c)
					close(c.send)
				}
			}
			atomic.StoreInt64(&h.clientCount, int64(len(h.clients)))
		}
	}
}

// logf is a small helper so handlers and pumps can share one logger shape.
func logf(format string, args ...any) {
	log.Printf("ws: "+format, args...)
}
