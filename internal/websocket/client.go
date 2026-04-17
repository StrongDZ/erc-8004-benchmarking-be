package websocket

// client.go — Per-connection read/write pumps for WebSocket clients.
// readPump drops all incoming frames (we're one-way broadcast) but handles
// pong/close frames. writePump fans messages out with periodic pings.

import (
	"time"

	gws "github.com/gorilla/websocket"
)

// Tuning constants.
const (
	writeWait      = 10 * time.Second     // per-frame write deadline
	pongWait       = 60 * time.Second     // max between pongs before read pump errors
	pingPeriod     = (pongWait * 9) / 10  // 54s
	maxMessageSize = 1 << 16              // 64 KiB inbound cap (client messages should be tiny)
	sendBufferSize = 256                  // drop-threshold for slow clients
)

// Client wraps a single WebSocket connection registered with a Hub.
type Client struct {
	hub  *Hub
	conn *gws.Conn
	send chan []byte
}

// NewClient wires the connection up with the given hub. Caller must also call
// go client.readPump() and go client.writePump() after registering.
func NewClient(hub *Hub, conn *gws.Conn) *Client {
	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, sendBufferSize),
	}
}

// readPump reads from the peer. We accept and discard any inbound frames so the
// conn remains alive, and we update the pong deadline on any received message.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		if _, _, err := c.conn.NextReader(); err != nil {
			if gws.IsUnexpectedCloseError(err, gws.CloseGoingAway, gws.CloseAbnormalClosure) {
				logf("read: %v", err)
			}
			return
		}
		// Extend deadline on any inbound frame (client heartbeat or ping text).
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	}
}

// writePump drains the send channel and emits periodic pings. Exits when the
// hub closes c.send or an I/O error occurs.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(gws.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(gws.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(gws.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
