package redis

// client.go — Redis client factory used by API (subscribe) and consumer (publish).
// Provides a thin Pub/Sub wrapper for cross-process realtime events.

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Client wraps a *redis.Client with a Publish/Subscribe API.
type Client struct {
	r *goredis.Client
}

// NewClient parses a redis URL (redis://host:port/db) and verifies the connection with PING.
func NewClient(ctx context.Context, url string) (*Client, error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: parse url: %w", err)
	}
	rdb := goredis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &Client{r: rdb}, nil
}

// Publish sends payload on the given channel. Returns the number of subscribers.
func (c *Client) Publish(ctx context.Context, channel string, payload []byte) error {
	if c == nil || c.r == nil {
		return fmt.Errorf("redis: client is nil")
	}
	return c.r.Publish(ctx, channel, payload).Err()
}

// Subscribe opens a subscription on the given channel and returns a channel of
// raw payload bytes. The returned closer stops the subscription and drains the
// channel. The subscriber goroutine exits when ctx is cancelled or closer runs.
func (c *Client) Subscribe(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	if c == nil || c.r == nil {
		return nil, nil, fmt.Errorf("redis: client is nil")
	}
	ps := c.r.Subscribe(ctx, channel)
	// Force the initial subscribe RTT so connection issues surface here.
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, nil, fmt.Errorf("redis: subscribe %q: %w", channel, err)
	}

	out := make(chan []byte, 256)
	stop := make(chan struct{})

	go func() {
		defer close(out)
		ch := ps.Channel(goredis.WithChannelSize(256))
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- []byte(msg.Payload):
				default:
					// drop on slow consumer; realtime stream is best-effort.
				}
			}
		}
	}()

	closer := func() {
		close(stop)
		_ = ps.Close()
	}
	return out, closer, nil
}

// Close releases the underlying Redis connection.
func (c *Client) Close() error {
	if c == nil || c.r == nil {
		return nil
	}
	return c.r.Close()
}

// Raw returns the underlying go-redis client for callers that need commands
// beyond Pub/Sub. Intentionally kept small: most callers should use methods above.
func (c *Client) Raw() *goredis.Client {
	if c == nil {
		return nil
	}
	return c.r
}
