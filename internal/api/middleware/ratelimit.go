package middleware

// ratelimit.go — Per-IP token bucket rate limiter.
// Uses golang.org/x/time/rate for the token bucket algorithm.
// A background goroutine periodically evicts idle visitor entries.

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"erc-8004-benchmarking-be/internal/api/dto"
)

// RateLimitConfig defines the limits applied by RateLimit.
type RateLimitConfig struct {
	// RequestsPerSecond is the sustained token refill rate per IP.
	RequestsPerSecond float64
	// Burst is the maximum token bucket depth (handles short spikes).
	Burst int
	// CleanupInterval controls how often idle visitors are evicted.
	CleanupInterval time.Duration
	// IdleTTL is how long an IP must be inactive before eviction.
	IdleTTL time.Duration
}

// DefaultRateLimitConfig returns sensible defaults: 20 req/s per IP, burst 40.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		RequestsPerSecond: 20,
		Burst:             40,
		CleanupInterval:   5 * time.Minute,
		IdleTTL:           10 * time.Minute,
	}
}

type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type ipLimiterStore struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	cfg      RateLimitConfig
}

func newIPLimiterStore(cfg RateLimitConfig) *ipLimiterStore {
	s := &ipLimiterStore{
		visitors: make(map[string]*visitor),
		cfg:      cfg,
	}
	go s.cleanupLoop()
	return s
}

func (s *ipLimiterStore) allow(ip string) bool {
	s.mu.Lock()
	v, ok := s.visitors[ip]
	if !ok {
		v = &visitor{limiter: rate.NewLimiter(rate.Limit(s.cfg.RequestsPerSecond), s.cfg.Burst)}
		s.visitors[ip] = v
	}
	v.lastSeen = time.Now()
	allowed := v.limiter.Allow()
	s.mu.Unlock()
	return allowed
}

func (s *ipLimiterStore) cleanupLoop() {
	interval := s.cfg.CleanupInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	for range time.Tick(interval) {
		s.mu.Lock()
		ttl := s.cfg.IdleTTL
		if ttl <= 0 {
			ttl = 10 * time.Minute
		}
		cutoff := time.Now().Add(-ttl)
		for ip, v := range s.visitors {
			if v.lastSeen.Before(cutoff) {
				delete(s.visitors, ip)
			}
		}
		s.mu.Unlock()
	}
}

// RateLimit returns middleware that throttles requests per client IP.
// Requests exceeding the limit receive 429 Too Many Requests.
func RateLimit(cfg RateLimitConfig) func(http.Handler) http.Handler {
	store := newIPLimiterStore(cfg)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := realIP(r)
			if !store.allow(ip) {
				w.Header().Set("Retry-After", "1")
				dto.Fail(w, r, http.StatusTooManyRequests, dto.CodeRateLimited, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// realIP extracts the client IP, honouring X-Forwarded-For behind a trusted proxy.
func realIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the leftmost (originating) address, strip any port.
		for i := range xff {
			if xff[i] == ',' {
				xff = xff[:i]
				break
			}
		}
		xff = strings.TrimSpace(xff)
		if ip, _, err := net.SplitHostPort(xff); err == nil {
			return ip
		}
		return xff
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
