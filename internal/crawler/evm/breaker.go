package evm

// breaker.go — Simple per-chain circuit breaker for RPC calls.
// States: closed (normal) → open (tripped) → half-open (probe after cooldown).
//
// The breaker is keyed per chainID so a dead chain does not affect healthy ones.

import (
	"fmt"
	"sync"
	"time"
)

type breakerState int

const (
	breakerClosed   breakerState = iota // normal operation
	breakerOpen                         // tripped — fast-fail all calls
	breakerHalfOpen                     // cooldown elapsed — allow one probe
)

// BreakerConfig controls trip/reset thresholds.
type BreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before tripping.
	FailureThreshold int
	// CooldownDuration is how long the breaker stays open before going half-open.
	CooldownDuration time.Duration
}

// DefaultBreakerConfig returns thresholds suitable for EVM RPC endpoints.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold: 5,
		CooldownDuration: 30 * time.Second,
	}
}

// chainBreaker is a single per-chain state machine.
type chainBreaker struct {
	mu           sync.Mutex
	state        breakerState
	failures     int
	tripTime     time.Time
	cfg          BreakerConfig
}

// Allow returns nil when the call should proceed, or a descriptive error when
// the circuit is open (fast-fail path).
func (b *chainBreaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerOpen:
		if time.Since(b.tripTime) >= b.cfg.CooldownDuration {
			b.state = breakerHalfOpen
			return nil // allow the probe call
		}
		return fmt.Errorf("circuit open: RPC endpoint unavailable (cooldown %s remaining)",
			b.cfg.CooldownDuration-time.Since(b.tripTime).Round(time.Second))
	default:
		return nil
	}
}

// RecordSuccess resets the breaker to closed.
func (b *chainBreaker) RecordSuccess() {
	b.mu.Lock()
	b.failures = 0
	b.state = breakerClosed
	b.mu.Unlock()
}

// RecordFailure increments the failure counter and trips the breaker when the
// threshold is reached.
func (b *chainBreaker) RecordFailure() {
	b.mu.Lock()
	b.failures++
	if b.failures >= b.cfg.FailureThreshold || b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.tripTime = time.Now()
	}
	b.mu.Unlock()
}

// BreakerRegistry holds one breaker per chainID.
type BreakerRegistry struct {
	mu       sync.Mutex
	breakers map[int64]*chainBreaker
	cfg      BreakerConfig
}

// NewBreakerRegistry creates a registry with the given config.
func NewBreakerRegistry(cfg BreakerConfig) *BreakerRegistry {
	return &BreakerRegistry{
		breakers: make(map[int64]*chainBreaker),
		cfg:      cfg,
	}
}

// Get returns (or lazily creates) the breaker for a chain.
func (r *BreakerRegistry) Get(chainID int64) *chainBreaker {
	r.mu.Lock()
	b, ok := r.breakers[chainID]
	if !ok {
		b = &chainBreaker{cfg: r.cfg}
		r.breakers[chainID] = b
	}
	r.mu.Unlock()
	return b
}
