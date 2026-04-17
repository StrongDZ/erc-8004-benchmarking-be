package cache

// cache.go — Tiny thread-safe TTL cache for read-heavy API handlers.
// This is a stand-in until the Redis client is wired (§7). The Cache interface
// is defined at the consumer (service layer) — see service/types.go.

import (
	"encoding/json"
	"sync"
	"time"
)

// Entry is one cached value with its expiry deadline.
type entry struct {
	value  []byte
	expiry time.Time
}

// Memory is an in-process, TTL-bounded cache. Safe for concurrent use.
type Memory struct {
	mu     sync.RWMutex
	store  map[string]entry
	defTTL time.Duration
}

// NewMemory returns a Memory cache using `defTTL` when callers pass 0 to Set.
func NewMemory(defTTL time.Duration) *Memory {
	if defTTL <= 0 {
		defTTL = 60 * time.Second
	}
	return &Memory{store: make(map[string]entry), defTTL: defTTL}
}

// Get returns the raw bytes for a key (ok == true when present and not expired).
func (m *Memory) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	e, ok := m.store[key]
	m.mu.RUnlock()
	if !ok || time.Now().After(e.expiry) {
		return nil, false
	}
	return e.value, true
}

// Set stores the given bytes with ttl. ttl <= 0 uses the cache's default.
func (m *Memory) Set(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		ttl = m.defTTL
	}
	m.mu.Lock()
	m.store[key] = entry{value: value, expiry: time.Now().Add(ttl)}
	m.mu.Unlock()
}

// Delete removes a key from the cache.
func (m *Memory) Delete(key string) {
	m.mu.Lock()
	delete(m.store, key)
	m.mu.Unlock()
}

// GetJSON decodes a cached JSON payload into `dst`. Returns (true, nil) on hit.
func (m *Memory) GetJSON(key string, dst any) (bool, error) {
	b, ok := m.Get(key)
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return false, err
	}
	return true, nil
}

// SetJSON marshals `src` and stores it under key with ttl.
func (m *Memory) SetJSON(key string, src any, ttl time.Duration) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	m.Set(key, b, ttl)
	return nil
}

// StartJanitor launches a goroutine that periodically removes expired entries.
// Call with a cancel context to stop it on shutdown.
func (m *Memory) StartJanitor(stop <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-t.C:
				m.mu.Lock()
				for k, v := range m.store {
					if now.After(v.expiry) {
						delete(m.store, k)
					}
				}
				m.mu.Unlock()
			}
		}
	}()
}
