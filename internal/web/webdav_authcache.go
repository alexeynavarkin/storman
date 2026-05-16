package web

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/alexnav/storman/internal/auth"
)

// davAuthCache memoizes the result of a successful WebDAV Basic-auth attempt
// so that macOS Finder's chatty, many-parallel-connections request pattern
// doesn't re-run bcrypt + a DB lookup on every request and doesn't exhaust the
// per-login rate-limit bucket. A cache key is (login, sha256(secret), ip), so
// a leaked entry is still scoped to the original client IP, and a credential
// change invalidates the entry on its next miss.
//
// Failed attempts are never cached: they continue to flow through the limiter
// and the normal DB-backed check, preserving brute-force protection.
type davAuthCache struct {
	mu      sync.Mutex
	entries map[string]davAuthEntry
	ttl     time.Duration
	max     int
	now     func() time.Time // injectable for tests
}

type davAuthEntry struct {
	user      auth.User
	expiresAt time.Time
}

func newDavAuthCache(ttl time.Duration, max int) *davAuthCache {
	return &davAuthCache{
		entries: make(map[string]davAuthEntry),
		ttl:     ttl,
		max:     max,
		now:     time.Now,
	}
}

func davAuthKey(login, secret, ip string) string {
	sum := sha256.Sum256([]byte(secret))
	return login + "|" + hex.EncodeToString(sum[:]) + "|" + ip
}

// Get returns the cached user for the key if present and unexpired.
func (c *davAuthCache) Get(key string) (auth.User, bool) {
	if c == nil {
		return auth.User{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return auth.User{}, false
	}
	if !c.now().Before(e.expiresAt) {
		delete(c.entries, key)
		return auth.User{}, false
	}
	return e.user, true
}

// Put stores a successful authentication.
func (c *davAuthCache) Put(key string, user auth.User) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gcLocked()
	if len(c.entries) >= c.max {
		// Drop a single arbitrary entry. The map's randomized iteration order
		// keeps this fair on average, and the GC sweep above already evicted
		// anything expired — so this only fires under sustained pressure from
		// many distinct credentials, which isn't a workload we optimize for.
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = davAuthEntry{user: user, expiresAt: c.now().Add(c.ttl)}
}

// HasLogin reports whether any unexpired entry exists for the given login,
// regardless of secret/IP. Used to suppress redundant rate_limited audit
// events when the same login has recently authenticated successfully.
func (c *davAuthCache) HasLogin(login string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	prefix := login + "|"
	for k, e := range c.entries {
		if !now.Before(e.expiresAt) {
			delete(c.entries, k)
			continue
		}
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func (c *davAuthCache) gcLocked() {
	now := c.now()
	for k, e := range c.entries {
		if !now.Before(e.expiresAt) {
			delete(c.entries, k)
		}
	}
}
