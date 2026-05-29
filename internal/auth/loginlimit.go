package auth

import (
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// LoginLimiter rate-limits authentication attempts by login name and by
// client IP. Two independent buckets — exceeding either returns 429 with a
// Retry-After hint. Complements UserService's failed_attempts/locked_until
// bookkeeping by adding a cheap in-memory burst guard.
//
// One instance is shared across surfaces (web login, WebDAV Basic auth,
// FTPS auth) so an attacker can't bypass the limit by switching protocols.
//
// Eviction: buckets keep state for ~10 min after their last touch via a
// lazy sweep on each Allow call. For a personal-scale server with a handful
// of users this is plenty; for higher fan-out, swap for an LRU cache.
type LoginLimiter struct {
	mu       sync.Mutex
	byLogin  map[string]*bucket
	byIP     map[string]*bucket
	loginCfg rate.Limit
	loginB   int
	ipCfg    rate.Limit
	ipB      int
}

type bucket struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// NewLoginLimiter returns the production-default limiter: 10 attempts per
// minute per login, 60 attempts per minute per client IP.
func NewLoginLimiter() *LoginLimiter {
	return &LoginLimiter{
		byLogin:  make(map[string]*bucket),
		byIP:     make(map[string]*bucket),
		loginCfg: rate.Every(time.Minute / 10),
		loginB:   10,
		ipCfg:    rate.Every(time.Minute / 60),
		ipB:      60,
	}
}

// Allow reports whether a new login attempt for (login, ip) is permitted.
// When refused it returns a conservative Retry-After hint (1 second — the
// bucket usually refills faster than that, but giving the client a precise
// number leaks the limiter's internals).
//
// If either bucket is exhausted the attempt is denied; we accept the mild
// over-debit when one bucket is empty but the other isn't (the still-fresh
// bucket consumes a token it shouldn't have). At the rate humans hammer a
// login form, that's noise.
//
// A nil receiver allows everything — handy for test harnesses that want to
// disable rate limiting.
func (l *LoginLimiter) Allow(login, ip string) (ok bool, retryAfter time.Duration) {
	if l == nil {
		return true, 0
	}
	login = strings.ToLower(strings.TrimSpace(login))
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gcLocked()

	now := time.Now()
	loginBucket := l.bucketLocked(l.byLogin, login, l.loginCfg, l.loginB)
	ipBucket := l.bucketLocked(l.byIP, ip, l.ipCfg, l.ipB)
	loginBucket.lastSeen = now
	ipBucket.lastSeen = now

	if !loginBucket.lim.AllowN(now, 1) || !ipBucket.lim.AllowN(now, 1) {
		return false, time.Second
	}
	return true, 0
}

// AllowIP applies only the per-IP bucket. Intended for flows where the user
// identity isn't known at the start of the request — passkey discoverable
// login is the canonical case (the assertion arrives without a login string).
func (l *LoginLimiter) AllowIP(ip string) (ok bool, retryAfter time.Duration) {
	if l == nil {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gcLocked()

	now := time.Now()
	b := l.bucketLocked(l.byIP, ip, l.ipCfg, l.ipB)
	b.lastSeen = now
	if !b.lim.AllowN(now, 1) {
		return false, time.Second
	}
	return true, 0
}

func (l *LoginLimiter) bucketLocked(m map[string]*bucket, key string, r rate.Limit, burst int) *bucket {
	if b, ok := m[key]; ok {
		return b
	}
	b := &bucket{lim: rate.NewLimiter(r, burst)}
	m[key] = b
	return b
}

// gcLocked drops buckets idle for >10 min. O(n) on the maps; fine for a
// personal-scale deployment with at most hundreds of distinct logins/IPs.
func (l *LoginLimiter) gcLocked() {
	cutoff := time.Now().Add(-10 * time.Minute)
	for k, b := range l.byLogin {
		if b.lastSeen.Before(cutoff) {
			delete(l.byLogin, k)
		}
	}
	for k, b := range l.byIP {
		if b.lastSeen.Before(cutoff) {
			delete(l.byIP, k)
		}
	}
}
