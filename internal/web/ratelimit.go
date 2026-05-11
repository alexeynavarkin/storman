package web

import (
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// loginLimiter rate-limits the /api/auth/login endpoint by login name and by
// client IP. Two independent buckets — exceeding either returns 429 with a
// Retry-After header. Complements UserService's failed_attempts/locked_until
// bookkeeping by adding a cheap in-memory burst guard.
//
// Eviction: buckets keep state for ~10 min after their last touch via a
// lazy sweep on each Allow call. For a personal-scale server with a handful
// of users this is plenty; for higher fan-out, swap for an LRU cache.
type loginLimiter struct {
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

// newLoginLimiter returns the production-default limiter: 10 attempts per
// minute per login, 60 attempts per minute per client IP.
func newLoginLimiter() *loginLimiter {
	return &loginLimiter{
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
func (l *loginLimiter) Allow(login, ip string) (ok bool, retryAfter time.Duration) {
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

func (l *loginLimiter) bucketLocked(m map[string]*bucket, key string, r rate.Limit, burst int) *bucket {
	if b, ok := m[key]; ok {
		return b
	}
	b := &bucket{lim: rate.NewLimiter(r, burst)}
	m[key] = b
	return b
}

// gcLocked drops buckets idle for >10 min. O(n) on the maps; fine for a
// personal-scale deployment with at most hundreds of distinct logins/IPs.
func (l *loginLimiter) gcLocked() {
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
