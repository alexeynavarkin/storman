package auth

import (
	"testing"
)

// TestLoginLimiterBurst verifies that a single (login, ip) pair can fire its
// burst quota (10 attempts) and then gets denied. We don't sleep for the
// refill — that's covered by the rate.Limiter unit tests upstream.
func TestLoginLimiterBurst(t *testing.T) {
	l := NewLoginLimiter()
	for i := 0; i < 10; i++ {
		ok, _ := l.Allow("alice", "1.2.3.4")
		if !ok {
			t.Fatalf("attempt %d denied early", i+1)
		}
	}
	ok, retry := l.Allow("alice", "1.2.3.4")
	if ok {
		t.Error("11th attempt should be denied")
	}
	if retry == 0 {
		t.Error("retry hint should be non-zero on deny")
	}
}

// TestLoginLimiterPerKeyIsolation makes sure exhausting one login doesn't
// punish another. Same IP, different login — fresh bucket.
func TestLoginLimiterPerKeyIsolation(t *testing.T) {
	l := NewLoginLimiter()
	for i := 0; i < 10; i++ {
		if ok, _ := l.Allow("alice", "1.2.3.4"); !ok {
			t.Fatalf("alice attempt %d denied early", i+1)
		}
	}
	// Alice exhausted. Bob from the same IP should still pass — until the
	// IP bucket also tightens, which happens after its own 60-attempt burst.
	if ok, _ := l.Allow("bob", "1.2.3.4"); !ok {
		t.Error("bob should still be allowed under per-login bucketing")
	}
}

// TestNilLimiterPermissive guards the test-harness case where callers leave
// the limiter unset (e.g. tests that want to disable rate limiting).
func TestNilLimiterPermissive(t *testing.T) {
	var l *LoginLimiter
	ok, _ := l.Allow("anyone", "anywhere")
	if !ok {
		t.Error("nil limiter must allow everything")
	}
}
