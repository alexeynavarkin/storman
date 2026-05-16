package web

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/auth"
)

func TestDavAuthCache_HitMiss(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	key := davAuthKey("alice", "secret", "10.0.0.1")
	if _, ok := c.Get(key); ok {
		t.Fatal("expected miss on fresh cache")
	}
	id := uuid.New()
	c.Put(key, auth.User{ID: id, Login: "alice"})
	got, ok := c.Get(key)
	if !ok || got.ID != id {
		t.Fatalf("expected hit, got %+v ok=%v", got, ok)
	}
}

func TestDavAuthCache_KeyScopedByIP(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	c.Put(davAuthKey("alice", "secret", "10.0.0.1"), auth.User{ID: uuid.New()})
	if _, ok := c.Get(davAuthKey("alice", "secret", "10.0.0.2")); ok {
		t.Error("entry must not be reused from a different IP")
	}
}

func TestDavAuthCache_KeyScopedBySecret(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	c.Put(davAuthKey("alice", "secret", "10.0.0.1"), auth.User{ID: uuid.New()})
	if _, ok := c.Get(davAuthKey("alice", "other", "10.0.0.1")); ok {
		t.Error("entry must not be reused with a different secret")
	}
}

func TestDavAuthCache_Expiry(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	clock := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return clock }

	key := davAuthKey("alice", "secret", "10.0.0.1")
	c.Put(key, auth.User{ID: uuid.New()})
	if _, ok := c.Get(key); !ok {
		t.Fatal("expected hit before TTL")
	}

	clock = clock.Add(2 * time.Minute)
	if _, ok := c.Get(key); ok {
		t.Fatal("expected miss after TTL")
	}
}

func TestDavAuthCache_HasLogin(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	c.Put(davAuthKey("alice", "secret", "10.0.0.1"), auth.User{ID: uuid.New()})

	if !c.HasLogin("alice") {
		t.Error("HasLogin(alice) should be true after a successful auth")
	}
	if c.HasLogin("bob") {
		t.Error("HasLogin(bob) should be false")
	}
}

func TestDavAuthCache_HasLoginExpires(t *testing.T) {
	c := newDavAuthCache(time.Minute, 16)
	clock := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return clock }
	c.Put(davAuthKey("alice", "secret", "10.0.0.1"), auth.User{ID: uuid.New()})

	clock = clock.Add(2 * time.Minute)
	if c.HasLogin("alice") {
		t.Error("HasLogin should return false once the only entry has expired")
	}
}

func TestDavAuthCache_NilSafe(t *testing.T) {
	var c *davAuthCache
	if _, ok := c.Get("anything"); ok {
		t.Error("nil cache Get should miss")
	}
	c.Put("anything", auth.User{}) // must not panic
	if c.HasLogin("anything") {
		t.Error("nil cache HasLogin should be false")
	}
}

func TestDavAuthCache_EvictsAtCapacity(t *testing.T) {
	c := newDavAuthCache(time.Minute, 2)
	c.Put(davAuthKey("a", "s", "1"), auth.User{ID: uuid.New()})
	c.Put(davAuthKey("b", "s", "1"), auth.User{ID: uuid.New()})
	c.Put(davAuthKey("c", "s", "1"), auth.User{ID: uuid.New()})
	if len(c.entries) > 2 {
		t.Errorf("cache exceeded capacity: %d entries", len(c.entries))
	}
}
