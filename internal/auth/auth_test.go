package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

// fastUsers returns a UserService with cheap argon2 params so tests stay snappy.
func fastUsers(pool *pgxpool.Pool) *auth.UserService {
	u := auth.NewUserService(pool)
	u.SetTestParams(auth.HashParams{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLen: 16, KeyLen: 32})
	u.MinPasswordLen = 6
	return u
}

func TestUserCreateAndAuthenticate(t *testing.T) {
	pool := testpg.Pool(t)
	users := fastUsers(pool)
	ctx := context.Background()

	u, err := users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if u.Login != "alice" {
		t.Errorf("login: %q", u.Login)
	}

	if _, err := users.Create(ctx, "ALICE", "different"); !errors.Is(err, auth.ErrLoginTaken) {
		t.Errorf("expected ErrLoginTaken (citext case-insensitive), got %v", err)
	}

	if _, err := users.Authenticate(ctx, "alice", "hunter22"); err != nil {
		t.Errorf("auth success: %v", err)
	}
	if _, err := users.Authenticate(ctx, "alice", "wrong"); !errors.Is(err, auth.ErrPasswordMismatch) {
		t.Errorf("expected ErrPasswordMismatch, got %v", err)
	}
}

func TestUserCreateRejectsWeakPassword(t *testing.T) {
	pool := testpg.Pool(t)
	users := auth.NewUserService(pool)
	_, err := users.Create(context.Background(), "bob", "short")
	if !errors.Is(err, auth.ErrWeakPassword) {
		t.Errorf("expected ErrWeakPassword, got %v", err)
	}
}

func TestSessionLifecycle(t *testing.T) {
	pool := testpg.Pool(t)
	users := fastUsers(pool)
	sessions := auth.NewSessionService(pool)
	ctx := context.Background()

	u, err := users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}

	sess, err := sessions.Create(ctx, u.ID, nil, "test-agent")
	if err != nil {
		t.Fatal(err)
	}
	if sess.ID == "" {
		t.Error("empty session id")
	}

	got, err := sessions.Validate(ctx, sess.ID)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.UserID != u.ID {
		t.Errorf("wrong user_id: %v", got.UserID)
	}

	if err := sessions.Touch(ctx, sess.ID); err != nil {
		t.Errorf("touch: %v", err)
	}

	if err := sessions.Logout(ctx, sess.ID); err != nil {
		t.Errorf("logout: %v", err)
	}
	if _, err := sessions.Validate(ctx, sess.ID); !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("expected ErrNotFound after logout, got %v", err)
	}
}

func TestSessionExpired(t *testing.T) {
	pool := testpg.Pool(t)
	users := fastUsers(pool)
	sessions := auth.NewSessionService(pool)

	// Inject a clock so we can force expiry.
	now := time.Now()
	sessions.SetClock(func() time.Time { return now })
	sessions.SetPolicy(auth.SessionPolicy{IdleTimeout: time.Hour, AbsoluteTimeout: 24 * time.Hour})

	ctx := context.Background()
	u, err := users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	sess, err := sessions.Create(ctx, u.ID, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// Jump past idle timeout.
	now = now.Add(2 * time.Hour)
	if _, err := sessions.Validate(ctx, sess.ID); !errors.Is(err, auth.ErrSessionExpired) {
		t.Errorf("expected ErrSessionExpired, got %v", err)
	}
}

func TestPermissionEffectiveInheritance(t *testing.T) {
	pool := testpg.Pool(t)
	users := fastUsers(pool)
	perms := rbac.NewPermissionService(pool)
	ctx := context.Background()

	fs := dbfs.New(pool, "")
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, fs, "/a", false)
	mustMkdir(t, fs, "/a/b", false)
	mustMkdir(t, fs, "/a/b/c", false)

	a := mustStatID(t, fs, "/a")
	b := mustStatID(t, fs, "/a/b")
	c := mustStatID(t, fs, "/a/b/c")

	u, err := users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}

	// Grant Read+Write on /a; nothing else.
	if err := perms.Grant(ctx, a, u.ID, rbac.Read|rbac.Write); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path string
		id   uuid.UUID
		want rbac.Action
	}{
		{"/a", a, rbac.Read | rbac.Write},
		{"/a/b", b, rbac.Read | rbac.Write},     // inherited
		{"/a/b/c", c, rbac.Read | rbac.Write},   // inherited
	} {
		got, err := perms.Effective(ctx, u.ID, tc.id)
		if err != nil {
			t.Fatalf("effective %s: %v", tc.path, err)
		}
		if got != tc.want {
			t.Errorf("effective %s: got %08b, want %08b", tc.path, got, tc.want)
		}
	}
}

func TestPermissionTraverse(t *testing.T) {
	pool := testpg.Pool(t)
	users := fastUsers(pool)
	perms := rbac.NewPermissionService(pool)
	ctx := context.Background()

	fs := dbfs.New(pool, "")
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, fs, "/projects", false)
	mustMkdir(t, fs, "/projects/secret", false)
	mustMkdir(t, fs, "/projects/secret/inner", false)

	projects := mustStatID(t, fs, "/projects")
	secret := mustStatID(t, fs, "/projects/secret")
	inner := mustStatID(t, fs, "/projects/secret/inner")

	u, err := users.Create(ctx, "bob", "hunter22")
	if err != nil {
		t.Fatal(err)
	}

	// Grant Read on the deepest node only.
	if err := perms.Grant(ctx, inner, u.ID, rbac.Read); err != nil {
		t.Fatal(err)
	}

	// Ancestors get Traverse, not Read.
	for _, id := range []uuid.UUID{projects, secret} {
		mask, err := perms.Effective(ctx, u.ID, id)
		if err != nil {
			t.Fatal(err)
		}
		if rbac.Has(mask, rbac.Read) {
			t.Errorf("unexpected Read on ancestor: %08b", mask)
		}
		if !rbac.Has(mask, rbac.Traverse) {
			t.Errorf("missing Traverse on ancestor: %08b", mask)
		}
	}

	// Target node has Read directly.
	mask, err := perms.Effective(ctx, u.ID, inner)
	if err != nil {
		t.Fatal(err)
	}
	if !rbac.Has(mask, rbac.Read) {
		t.Errorf("missing Read on target: %08b", mask)
	}

	// Check API: Read on target ok, Write denied.
	if err := perms.Check(ctx, u.ID, inner, rbac.Read); err != nil {
		t.Errorf("expected allowed, got %v", err)
	}
	if err := perms.Check(ctx, u.ID, inner, rbac.Write); !errors.Is(err, rbac.ErrDenied) {
		t.Errorf("expected ErrDenied, got %v", err)
	}
}

func TestShareLinkLifecycle(t *testing.T) {
	pool := testpg.Pool(t)
	ctx := context.Background()

	fs := dbfs.New(pool, "")
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, fs, "/share", false)
	id := mustStatID(t, fs, "/share")

	links := auth.NewShareLinkService(pool)

	// Admin scope is rejected.
	if _, err := links.Create(ctx, auth.CreateOpts{NodeID: id, Actions: rbac.Admin, TTL: time.Hour}); err == nil {
		t.Error("expected Admin to be rejected")
	}

	max := 2
	link, err := links.Create(ctx, auth.CreateOpts{
		NodeID: id, Actions: rbac.Read, TTL: time.Hour, MaxUses: &max,
	})
	if err != nil {
		t.Fatal(err)
	}
	if link.Token == "" {
		t.Fatal("empty token")
	}

	// Scope check.
	if _, err := links.Validate(ctx, link.Token, rbac.Read); err != nil {
		t.Errorf("validate read: %v", err)
	}
	if _, err := links.Validate(ctx, link.Token, rbac.Write); !errors.Is(err, auth.ErrShareLinkScope) {
		t.Errorf("expected scope error, got %v", err)
	}

	// Use exhausts after MaxUses.
	if _, err := links.Use(ctx, link.Token, rbac.Read); err != nil {
		t.Fatal(err)
	}
	if _, err := links.Use(ctx, link.Token, rbac.Read); err != nil {
		t.Fatal(err)
	}
	if _, err := links.Use(ctx, link.Token, rbac.Read); !errors.Is(err, auth.ErrShareLinkUsedUp) {
		t.Errorf("expected ErrShareLinkUsedUp, got %v", err)
	}
}

func TestShareLinkExpiry(t *testing.T) {
	pool := testpg.Pool(t)
	ctx := context.Background()

	fs := dbfs.New(pool, "")
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, fs, "/share", false)
	id := mustStatID(t, fs, "/share")

	links := auth.NewShareLinkService(pool)
	now := time.Now()
	links.SetClock(func() time.Time { return now })

	link, err := links.Create(ctx, auth.CreateOpts{NodeID: id, Actions: rbac.Read, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(2 * time.Minute)
	if _, err := links.Validate(ctx, link.Token, rbac.Read); !errors.Is(err, auth.ErrShareLinkExpired) {
		t.Errorf("expected ErrShareLinkExpired, got %v", err)
	}
}

// --- helpers ---

func mustMkdir(t *testing.T, fs *dbfs.DBFS, path string, parents bool) {
	t.Helper()
	if err := fs.Mkdir(context.Background(), path, storage.MkdirOpts{Parents: parents}); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustStatID(t *testing.T, fs *dbfs.DBFS, path string) uuid.UUID {
	t.Helper()
	info, err := fs.Stat(context.Background(), path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.ID
}
