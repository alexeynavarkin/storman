package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

// setupHarness boots a server backed by a fresh DB with no users so the setup
// endpoints are live. Unlike newHarness, it does not create a seed user.
func setupHarness(t *testing.T) (*httptest.Server, *web.Server, *auth.UserService, *rbac.PermissionService, *dbfs.DBFS) {
	t.Helper()
	pool := testpg.Pool(t)
	root := t.TempDir()
	storageDir := filepath.Join(root, "flat-storage")
	uploadsDir := filepath.Join(root, "meta-storage", "uploads")
	trashDir := filepath.Join(root, "meta-storage", "trash")
	for _, d := range []string{storageDir, uploadsDir, trashDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	users := auth.NewUserService(pool)
	users.SetTestParams(auth.HashParams{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLen: 16, KeyLen: 32})
	users.MinPasswordLen = 6
	sessions := auth.NewSessionService(pool)
	perms := rbac.NewPermissionService(pool)
	fs := dbfs.New(pool, trashDir, flat.New(storageDir, uploadsDir))
	if _, err := fs.Bootstrap(context.Background()); err != nil {
		t.Fatalf("bootstrap fs: %v", err)
	}

	srv := web.NewServer(web.Config{SecureCookies: false}, users, sessions, perms, auth.NewShareLinkService(pool), fs, nil)
	if err := srv.InitSetup(context.Background()); err != nil {
		t.Fatalf("init setup: %v", err)
	}
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)
	return ts, srv, users, perms, fs
}

func TestSetupStateInitiallyNeedsSetup(t *testing.T) {
	ts, srv, _, _, _ := setupHarness(t)

	resp, err := http.Get(ts.URL + "/api/setup/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body struct {
		NeedsSetup bool `json:"needs_setup"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.NeedsSetup {
		t.Fatal("expected needs_setup=true on fresh server")
	}
	if srv.SetupTokenForTest() == "" {
		t.Fatal("InitSetup did not store a token")
	}
}

func TestSetupAdminCreatesFirstUserAndGrantsAdmin(t *testing.T) {
	ts, srv, users, perms, fs := setupHarness(t)
	token := srv.SetupTokenForTest()

	body, _ := json.Marshal(map[string]string{
		"token":    token,
		"login":    "alice",
		"password": "supersecret",
	})
	resp, err := http.Post(ts.URL+"/api/setup/admin", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("setup admin status: %d", resp.StatusCode)
	}

	// User exists.
	user, err := users.FindByLogin(context.Background(), "alice")
	if err != nil {
		t.Fatalf("find user: %v", err)
	}

	// Granted Admin (and full perms) on root.
	root, err := fs.Stat(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	mask, err := perms.Effective(context.Background(), user.ID, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rbac.Has(rbac.Action(mask), rbac.Admin) {
		t.Fatalf("first user did not receive Admin on root (mask=%b)", mask)
	}

	// Token invalidated.
	if got := srv.SetupTokenForTest(); got != "" {
		t.Fatalf("token still present after successful setup: %q", got)
	}

	// State flips.
	resp2, err := http.Get(ts.URL + "/api/setup/state")
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		NeedsSetup bool `json:"needs_setup"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&st)
	resp2.Body.Close()
	if st.NeedsSetup {
		t.Fatal("expected needs_setup=false after admin creation")
	}

	// Repeated setup attempts → 410.
	resp3, err := http.Post(ts.URL+"/api/setup/admin", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusGone {
		t.Fatalf("repeat setup status: %d, want 410", resp3.StatusCode)
	}
}

func TestSetupAdminRejectsWrongToken(t *testing.T) {
	ts, _, _, _, _ := setupHarness(t)

	body, _ := json.Marshal(map[string]string{
		"token":    "obviously-wrong",
		"login":    "alice",
		"password": "supersecret",
	})
	resp, err := http.Post(ts.URL+"/api/setup/admin", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %d, want 401", resp.StatusCode)
	}
}
