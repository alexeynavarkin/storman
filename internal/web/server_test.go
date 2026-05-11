package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

type harness struct {
	t        *testing.T
	server   *httptest.Server
	users    *auth.UserService
	perms    *rbac.PermissionService
	fs       *dbfs.DBFS
	audit    *audit.Service
	trashDir string
}

// newHarness boots a complete in-memory(-ish) stack: ephemeral PG, FlatFile
// backend on a temp dir, real services, httptest.Server.
func newHarness(t *testing.T) *harness {
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
		t.Fatalf("bootstrap: %v", err)
	}

	server := web.NewServer(web.Config{SecureCookies: false}, users, sessions, perms, auth.NewShareLinkService(pool), fs, nil)
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)

	return &harness{t: t, server: ts, users: users, perms: perms, fs: fs, trashDir: trashDir}
}

// newHarnessWithAudit is identical to newHarness but wires a real
// audit.Service so the assertions can read back rows.
func newHarnessWithAudit(t *testing.T) *harness {
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
		t.Fatalf("bootstrap: %v", err)
	}
	auditSvc := audit.NewService(pool, nil)

	server := web.NewServer(web.Config{SecureCookies: false}, users, sessions, perms, auth.NewShareLinkService(pool), fs, auditSvc)
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)

	return &harness{t: t, server: ts, users: users, perms: perms, fs: fs, audit: auditSvc, trashDir: trashDir}
}

// client is a convenience wrapper for an authenticated HTTP client. It tracks
// cookies via a jar and stashes the CSRF token from the login response so
// mutating calls can include the X-CSRF-Token header automatically.
type client struct {
	t    *testing.T
	hc   *http.Client
	base string
	csrf string
}

func (h *harness) newClient() *client {
	jar, _ := cookiejar.New(nil)
	return &client{t: h.t, hc: &http.Client{Jar: jar}, base: h.server.URL}
}

func (c *client) do(method, path string, body any, opts ...func(*http.Request)) *http.Response {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		switch v := body.(type) {
		case []byte:
			reader = bytes.NewReader(v)
		case string:
			reader = strings.NewReader(v)
		default:
			data, err := json.Marshal(body)
			if err != nil {
				c.t.Fatal(err)
			}
			reader = bytes.NewReader(data)
		}
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		c.t.Fatal(err)
	}
	if body != nil {
		if _, isBytes := body.([]byte); !isBytes {
			if _, isString := body.(string); !isString {
				req.Header.Set("Content-Type", "application/json")
			}
		}
	}
	if c.csrf != "" && method != http.MethodGet {
		req.Header.Set("X-CSRF-Token", c.csrf)
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func (c *client) login(login, password string) {
	c.t.Helper()
	resp := c.do(http.MethodPost, "/api/auth/login", map[string]string{"login": login, "password": password})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		c.t.Fatalf("login: status %d body=%s", resp.StatusCode, body)
	}
	for _, ck := range resp.Cookies() {
		if ck.Name == "storman_csrf" {
			c.csrf = ck.Value
		}
	}
	if c.csrf == "" {
		c.t.Fatal("login response did not include csrf cookie")
	}
}

// --- Tests ---

func TestLoginAndMe(t *testing.T) {
	h := newHarness(t)
	if _, err := h.users.Create(context.Background(), "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/auth/me", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("me: %d", resp.StatusCode)
	}
	var me struct{ Login string }
	_ = json.NewDecoder(resp.Body).Decode(&me)
	if me.Login != "alice" {
		t.Errorf("me.Login: %q", me.Login)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	h := newHarness(t)
	if _, err := h.users.Create(context.Background(), "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}
	c := h.newClient()
	resp := c.do(http.MethodPost, "/api/auth/login", map[string]string{"login": "alice", "password": "wrong"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestUnauthenticatedRejected(t *testing.T) {
	h := newHarness(t)
	c := h.newClient()
	resp := c.do(http.MethodGet, "/api/fs/stat?path=/", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestCSRFRejection(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// Wipe CSRF header by overriding via opts.
	resp := c.do(http.MethodPost, "/api/fs/mkdir", map[string]any{"path": "/x"},
		func(r *http.Request) { r.Header.Del("X-CSRF-Token") })
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 on missing CSRF, got %d", resp.StatusCode)
	}
}

func TestFSRoundtrip(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// mkdir
	resp := c.do(http.MethodPost, "/api/fs/mkdir", map[string]any{"path": "/docs"})
	checkStatus(t, resp, http.StatusNoContent)

	// write
	body := []byte("hello, web")
	resp = c.do(http.MethodPut, "/api/fs/write?path=/docs/hello.txt", body)
	checkStatus(t, resp, http.StatusNoContent)

	// stat
	resp = c.do(http.MethodGet, "/api/fs/stat?path=/docs/hello.txt", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("stat: %d", resp.StatusCode)
	}
	var info struct {
		Size int64  `json:"size"`
		Type string `json:"type"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&info)
	resp.Body.Close()
	if info.Size != int64(len(body)) || info.Type != "file" {
		t.Errorf("stat: %+v", info)
	}

	// read full
	resp = c.do(http.MethodGet, "/api/fs/read?path=/docs/hello.txt", nil)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, body) {
		t.Errorf("read full: %q", got)
	}

	// read range bytes=2-5 ("llo,")
	resp = c.do(http.MethodGet, "/api/fs/read?path=/docs/hello.txt", nil,
		func(r *http.Request) { r.Header.Set("Range", "bytes=2-5") })
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status: %d", resp.StatusCode)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "llo," {
		t.Errorf("range body: %q", got)
	}

	// list
	resp = c.do(http.MethodGet, "/api/fs/list?path=/docs", nil)
	var entries []struct{ Name string }
	_ = json.NewDecoder(resp.Body).Decode(&entries)
	resp.Body.Close()
	if len(entries) != 1 || entries[0].Name != "hello.txt" {
		t.Errorf("list: %+v", entries)
	}

	// rename
	resp = c.do(http.MethodPost, "/api/fs/rename", map[string]any{"from": "/docs/hello.txt", "to": "/docs/world.txt"})
	checkStatus(t, resp, http.StatusNoContent)

	// remove
	resp = c.do(http.MethodDelete, "/api/fs/remove?path=/docs/world.txt", nil)
	checkStatus(t, resp, http.StatusNoContent)
}

func TestPermissionDenied(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	_ = user // no Grant — alice has zero permissions

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/fs/stat?path=/", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestSecurityHeadersPresent(t *testing.T) {
	h := newHarness(t)
	resp := h.newClient().do(http.MethodGet, "/api/auth/me", nil)
	defer resp.Body.Close()
	for _, header := range []string{"X-Frame-Options", "Referrer-Policy", "Content-Security-Policy", "Strict-Transport-Security"} {
		if resp.Header.Get(header) == "" {
			t.Errorf("missing header %q", header)
		}
	}
}

func TestUsersList(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.users.Create(ctx, "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.users.Create(ctx, "bob", "hunter22"); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/users", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var users []struct{ Login string }
	if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestPermListRequiresAdmin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	user, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	// Read-only grant — not Admin.
	if err := h.perms.Grant(ctx, root.ID, user.ID, rbac.Read); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/perm/list?path=/", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 without Admin, got %d", resp.StatusCode)
	}
}

func TestPermGrantAndRevoke(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	admin, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	subject, err := h.users.Create(ctx, "bob", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(ctx, root.ID, admin.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// Grant Read+Write to bob on /.
	resp := c.do(http.MethodPost, "/api/perm/grant", map[string]any{
		"path":    "/",
		"user_id": subject.ID.String(),
		"actions": int(rbac.Read | rbac.Write),
	})
	checkStatus(t, resp, http.StatusNoContent)

	// List shows bob.
	resp = c.do(http.MethodGet, "/api/perm/list?path=/", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("list: %d", resp.StatusCode)
	}
	var listResp struct {
		Explicit []struct {
			Login   string
			Actions int
		}
	}
	_ = json.NewDecoder(resp.Body).Decode(&listResp)
	resp.Body.Close()
	foundBob := false
	for _, e := range listResp.Explicit {
		if e.Login == "bob" {
			foundBob = true
			if e.Actions != int(rbac.Read|rbac.Write) {
				t.Errorf("bob actions: %d", e.Actions)
			}
		}
	}
	if !foundBob {
		t.Errorf("bob not in explicit list: %+v", listResp.Explicit)
	}

	// Revoke.
	resp = c.do(http.MethodPost, "/api/perm/revoke", map[string]any{
		"path":    "/",
		"user_id": subject.ID.String(),
	})
	checkStatus(t, resp, http.StatusNoContent)

	mask, err := h.perms.Effective(ctx, subject.ID, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mask != 0 {
		t.Errorf("bob still has perms after revoke: %08b", mask)
	}
}

func TestStatExposesEffective(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	user, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(ctx, root.ID, user.ID, rbac.Read|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/fs/stat?path=/", nil)
	defer resp.Body.Close()
	var dto struct{ Effective int }
	_ = json.NewDecoder(resp.Body).Decode(&dto)
	if dto.Effective != int(rbac.Read|rbac.Admin) {
		t.Errorf("effective mask: got %08b, want %08b", dto.Effective, int(rbac.Read|rbac.Admin))
	}
}

func TestMeReportsAdminFlag(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.users.Create(ctx, "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}
	bob, err := h.users.Create(ctx, "bob", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(ctx, root.ID, bob.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")
	resp := c.do(http.MethodGet, "/api/auth/me", nil)
	var me struct{ IsRootAdmin bool `json:"is_root_admin"` }
	_ = json.NewDecoder(resp.Body).Decode(&me)
	resp.Body.Close()
	if me.IsRootAdmin {
		t.Error("alice should not be root admin")
	}

	c2 := h.newClient()
	c2.login("bob", "hunter22")
	resp = c2.do(http.MethodGet, "/api/auth/me", nil)
	_ = json.NewDecoder(resp.Body).Decode(&me)
	resp.Body.Close()
	if !me.IsRootAdmin {
		t.Error("bob should be root admin")
	}
}

func TestUserCreateRequiresRootAdmin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.users.Create(ctx, "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")
	resp := c.do(http.MethodPost, "/api/users",
		map[string]string{"login": "intruder", "password": "secret123456"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestUserCreateAndDeleteAsAdmin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	admin, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(ctx, root.ID, admin.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// Create
	resp := c.do(http.MethodPost, "/api/users",
		map[string]string{"login": "bob", "password": "hunter22bobby"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: %d", resp.StatusCode)
	}
	var created struct{ ID, Login string }
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if created.Login != "bob" {
		t.Errorf("login: %q", created.Login)
	}

	// Duplicate
	resp = c.do(http.MethodPost, "/api/users",
		map[string]string{"login": "bob", "password": "hunter22bobby"})
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("dup status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Cannot delete self
	resp = c.do(http.MethodDelete, "/api/users/"+admin.ID.String(), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("self-delete should be 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Delete bob
	resp = c.do(http.MethodDelete, "/api/users/"+created.ID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Verify bob is gone
	if _, err := h.users.FindByLogin(ctx, "bob"); !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("bob still present: %v", err)
	}
}

func TestSelfPasswordChangeKeepsCurrentSession(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	user, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// Open a second session for the same user that should get kicked.
	other := h.newClient()
	other.login("alice", "hunter22")

	resp := c.do(http.MethodPost, "/api/users/"+user.ID.String()+"/password",
		map[string]string{"password": "newSecretValue"})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("password change status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Current session still works.
	resp = c.do(http.MethodGet, "/api/auth/me", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("self session after change: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// The parallel session got logged out.
	resp = other.do(http.MethodGet, "/api/auth/me", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("other session should be 401 after self password change, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Old password no longer authenticates.
	bad := h.newClient()
	resp = bad.do(http.MethodPost, "/api/auth/login",
		map[string]string{"login": "alice", "password": "hunter22"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("old password should be rejected: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestAdminPasswordResetTerminatesTargetSessions(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	admin, err := h.users.Create(ctx, "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	subject, err := h.users.Create(ctx, "bob", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(ctx, root.ID, admin.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	bobSess := h.newClient()
	bobSess.login("bob", "hunter22")

	adminSess := h.newClient()
	adminSess.login("alice", "hunter22")

	resp := adminSess.do(http.MethodPost, "/api/users/"+subject.ID.String()+"/password",
		map[string]string{"password": "newSecretBob1"})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("admin reset status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = bobSess.do(http.MethodGet, "/api/auth/me", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("bob session should be terminated: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = adminSess.do(http.MethodGet, "/api/auth/me", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("admin session should survive: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestSPAFallback(t *testing.T) {
	pool := testpg.Pool(t)
	root := t.TempDir()
	storageDir := filepath.Join(root, "flat-storage")
	uploadsDir := filepath.Join(root, "meta-storage", "uploads")
	for _, d := range []string{storageDir, uploadsDir} {
		_ = os.MkdirAll(d, 0o700)
	}

	users := auth.NewUserService(pool)
	users.SetTestParams(auth.HashParams{MemoryKiB: 8 * 1024, Iterations: 1, Parallelism: 1, SaltLen: 16, KeyLen: 32})
	users.MinPasswordLen = 6
	sessions := auth.NewSessionService(pool)
	perms := rbac.NewPermissionService(pool)
	fs := dbfs.New(pool, "", flat.New(storageDir, uploadsDir))
	if _, err := fs.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}

	uiDir := filepath.Join(root, "ui-dist")
	_ = os.MkdirAll(uiDir, 0o700)
	if err := os.WriteFile(filepath.Join(uiDir, "index.html"), []byte("<!doctype html><title>storman</title>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(uiDir, "asset.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	spa, err := web.SPAFromDir(uiDir)
	if err != nil {
		t.Fatal(err)
	}
	server := web.NewServer(web.Config{SecureCookies: false}, users, sessions, perms, auth.NewShareLinkService(pool), fs, nil).WithSPA(spa)
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)

	// SPA fallback: deep link returns index.html.
	resp, err := http.Get(ts.URL + "/files/deep/link")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("<title>storman</title>")) {
		t.Errorf("expected index.html fallback, got: %s", body)
	}

	// Real asset is served literally.
	resp2, err := http.Get(ts.URL + "/asset.txt")
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(body2) != "hello" {
		t.Errorf("asset content: %q", body2)
	}
}

// --- helpers ---

func mustStat(t *testing.T, h *harness, path string) struct{ ID uuid.UUID } {
	t.Helper()
	info, err := h.fs.Stat(context.Background(), path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return struct{ ID uuid.UUID }{ID: info.ID}
}

func checkStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d (want %d): %s", resp.StatusCode, want, body)
	}
}

func TestTrashRequiresRootAdmin(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	// Alice has Remove on root but NOT Admin — trash endpoints must reject her.
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")
	resp := c.do(http.MethodGet, "/api/trash", nil)
	checkStatus(t, resp, http.StatusForbidden)
}

func TestTrashListRestorePurgeFlow(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "admin", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("admin", "hunter22")

	// Set up: create + soft-delete one file.
	resp := c.do(http.MethodPut, "/api/fs/write?path=/doomed.txt", []byte("bye"))
	checkStatus(t, resp, http.StatusNoContent)
	resp = c.do(http.MethodDelete, "/api/fs/remove?path=/doomed.txt", nil)
	checkStatus(t, resp, http.StatusNoContent)

	// List.
	resp = c.do(http.MethodGet, "/api/trash", nil)
	var entries []struct {
		TrashUUID       string `json:"trash_uuid"`
		RootLogicalPath string `json:"root_logical_path"`
	}
	if resp.StatusCode != 200 {
		t.Fatalf("list status: %d", resp.StatusCode)
	}
	_ = json.NewDecoder(resp.Body).Decode(&entries)
	resp.Body.Close()
	if len(entries) != 1 || entries[0].RootLogicalPath != "/doomed.txt" {
		t.Fatalf("entries: %+v", entries)
	}

	// Restore.
	resp = c.do(http.MethodPost, "/api/trash/"+entries[0].TrashUUID+"/restore", nil)
	checkStatus(t, resp, http.StatusNoContent)
	resp = c.do(http.MethodGet, "/api/fs/stat?path=/doomed.txt", nil)
	checkStatus(t, resp, http.StatusOK)

	// Delete again, then purge.
	resp = c.do(http.MethodDelete, "/api/fs/remove?path=/doomed.txt", nil)
	checkStatus(t, resp, http.StatusNoContent)
	resp = c.do(http.MethodGet, "/api/trash", nil)
	_ = json.NewDecoder(resp.Body).Decode(&entries)
	resp.Body.Close()
	if len(entries) != 1 {
		t.Fatalf("post-redelete: %d entries", len(entries))
	}
	resp = c.do(http.MethodDelete, "/api/trash/"+entries[0].TrashUUID, nil)
	checkStatus(t, resp, http.StatusNoContent)

	resp = c.do(http.MethodGet, "/api/trash", nil)
	_ = json.NewDecoder(resp.Body).Decode(&entries)
	resp.Body.Close()
	if len(entries) != 0 {
		t.Errorf("trash should be empty after purge, got %d", len(entries))
	}
}

func TestAuditLoginAndDenied(t *testing.T) {
	h := newHarnessWithAudit(t)
	if _, err := h.users.Create(context.Background(), "auditor", "hunter22"); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()

	// Wrong password — expect denied row.
	resp := c.do(http.MethodPost, "/api/auth/login", map[string]string{"login": "auditor", "password": "wrong"})
	resp.Body.Close()

	// Correct password — expect ok row.
	c.login("auditor", "hunter22")

	rows, err := h.audit.List(context.Background(), audit.Filter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var sawOK, sawDenied bool
	for _, r := range rows {
		if r.Action == audit.ActionLogin && r.Result == audit.ResultOK {
			sawOK = true
		}
		if r.Action == audit.ActionLoginFailed && r.Result == audit.ResultDenied {
			sawDenied = true
		}
	}
	if !sawOK {
		t.Error("missing ok login event")
	}
	if !sawDenied {
		t.Error("missing denied login event")
	}
}

func TestAuditEndpointRequiresAdmin(t *testing.T) {
	h := newHarnessWithAudit(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write); err != nil {
		t.Fatal(err)
	}
	c := h.newClient()
	c.login("alice", "hunter22")
	resp := c.do(http.MethodGet, "/api/audit", nil)
	checkStatus(t, resp, http.StatusForbidden)
}

func TestShareLinkLifecycle(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID,
		rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	// Upload a file we can share.
	resp := c.do(http.MethodPut, "/api/fs/write?path=/note.txt", []byte("share me"))
	checkStatus(t, resp, http.StatusNoContent)

	// Create share with Read scope.
	resp = c.do(http.MethodPost, "/api/share", map[string]any{
		"path":        "/note.txt",
		"actions":     int(rbac.Read),
		"ttl_seconds": 3600,
	})
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create: %d %s", resp.StatusCode, body)
	}
	var created struct {
		Token string `json:"token"`
		URL   string `json:"url"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if created.Token == "" {
		t.Fatal("missing token")
	}

	// Anonymous client (no cookies/CSRF) hits the public endpoint.
	anon := &http.Client{}
	resp, err = anon.Get(h.server.URL + "/share/" + created.Token + "/info")
	if err != nil {
		t.Fatal(err)
	}
	checkStatus(t, resp, http.StatusOK)

	// Download once — should succeed.
	resp, err = anon.Get(h.server.URL + "/share/" + created.Token + "/download")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "share me" {
		t.Errorf("download: %q", body)
	}

	// Revoke (owner can).
	resp = c.do(http.MethodDelete, "/api/share/"+created.Token, nil)
	checkStatus(t, resp, http.StatusNoContent)

	// After revoke — 404.
	resp, _ = anon.Get(h.server.URL + "/share/" + created.Token + "/download")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("post-revoke: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestShareCreateRequiresAdmin(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	// Read+Write but NO Admin — alice shouldn't be able to share.
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write); err != nil {
		t.Fatal(err)
	}

	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodPost, "/api/fs/mkdir", map[string]any{"path": "/d"})
	checkStatus(t, resp, http.StatusNoContent)
	resp = c.do(http.MethodPost, "/api/share", map[string]any{
		"path":        "/d",
		"actions":     int(rbac.Read),
		"ttl_seconds": 3600,
	})
	checkStatus(t, resp, http.StatusForbidden)
}

func TestShareLinkRejectsAdminScope(t *testing.T) {
	h := newHarness(t)
	user, err := h.users.Create(context.Background(), "alice", "hunter22")
	if err != nil {
		t.Fatal(err)
	}
	root := mustStat(t, h, "/")
	if err := h.perms.Grant(context.Background(), root.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		t.Fatal(err)
	}
	c := h.newClient()
	c.login("alice", "hunter22")

	resp := c.do(http.MethodPost, "/api/share", map[string]any{
		"path":        "/",
		"actions":     int(rbac.Admin),
		"ttl_seconds": 3600,
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: %d", resp.StatusCode)
	}
}
