package web_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

// davHarness bundles the WebDAV-enabled httptest server plus a primed user
// (root admin) and the matching app-password the tests authenticate with.
type davHarness struct {
	*harness
	login string
	token string
}

func newDavHarness(t *testing.T) *davHarness {
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

	server := web.NewServer(web.Config{SecureCookies: false}, users, sessions, perms, auth.NewShareLinkService(pool), fs, auditSvc).
		SetDav(true, "/dav")
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)

	ctx := context.Background()
	user, err := users.Create(ctx, "dav-user", "hunter22-main")
	if err != nil {
		t.Fatal(err)
	}
	rootInfo, err := fs.Stat(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	if err := perms.Grant(ctx, rootInfo.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove); err != nil {
		t.Fatal(err)
	}
	token := "dav-token-long-enough-1234"
	if _, err := users.CreateAppPassword(ctx, user.ID, "test", token); err != nil {
		t.Fatal(err)
	}

	return &davHarness{
		harness: &harness{t: t, server: ts, users: users, perms: perms, fs: fs, audit: auditSvc, trashDir: trashDir},
		login:   "dav-user",
		token:   token,
	}
}

// dav builds a request to /dav<path> (path must start with "/"), optionally
// applying a body and request-shaping fn. Sets Basic auth using the harness
// credentials unless skip=true.
func (h *davHarness) dav(method, p string, body []byte, skipAuth bool, modify func(*http.Request)) *http.Response {
	h.t.Helper()
	var br io.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, h.server.URL+"/dav"+p, br)
	if err != nil {
		h.t.Fatal(err)
	}
	if !skipAuth {
		req.SetBasicAuth(h.login, h.token)
	}
	if modify != nil {
		modify(req)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func TestWebDAVRequiresBasicAuth(t *testing.T) {
	h := newDavHarness(t)
	resp := h.dav(http.MethodOptions, "/", nil, true, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, "Basic ") {
		t.Errorf("missing Basic challenge: %q", got)
	}
}

func TestWebDAVRejectsInvalidCreds(t *testing.T) {
	h := newDavHarness(t)
	resp := h.dav(http.MethodOptions, "/", nil, false, func(r *http.Request) {
		r.SetBasicAuth(h.login, "wrong-token")
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: %d", resp.StatusCode)
	}

	rows, err := h.audit.List(context.Background(), audit.Filter{Action: audit.ActionLoginFailed, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	saw := false
	for _, r := range rows {
		if ch, _ := r.Details["channel"].(string); ch == "webdav" {
			saw = true
		}
	}
	if !saw {
		t.Error("missing webdav login_failed audit event")
	}
}

func TestWebDAVPutGetRoundtrip(t *testing.T) {
	h := newDavHarness(t)
	body := []byte("hello over webdav")

	resp := h.dav(http.MethodPut, "/note.txt", body, false, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status: %d", resp.StatusCode)
	}

	resp = h.dav(http.MethodGet, "/note.txt", nil, false, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("GET body: %q want %q", got, body)
	}
}

func TestWebDAVMkcolAndDelete(t *testing.T) {
	h := newDavHarness(t)

	resp := h.dav("MKCOL", "/folder", nil, false, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("MKCOL status: %d", resp.StatusCode)
	}

	// Sanity: directory shows up via Stat.
	if info, err := h.fs.Stat(context.Background(), "/folder"); err != nil {
		t.Fatalf("post-MKCOL stat: %v", err)
	} else if info.Type != "dir" {
		t.Fatalf("type: %s", info.Type)
	}

	resp = h.dav(http.MethodDelete, "/folder", nil, false, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: %d", resp.StatusCode)
	}

	entries, err := h.fs.ListTrash(context.Background())
	if err != nil {
		t.Fatalf("list trash: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected trash entry after DELETE")
	}
}

func TestWebDAVMoveRename(t *testing.T) {
	h := newDavHarness(t)
	resp := h.dav(http.MethodPut, "/old.txt", []byte("move me"), false, nil)
	resp.Body.Close()

	resp = h.dav("MOVE", "/old.txt", nil, false, func(r *http.Request) {
		r.Header.Set("Destination", h.server.URL+"/dav/new.txt")
		r.Header.Set("Overwrite", "F")
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("MOVE status: %d", resp.StatusCode)
	}

	resp = h.dav(http.MethodGet, "/new.txt", nil, false, nil)
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "move me" {
		t.Errorf("post-move body: %q", got)
	}
}

func TestWebDAVRespectsACL(t *testing.T) {
	h := newDavHarness(t)
	ctx := context.Background()

	// Second user, no perms at all.
	other, err := h.users.Create(ctx, "noperms", "hunter22-other")
	if err != nil {
		t.Fatal(err)
	}
	otherToken := "noperms-token-long-1234"
	if _, err := h.users.CreateAppPassword(ctx, other.ID, "t", otherToken); err != nil {
		t.Fatal(err)
	}

	resp := h.dav(http.MethodPut, "/forbidden.txt", []byte("x"), false, func(r *http.Request) {
		r.SetBasicAuth("noperms", otherToken)
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusNotFound {
		// webdav returns 404 when Stat denies (parent unreachable) or 403
		// when we explicitly deny on parent. Both are acceptable; allowing a
		// 200 would be the actual bug.
		t.Errorf("expected 403 or 404, got %d", resp.StatusCode)
	}
}

// TestWebDAVAuthCacheSurvivesBurst reproduces the macOS Finder pattern that
// previously broke folder operations: many parallel requests with the same
// valid credentials. With the auth cache in place the limiter should not trip.
func TestWebDAVAuthCacheSurvivesBurst(t *testing.T) {
	h := newDavHarness(t)

	const burst = 50
	type result struct{ status int }
	results := make(chan result, burst)
	for i := 0; i < burst; i++ {
		go func() {
			resp := h.dav(http.MethodOptions, "/", nil, false, nil)
			resp.Body.Close()
			results <- result{status: resp.StatusCode}
		}()
	}
	for i := 0; i < burst; i++ {
		r := <-results
		if r.status == http.StatusTooManyRequests {
			t.Fatalf("burst request %d hit rate limiter despite valid creds", i)
		}
	}

	// No webdav rate_limited audit should have been emitted for our login.
	rows, err := h.audit.List(context.Background(), audit.Filter{Action: audit.ActionLoginFailed, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if ch, _ := r.Details["channel"].(string); ch != "webdav" {
			continue
		}
		if reason, _ := r.Details["reason"].(string); reason == "rate_limited" {
			t.Errorf("unexpected rate_limited audit during valid-credentials burst")
		}
	}
}

// TestWebDAVInvalidCredsStillRateLimited confirms the cache does not weaken
// brute-force protection: 50 wrong-password attempts must still trip the
// limiter and generate audit events.
func TestWebDAVInvalidCredsStillRateLimited(t *testing.T) {
	h := newDavHarness(t)

	denied := 0
	for i := 0; i < 50; i++ {
		resp := h.dav(http.MethodOptions, "/", nil, false, func(r *http.Request) {
			r.SetBasicAuth(h.login, "wrong-token")
		})
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			denied++
		}
	}
	if denied == 0 {
		t.Error("expected the limiter to trip on a sustained wrong-password burst")
	}
}

func TestWebDAVRangedGet(t *testing.T) {
	h := newDavHarness(t)
	body := []byte("abcdefghijklmnopqrstuvwxyz")
	resp := h.dav(http.MethodPut, "/alphabet.txt", body, false, nil)
	resp.Body.Close()

	resp = h.dav(http.MethodGet, "/alphabet.txt", nil, false, func(r *http.Request) {
		r.Header.Set("Range", "bytes=10-14")
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status: %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "klmno" {
		t.Errorf("range body: %q", got)
	}
}
