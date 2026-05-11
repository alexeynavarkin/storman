package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
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

// secureHarness builds a server with the requested cookie/proxy flags and a
// pre-created user so we can hit /api/auth/login and inspect the response
// cookies directly without going through cookiejar (which strips Secure when
// running over plain HTTP).
func secureHarness(t *testing.T, cfg web.Config) (*httptest.Server, string, string) {
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
	if _, err := users.Create(context.Background(), "alice", "hunter22"); err != nil {
		t.Fatal(err)
	}

	srv := web.NewServer(cfg, users, sessions, perms, auth.NewShareLinkService(pool), fs, nil)
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)
	return ts, "alice", "hunter22"
}

func loginGetCookies(t *testing.T, ts *httptest.Server, login, password, forwardedProto string) []*http.Cookie {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	hc := &http.Client{Jar: jar}
	body, _ := json.Marshal(map[string]string{"login": login, "password": password})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if forwardedProto != "" {
		req.Header.Set("X-Forwarded-Proto", forwardedProto)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status: %d", resp.StatusCode)
	}
	return resp.Cookies()
}

func cookieByName(cookies []*http.Cookie, name string) *http.Cookie {
	for _, c := range cookies {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func TestSecureCookieRequiresHTTPSOrProxy(t *testing.T) {
	// Plain HTTP, no proxy → Secure must NOT be set even with SecureCookies=true.
	ts, login, pwd := secureHarness(t, web.Config{SecureCookies: true})
	cookies := loginGetCookies(t, ts, login, pwd, "")
	sess := cookieByName(cookies, "storman_session")
	if sess == nil {
		t.Fatal("session cookie missing")
	}
	if sess.Secure {
		t.Fatal("Secure flag set on plain HTTP without proxy trust")
	}
}

func TestSecureCookieHonoursForwardedProtoWhenTrusted(t *testing.T) {
	ts, login, pwd := secureHarness(t, web.Config{SecureCookies: true, TrustProxyHeaders: true})
	cookies := loginGetCookies(t, ts, login, pwd, "https")
	sess := cookieByName(cookies, "storman_session")
	if sess == nil || !sess.Secure {
		t.Fatalf("expected Secure session cookie behind trusted proxy, got %+v", sess)
	}
	csrf := cookieByName(cookies, "storman_csrf")
	if csrf == nil || !csrf.Secure {
		t.Fatalf("expected Secure csrf cookie behind trusted proxy, got %+v", csrf)
	}
}

func TestSecureCookieIgnoresForwardedProtoWhenNotTrusted(t *testing.T) {
	// SecureCookies=true but TrustProxyHeaders=false → header is ignored.
	ts, login, pwd := secureHarness(t, web.Config{SecureCookies: true})
	cookies := loginGetCookies(t, ts, login, pwd, "https")
	sess := cookieByName(cookies, "storman_session")
	if sess == nil {
		t.Fatal("session cookie missing")
	}
	if sess.Secure {
		t.Fatal("Secure flag set despite untrusted X-Forwarded-Proto")
	}
}
