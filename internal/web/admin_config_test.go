package web_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/web"
)

// adminCfgHarness boots a server backed by a real config.json file so the
// admin settings endpoints have somewhere to write.
type adminCfgHarness struct {
	t       *testing.T
	server  *httptest.Server
	users   *auth.UserService
	perms   *rbac.PermissionService
	fs      *dbfs.DBFS
	cfgPath string
}

func newAdminCfgHarness(t *testing.T) *adminCfgHarness {
	t.Helper()
	pool := testpg.Pool(t)
	dataDir := t.TempDir()
	storageDir := filepath.Join(dataDir, "flat-storage")
	uploadsDir := filepath.Join(dataDir, "meta-storage", "uploads")
	trashDir := filepath.Join(dataDir, "meta-storage", "trash")
	for _, d := range []string{storageDir, uploadsDir, trashDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	key := make([]byte, config.SecretsKeyLen)
	cfg := config.Default(dataDir)
	cfg.SecretsKey = base64.StdEncoding.EncodeToString(key)
	cfgPath := filepath.Join(dataDir, "config.json")
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatalf("save: %v", err)
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
	server := web.NewServer(web.Config{
		SecureCookies: false,
		ConfigPath:    cfgPath,
	}, users, sessions, perms, auth.NewShareLinkService(pool), fs, auditSvc)
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)

	return &adminCfgHarness{t: t, server: ts, users: users, perms: perms, fs: fs, cfgPath: cfgPath}
}

func (h *adminCfgHarness) loginAsAdmin(login, password string) *client {
	h.t.Helper()
	ctx := context.Background()
	user, err := h.users.Create(ctx, login, password)
	if err != nil {
		h.t.Fatal(err)
	}
	rootInfo, err := h.fs.Stat(ctx, "/")
	if err != nil {
		h.t.Fatalf("stat root: %v", err)
	}
	if err := h.perms.Grant(ctx, rootInfo.ID, user.ID, rbac.Read|rbac.Write|rbac.Remove|rbac.Admin); err != nil {
		h.t.Fatal(err)
	}
	jar, _ := cookiejar.New(nil)
	c := &client{t: h.t, hc: &http.Client{Jar: jar}, base: h.server.URL}
	c.login(login, password)
	return c
}

func TestAdminConfigRoundtrip(t *testing.T) {
	h := newAdminCfgHarness(t)
	c := h.loginAsAdmin("alice", "hunter22")

	resp := c.do(http.MethodGet, "/api/admin/config", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET: status %d body=%s", resp.StatusCode, body)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	cfgMap, _ := got["config"].(map[string]any)
	if cfgMap == nil {
		t.Fatal("config missing in envelope")
	}
	if cfgMap["secrets_key"] != web.SecretsKeySentinel {
		t.Errorf("secrets_key not redacted: %v", cfgMap["secrets_key"])
	}
	if got["secrets_key_present"] != true {
		t.Errorf("secrets_key_present: %v", got["secrets_key_present"])
	}

	cfgMap["trash"].(map[string]any)["retention_days"] = float64(99)
	resp2 := c.do(http.MethodPut, "/api/admin/config", cfgMap)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("PUT: status %d body=%s", resp2.StatusCode, body)
	}
	var after map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&after); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if after["restart_pending"] != true {
		t.Errorf("restart_pending not set after PUT")
	}

	raw, err := os.ReadFile(h.cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var fileCfg map[string]any
	if err := json.Unmarshal(raw, &fileCfg); err != nil {
		t.Fatalf("parse file: %v", err)
	}
	if fileCfg["trash"].(map[string]any)["retention_days"].(float64) != 99 {
		t.Errorf("trash retention not persisted: %v", fileCfg["trash"])
	}
	if fileCfg["secrets_key"] == web.SecretsKeySentinel {
		t.Errorf("secrets_key overwritten with sentinel — should keep previous value")
	}
}

func TestAdminConfigNonAdmin(t *testing.T) {
	h := newAdminCfgHarness(t)
	if _, err := h.users.Create(context.Background(), "bob", "hunter22"); err != nil {
		t.Fatal(err)
	}
	jar, _ := cookiejar.New(nil)
	c := &client{t: h.t, hc: &http.Client{Jar: jar}, base: h.server.URL}
	c.login("bob", "hunter22")

	resp := c.do(http.MethodGet, "/api/admin/config", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("GET as non-admin: status %d, want 403", resp.StatusCode)
	}
}

func TestAdminConfigInvalidPayload(t *testing.T) {
	h := newAdminCfgHarness(t)
	c := h.loginAsAdmin("alice", "hunter22")

	// Empty DSN → Config.validate() rejects, file should NOT change.
	original, _ := os.ReadFile(h.cfgPath)

	body := map[string]any{
		"data_dir":    "/ignored", // ignored — server forces back to current
		"database":    map[string]any{"dsn": ""},
		"secrets_key": web.SecretsKeySentinel,
		"backup":      map[string]any{"interval": "24h", "retention": 7},
		"web":         map[string]any{"listen_addr": ":8443"},
		"trash":       map[string]any{"retention_days": 30, "gc_interval": "1h"},
		"ftp":         map[string]any{"passive_port_min": 50000, "passive_port_max": 50050},
		"indexing":    map[string]any{"workers": 2, "poll_interval": "5s"},
		"tus":         map[string]any{"retention_hours": 24, "sweep_interval": "1h"},
		"webdav":      map[string]any{"enabled": false, "path_prefix": "/dav"},
		"webauthn":    map[string]any{"rp_id": ""},
	}
	resp := c.do(http.MethodPut, "/api/admin/config", body)
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		t.Fatalf("expected non-2xx for invalid DSN, got %d", resp.StatusCode)
	}

	after, _ := os.ReadFile(h.cfgPath)
	if string(after) != string(original) {
		t.Errorf("config file mutated despite failed PUT")
	}
}
