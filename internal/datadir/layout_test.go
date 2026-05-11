package datadir

import (
	"os"
	"testing"

	"github.com/alexnav/storman/internal/config"
)

func TestBootstrapCreatesLayout(t *testing.T) {
	dir := t.TempDir()

	created, err := Bootstrap(dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on fresh dir")
	}

	for _, sub := range []string{StorageDir(dir), TrashDir(dir), UploadsDir(dir), BackupsDir(dir)} {
		info, err := os.Stat(sub)
		if err != nil {
			t.Fatalf("stat %s: %v", sub, err)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", sub)
		}
	}

	cfg, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatalf("load generated config: %v", err)
	}
	if cfg.DataDir != dir {
		t.Errorf("data_dir mismatch: %q vs %q", cfg.DataDir, dir)
	}
}

func TestBootstrapWithOverrides(t *testing.T) {
	dir := t.TempDir()
	trueVal := true

	created, err := BootstrapWithOverrides(dir, Overrides{
		DatabaseDSN:       "postgres://u:p@example:5432/db?sslmode=disable",
		ListenAddr:        ":9000",
		TrustProxyHeaders: &trueVal,
		SecureCookies:     &trueVal,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on fresh dir")
	}

	cfg, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got, want := cfg.Database.DSN, "postgres://u:p@example:5432/db?sslmode=disable"; got != want {
		t.Errorf("dsn: %q, want %q", got, want)
	}
	if got, want := cfg.Web.ListenAddr, ":9000"; got != want {
		t.Errorf("listen_addr: %q, want %q", got, want)
	}
	if !cfg.Web.TrustProxyHeaders {
		t.Error("trust_proxy_headers: want true")
	}
	if !cfg.Web.SecureCookies {
		t.Error("secure_cookies: want true")
	}
}

func TestBootstrapWithOverridesDoesNotRewriteExistingConfig(t *testing.T) {
	dir := t.TempDir()
	if _, err := Bootstrap(dir); err != nil {
		t.Fatalf("first: %v", err)
	}
	cfg1, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatal(err)
	}

	created, err := BootstrapWithOverrides(dir, Overrides{
		DatabaseDSN: "postgres://other:secret@host/db?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if created {
		t.Error("expected created=false on already-initialized dir")
	}

	cfg2, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if cfg1.Database.DSN != cfg2.Database.DSN {
		t.Errorf("dsn was rewritten despite existing config: %q → %q", cfg1.Database.DSN, cfg2.Database.DSN)
	}
}

func TestBootstrapIdempotent(t *testing.T) {
	dir := t.TempDir()
	if _, err := Bootstrap(dir); err != nil {
		t.Fatalf("first: %v", err)
	}
	cfg1, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatal(err)
	}

	created, err := Bootstrap(dir)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if created {
		t.Error("expected created=false on second run")
	}

	cfg2, err := config.Load(ConfigPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if cfg1.SecretsKey != cfg2.SecretsKey {
		t.Error("secrets_key was rewritten on second bootstrap — must be preserved")
	}
}
