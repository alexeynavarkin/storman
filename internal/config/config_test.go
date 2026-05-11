package config

import (
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func writeValid(t *testing.T, dir string) string {
	t.Helper()
	key := make([]byte, SecretsKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	cfg := Default(dir)
	cfg.SecretsKey = base64.StdEncoding.EncodeToString(key)
	path := filepath.Join(dir, "config.json")
	if err := Save(path, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	dir := t.TempDir()
	path := writeValid(t, dir)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.DataDir != dir {
		t.Errorf("data_dir: got %q, want %q", got.DataDir, dir)
	}
	if time.Duration(got.Backup.Interval) != 24*time.Hour {
		t.Errorf("default interval not 24h: %v", got.Backup.Interval)
	}
	if got.Backup.Retention != 7 {
		t.Errorf("default retention not 7: %v", got.Backup.Retention)
	}
}

func TestLoadRejectsMismatchedDataDir(t *testing.T) {
	dir := t.TempDir()
	path := writeValid(t, dir)

	// Mutate data_dir to something unrelated.
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cfg.DataDir = "/some/elsewhere"
	if err := Save(path, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected validation error for mismatched data_dir")
	}
}

func TestLoadRejectsShortSecretsKey(t *testing.T) {
	dir := t.TempDir()
	cfg := Default(dir)
	cfg.SecretsKey = base64.StdEncoding.EncodeToString([]byte("too-short"))
	path := filepath.Join(dir, "config.json")
	if err := Save(path, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected secrets_key validation error")
	}
}

func TestLoadRejectsBadDuration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	bad := `{"data_dir":"` + dir + `","database":{"dsn":"postgres://x"},"secrets_key":"","backup":{"interval":"not-a-duration","retention":7}}`
	if err := writeFile(path, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected duration parse error")
	}
}
