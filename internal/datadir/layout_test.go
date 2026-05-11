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
