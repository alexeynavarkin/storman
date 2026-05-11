package backup

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestEnforceRetentionKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Add(-10 * time.Hour)
	names := []string{"a.dump", "b.dump", "c.dump", "d.dump", "e.dump", "ignore.txt"}
	for i, n := range names {
		path := filepath.Join(dir, n)
		if err := os.WriteFile(path, []byte(n), 0o600); err != nil {
			t.Fatal(err)
		}
		ts := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatal(err)
		}
	}

	if err := enforceRetention(dir, 3); err != nil {
		t.Fatalf("enforce: %v", err)
	}

	got := remaining(t, dir)
	want := []string{"c.dump", "d.dump", "e.dump", "ignore.txt"}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("remaining: %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] %q != %q", i, got[i], want[i])
		}
	}
}

func TestEnforceRetentionUnderLimit(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"a.dump", "b.dump"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := enforceRetention(dir, 5); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	got := remaining(t, dir)
	if len(got) != 2 {
		t.Errorf("expected 2 files left, got %d", len(got))
	}
}

func TestLatestDumpEmpty(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "meta-storage", "backups"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := LatestDump(dataDir)
	if err != ErrNoDump {
		t.Errorf("expected ErrNoDump, got %v", err)
	}
}

func TestLatestDumpPicksNewest(t *testing.T) {
	dataDir := t.TempDir()
	backupsDir := filepath.Join(dataDir, "meta-storage", "backups")
	if err := os.MkdirAll(backupsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-5 * time.Hour)
	for i, n := range []string{"first.dump", "middle.dump", "newest.dump"} {
		path := filepath.Join(backupsDir, n)
		if err := os.WriteFile(path, []byte(n), 0o600); err != nil {
			t.Fatal(err)
		}
		ts := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatal(err)
		}
	}
	got, err := LatestDump(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(got) != "newest.dump" {
		t.Errorf("picked %q, want newest.dump", filepath.Base(got))
	}
}

func remaining(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
