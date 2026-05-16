package storagetest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
)

// Fixture bundles the database pool, on-disk directory layout, and a fully
// wired DBFS over a flat backend. Replaces the ad-hoc `newFS` helpers that
// used to live in each storage test package.
type Fixture struct {
	FS     *dbfs.DBFS
	Pool   *pgxpool.Pool
	Flat   *flat.Backend
	Layout Layout
}

// NewFixture spins up a fresh DBFS on top of an isolated test database and a
// temp data dir. Bootstrap is run so the tree root is present. Cleanup is
// wired via t.Cleanup.
func NewFixture(t *testing.T) (*Fixture, context.Context) {
	t.Helper()
	pool := testpg.Pool(t)

	root := t.TempDir()
	layout := Layout{
		StorageDir: filepath.Join(root, "flat-storage"),
		UploadsDir: filepath.Join(root, "meta-storage", "uploads"),
		TrashDir:   filepath.Join(root, "meta-storage", "trash"),
	}
	for _, d := range []string{layout.StorageDir, layout.UploadsDir, layout.TrashDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	backend := flat.New(layout.StorageDir, layout.UploadsDir)
	fs := dbfs.New(pool, layout.TrashDir, backend)

	ctx := context.Background()
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return &Fixture{FS: fs, Pool: pool, Flat: backend, Layout: layout}, ctx
}

// AssertConsistent runs the consistency checker against this fixture.
func (f *Fixture) AssertConsistent(t *testing.T) {
	t.Helper()
	AssertConsistent(t, f.Pool, f.Layout)
}

// WriteFile is a small helper that creates a file via the FS-level OpenWrite
// lifecycle with proper Commit + Close. Centralises the boilerplate so test
// authors don't repeat it.
func (f *Fixture) WriteFile(t *testing.T, ctx context.Context, path string, data []byte) {
	t.Helper()
	w, err := f.FS.OpenWrite(ctx, path, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatalf("OpenWrite %q: %v", path, err)
	}
	if _, err := w.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt %q: %v", path, err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit %q: %v", path, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close %q: %v", path, err)
	}
}

// sprintf is a thin wrapper around fmt.Sprintf, kept in this package so the
// consistency checker stays self-contained.
func sprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
