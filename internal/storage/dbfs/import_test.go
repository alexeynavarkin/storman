package dbfs_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// stageImport writes a payload into the uploads dir and returns its absolute
// path. Mirrors how tus parks an assembled upload before calling ImportPath.
func stageImport(t *testing.T, uploadsDir string, name string, data []byte) string {
	t.Helper()
	src := filepath.Join(uploadsDir, name)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("stage %s: %v", src, err)
	}
	return src
}

func TestImportPathHappyPath(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/uploads", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	src := stageImport(t, f.uploadsDir, "incoming.bin", []byte("imported content"))
	if err := f.fs.ImportPath(ctx, "/uploads/incoming.bin", src); err != nil {
		t.Fatalf("ImportPath: %v", err)
	}

	// Source must have moved (os.Rename, not copy).
	if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("source not consumed by rename: %v", err)
	}

	info, err := f.fs.Stat(ctx, "/uploads/incoming.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len("imported content")) {
		t.Errorf("size: %d", info.Size)
	}

	r, err := f.fs.OpenRead(ctx, "/uploads/incoming.bin")
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer r.Close()
	buf := make([]byte, info.Size)
	if _, err := r.ReadAt(buf, 0); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	if string(buf) != "imported content" {
		t.Errorf("content: %q", buf)
	}

	// outbox must be archived (no pending row referring to this node).
	var pending int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox o JOIN nodes n ON n.id = o.node_id WHERE n.name='incoming.bin'`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("outbox row left pending after ImportPath: %d", pending)
	}
}

func TestImportPathRejectsMissingSource(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/uploads", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	err := f.fs.ImportPath(ctx, "/uploads/nope.bin",
		filepath.Join(f.uploadsDir, "does-not-exist"))
	if err == nil {
		t.Fatal("expected error for missing source")
	}

	// No node should have been left behind.
	if _, err := f.fs.Stat(ctx, "/uploads/nope.bin"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("node leaked after failed ImportPath: %v", err)
	}
	var leaked int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM nodes WHERE name='nope.bin' AND deleted_at IS NULL`,
	).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Errorf("leaked %d nodes after failed import", leaked)
	}
}

func TestImportPathRejectsDuplicate(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/taken.txt", []byte("first"))

	src := stageImport(t, f.uploadsDir, "alt.bin", []byte("second"))
	err := f.fs.ImportPath(ctx, "/taken.txt", src)
	if !errors.Is(err, storage.ErrExists) {
		t.Errorf("expected ErrExists, got %v", err)
	}

	// Source preserved on conflict — caller may retry with a different path.
	if _, err := os.Stat(src); err != nil {
		t.Errorf("source destroyed on duplicate target: %v", err)
	}

	// Original content intact.
	r, _ := f.fs.OpenRead(ctx, "/taken.txt")
	if r != nil {
		defer r.Close()
		if r.Size() != int64(len("first")) {
			t.Errorf("original size changed: %d", r.Size())
		}
	}
}

func TestImportPathRejectsDirSource(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/uploads", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	dirSrc := filepath.Join(f.uploadsDir, "subdir")
	if err := os.Mkdir(dirSrc, 0o700); err != nil {
		t.Fatal(err)
	}
	err := f.fs.ImportPath(ctx, "/uploads/x", dirSrc)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got %v", err)
	}
}

func TestImportPathIntoRootIsRejected(t *testing.T) {
	f, ctx := newFS(t)
	src := stageImport(t, f.uploadsDir, "root.bin", []byte("x"))
	err := f.fs.ImportPath(ctx, "/", src)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got %v", err)
	}
	// And the file should not be consumed.
	if _, err := os.Stat(src); err != nil {
		t.Errorf("source consumed despite invalid target: %v", err)
	}
	_ = context.Background()
}
