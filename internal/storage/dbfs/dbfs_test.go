package dbfs_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
)

type fixture struct {
	fs         *dbfs.DBFS
	pool       *pgxpool.Pool
	flat       *flat.Backend
	storageDir string
	uploadsDir string
	trashDir   string
}

func newFS(t *testing.T) (*fixture, context.Context) {
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

	flatBackend := flat.New(storageDir, uploadsDir)
	fs := dbfs.New(pool, trashDir, flatBackend)

	ctx := context.Background()
	if _, err := fs.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return &fixture{fs: fs, pool: pool, flat: flatBackend, storageDir: storageDir, uploadsDir: uploadsDir, trashDir: trashDir}, ctx
}

func TestBootstrapIsIdempotent(t *testing.T) {
	f, ctx := newFS(t)

	created, err := f.fs.Bootstrap(ctx)
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	if created {
		t.Error("second Bootstrap should report created=false")
	}

	info, err := f.fs.Stat(ctx, "/")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if info.Type != storage.NodeDir {
		t.Errorf("root type: %v", info.Type)
	}
	if info.Children != 0 {
		t.Errorf("fresh root should have 0 children, got %d", info.Children)
	}
}

func TestMkdirAndList(t *testing.T) {
	f, ctx := newFS(t)

	if err := f.fs.Mkdir(ctx, "/photos", storage.MkdirOpts{}); err != nil {
		t.Fatalf("mkdir /photos: %v", err)
	}
	if err := f.fs.Mkdir(ctx, "/docs", storage.MkdirOpts{}); err != nil {
		t.Fatalf("mkdir /docs: %v", err)
	}

	entries, err := f.fs.List(ctx, "/")
	if err != nil {
		t.Fatalf("list /: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 children, got %d: %+v", len(entries), entries)
	}
}

func TestMkdirParents(t *testing.T) {
	f, ctx := newFS(t)

	err := f.fs.Mkdir(ctx, "/a/b/c", storage.MkdirOpts{})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	if err := f.fs.Mkdir(ctx, "/a/b/c", storage.MkdirOpts{Parents: true}); err != nil {
		t.Fatalf("mkdir -p: %v", err)
	}
	if err := f.fs.Mkdir(ctx, "/a/b/c", storage.MkdirOpts{Parents: true}); err != nil {
		t.Fatalf("idempotent mkdir -p: %v", err)
	}
	if err := f.fs.Mkdir(ctx, "/a", storage.MkdirOpts{}); !errors.Is(err, storage.ErrExists) {
		t.Errorf("expected ErrExists, got %v", err)
	}
}

func TestStatNotFound(t *testing.T) {
	f, ctx := newFS(t)
	_, err := f.fs.Stat(ctx, "/does/not/exist")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestWriteCreateRoundtrip(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/docs", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	data := []byte("hello, dbfs")
	writeFile(t, f.fs, "/docs/hello.txt", data)

	info, err := f.fs.Stat(ctx, "/docs/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Type != storage.NodeFile {
		t.Errorf("type: %v", info.Type)
	}
	if info.Size != int64(len(data)) {
		t.Errorf("size: %d vs %d", info.Size, len(data))
	}

	r, err := f.fs.OpenRead(ctx, "/docs/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.Size() != int64(len(data)) {
		t.Errorf("reader size: %d", r.Size())
	}
	buf := make([]byte, len(data))
	if _, err := r.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf) != string(data) {
		t.Errorf("content: %q", buf)
	}
}

func TestWriteRejectsDuplicate(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/x", []byte("first"))

	w, err := f.fs.OpenWrite(ctx, "/x", storage.WriteOpts{Mode: storage.WriteCreate})
	if !errors.Is(err, storage.ErrExists) {
		t.Fatalf("expected ErrExists, got %v (writer=%v)", err, w)
	}
}

func TestWriteCloseWithoutFinalizeRollsBack(t *testing.T) {
	f, ctx := newFS(t)

	w, err := f.fs.OpenWrite(ctx, "/orphan", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("dangling"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, storage.ErrUnfinalized) {
		t.Fatalf("expected ErrUnfinalized, got %v", err)
	}

	if _, err := f.fs.Stat(ctx, "/orphan"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("orphan should be rolled back: %v", err)
	}
	// staging cleaned, target never created
	if entries, _ := os.ReadDir(f.uploadsDir); len(entries) != 0 {
		t.Errorf("staging leaked: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(f.storageDir, "orphan")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("target leaked: %v", err)
	}
}

func TestRemoveFileGoesToTrash(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/one.txt", []byte("one"))

	if _, err := os.Stat(filepath.Join(f.storageDir, "one.txt")); err != nil {
		t.Fatalf("expected published file: %v", err)
	}

	if err := f.fs.Remove(ctx, "/one.txt"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/one.txt"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("file should be gone from active tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.storageDir, "one.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("flat-storage entry should have moved to trash: %v", err)
	}
	if !trashHasEntry(t, f.trashDir, []byte("one")) {
		t.Error("trash dir does not contain the removed file payload")
	}
}

func TestRemoveDirRecursivelyMovesSubtree(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/dir/nested", storage.MkdirOpts{Parents: true}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.fs, "/dir/a.txt", []byte("a"))
	writeFile(t, f.fs, "/dir/nested/b.txt", []byte("b"))

	if err := f.fs.Remove(ctx, "/dir"); err != nil {
		t.Fatalf("recursive remove: %v", err)
	}

	for _, p := range []string{"/dir", "/dir/a.txt", "/dir/nested", "/dir/nested/b.txt"} {
		if _, err := f.fs.Stat(ctx, p); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("%s should be soft-deleted, got err=%v", p, err)
		}
	}

	if _, err := os.Stat(filepath.Join(f.storageDir, "dir")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("flat-storage/dir should have moved to trash, stat=%v", err)
	}
	if !trashHasEntry(t, f.trashDir, []byte("b")) {
		t.Error("trash payload missing nested file content")
	}

	// Name is free for reuse.
	if err := f.fs.Mkdir(ctx, "/dir", storage.MkdirOpts{}); err != nil {
		t.Errorf("name reuse after recursive remove: %v", err)
	}
}

// trashHasEntry walks the trashDir looking for a file whose content equals
// wantBytes. Returns true on first match — good enough to assert that a known
// payload landed in trash regardless of trash_uuid.
func trashHasEntry(t *testing.T, trashDir string, wantBytes []byte) bool {
	t.Helper()
	found := false
	err := filepath.Walk(trashDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if info.Name() == dbfs.TrashMetaFile {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(data) == string(wantBytes) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk trash: %v", err)
	}
	return found
}

func TestRenameWithinAndAcrossDirs(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/a/b", storage.MkdirOpts{Parents: true}); err != nil {
		t.Fatal(err)
	}
	if err := f.fs.Mkdir(ctx, "/dst", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.fs, "/a/b/file.txt", []byte("content"))

	// Rename within same parent.
	if err := f.fs.Rename(ctx, "/a/b/file.txt", "/a/b/renamed.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/a/b/renamed.txt"); err != nil {
		t.Errorf("stat after rename: %v", err)
	}

	// Move dir across parents — descendants must follow.
	if err := f.fs.Rename(ctx, "/a/b", "/dst/b"); err != nil {
		t.Fatalf("move dir: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/dst/b/renamed.txt"); err != nil {
		t.Errorf("descendant lost during dir rename: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/a/b"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("old dir should be gone: %v", err)
	}

	// Rename onto an existing name fails.
	writeFile(t, f.fs, "/dst/taken", []byte("x"))
	if err := f.fs.Rename(ctx, "/dst/b", "/dst/taken"); !errors.Is(err, storage.ErrExists) {
		t.Errorf("expected ErrExists, got %v", err)
	}

	// Loop check: cannot move dir into its own subtree.
	if err := f.fs.Rename(ctx, "/dst", "/dst/b/sub"); !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got %v", err)
	}
}

func TestRecoverPendingPromotesLandedContent(t *testing.T) {
	f, ctx := newFS(t)

	// Start a write and Commit the backend bytes but skip the FS-level Commit
	// (simulating a crash between backend rename and tx2). We do this by
	// reaching into the backend directly to land the bytes, while the FS state
	// stays pending. Easiest path: drive OpenWrite normally, but use the
	// internal Abort? No — we need pending state with content present. Build
	// it manually.
	//
	// Strategy: OpenWrite, WriteAt, Commit (the FS does inner.Commit then
	// tx2). To leave node 'pending', we hack the DB after Commit returns.
	w, err := f.fs.OpenWrite(ctx, "/recover.txt", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("recoverable"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	// Simulate crash: reset node to 'pending' and recreate outbox row as if
	// tx2 never ran.
	if _, err := f.pool.Exec(ctx, `UPDATE nodes SET status='pending' WHERE name='recover.txt'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		INSERT INTO outbox (op, node_id, payload, status)
		SELECT 'create_file', id, jsonb_build_object('node_id', id, 'backend_kind', backend_kind, 'backend_ref', backend_ref), 'pending'
		FROM nodes WHERE name='recover.txt'`); err != nil {
		t.Fatal(err)
	}

	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 1 {
		t.Errorf("acted: %d", acted)
	}

	info, err := f.fs.Stat(ctx, "/recover.txt")
	if err != nil {
		t.Fatalf("stat after recover: %v", err)
	}
	if info.Size != int64(len("recoverable")) {
		t.Errorf("size after recover: %d", info.Size)
	}
}

func TestRecoverPendingDropsMissingContent(t *testing.T) {
	f, ctx := newFS(t)

	// Build a pending node + outbox by hand whose backend ref points nowhere.
	if _, err := f.pool.Exec(ctx, `
		WITH ins AS (
		  INSERT INTO nodes (parent_id, path, name, type, backend_kind, backend_ref, status)
		  SELECT id, path || 'ghost'::ltree, 'ghost.txt', 'file', 'flat', 'ghost.txt', 'pending'
		  FROM nodes WHERE parent_id IS NULL
		  RETURNING id
		)
		INSERT INTO outbox (op, node_id, payload, status)
		SELECT 'create_file', id, jsonb_build_object('node_id', id, 'backend_kind', 'flat', 'backend_ref', 'ghost.txt'), 'pending'
		FROM ins`); err != nil {
		t.Fatal(err)
	}

	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 1 {
		t.Errorf("acted: %d", acted)
	}

	if _, err := f.fs.Stat(ctx, "/ghost.txt"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("pending node should be gone: %v", err)
	}
}

// writeFile is a small helper that creates a file via the FS-level OpenWrite
// lifecycle, with proper Commit + Close.
func writeFile(t *testing.T, fs *dbfs.DBFS, path string, data []byte) {
	t.Helper()
	w, err := fs.OpenWrite(context.Background(), path, storage.WriteOpts{Mode: storage.WriteCreate})
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

func TestRestoreBringsSubtreeBack(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/dir", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.fs, "/dir/a.txt", []byte("aaa"))

	if err := f.fs.Remove(ctx, "/dir"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatalf("list trash: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("trash size: %d, want 1", len(entries))
	}

	if err := f.fs.Restore(ctx, entries[0].TrashUUID); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if _, err := f.fs.Stat(ctx, "/dir"); err != nil {
		t.Errorf("dir after restore: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/dir/a.txt"); err != nil {
		t.Errorf("file after restore: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(f.storageDir, "dir", "a.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "aaa" {
		t.Errorf("content after restore: %q", data)
	}

	post, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(post) != 0 {
		t.Errorf("trash should be empty after restore, got %d", len(post))
	}
}

func TestPurgeRemovesEverything(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/doomed.txt", []byte("bye"))
	if err := f.fs.Remove(ctx, "/doomed.txt"); err != nil {
		t.Fatal(err)
	}
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("trash size: %d", len(entries))
	}

	if err := f.fs.Purge(ctx, entries[0].TrashUUID); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// DB row gone (no soft-deleted record either).
	var count int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM nodes WHERE id = $1`, entries[0].RootNodeID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("node row still present after purge: count=%d", count)
	}
	if _, err := os.Stat(filepath.Join(f.trashDir, entries[0].TrashUUID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("trash dir still present after purge: %v", err)
	}
}

func TestRunGCRespectsRetention(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/old.txt", []byte("old"))
	writeFile(t, f.fs, "/fresh.txt", []byte("fresh"))
	if err := f.fs.Remove(ctx, "/old.txt"); err != nil {
		t.Fatal(err)
	}
	if err := f.fs.Remove(ctx, "/fresh.txt"); err != nil {
		t.Fatal(err)
	}

	// Backdate one trash entry's deleted_at by hacking its meta file.
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries: %d", len(entries))
	}
	old := entries[0]
	metaPath := filepath.Join(f.trashDir, old.TrashUUID.String(), dbfs.TrashMetaFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	// Replace deleted_at with a date a year in the past.
	frozen := time.Now().AddDate(-1, 0, 0).UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	patched := strings.Replace(string(data), old.DeletedAt.Format("2006-01-02T15:04:05.999999999Z07:00"), frozen, 1)
	if err := os.WriteFile(metaPath, []byte(patched), 0o600); err != nil {
		t.Fatal(err)
	}

	purged, err := f.fs.RunGC(ctx, 7*24*time.Hour, nil)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if purged != 1 {
		t.Errorf("purged %d, want 1", purged)
	}
	post, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(post) != 1 {
		t.Errorf("trash size after gc: %d, want 1 (recent entry preserved)", len(post))
	}
}
