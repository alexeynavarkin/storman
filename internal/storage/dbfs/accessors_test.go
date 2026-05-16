package dbfs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/storage"
)

// Cheap accessor + helper tests. These cover the small surface methods that
// the integration tests don't naturally touch but that real callers depend on.

func TestTrashDirAccessor(t *testing.T) {
	f, _ := newFS(t)
	if got := f.fs.TrashDir(); got != f.trashDir {
		t.Errorf("TrashDir: %q vs %q", got, f.trashDir)
	}
}

func TestStorageDirAccessor(t *testing.T) {
	f, _ := newFS(t)
	if got := f.flat.StorageDir(); got != f.storageDir {
		t.Errorf("StorageDir: %q vs %q", got, f.storageDir)
	}
}

func TestPathByIDRoundTrip(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/a/b", storage.MkdirOpts{Parents: true}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.fs, "/a/b/c.txt", []byte("hello"))

	info, err := f.fs.Stat(ctx, "/a/b/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.fs.PathByID(ctx, info.ID)
	if err != nil {
		t.Fatalf("PathByID: %v", err)
	}
	if got != "/a/b/c.txt" {
		t.Errorf("PathByID: %q", got)
	}
}

func TestPathByIDNotFound(t *testing.T) {
	f, ctx := newFS(t)
	_, err := f.fs.PathByID(ctx, uuid.New())
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("PathByID missing: %v", err)
	}
}

func TestActorPropagatesToTrashMeta(t *testing.T) {
	f, _ := newFS(t)
	actor := uuid.New()
	ctx := storage.WithActor(context.Background(), actor)
	writeFile(t, f.fs, "/x.txt", []byte("x"))
	if err := f.fs.Remove(ctx, "/x.txt"); err != nil {
		t.Fatal(err)
	}
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries: %d", len(entries))
	}
	if entries[0].DeletedBy != actor {
		t.Errorf("DeletedBy: got %s want %s", entries[0].DeletedBy, actor)
	}
}

// TestTruncateThroughFsWriter exercises fsWriter.Truncate which forwards to
// the inner backend writer; the flat-level Truncate is already covered, this
// just confirms the dbfs wrapper doesn't drop the call.
func TestTruncateThroughFsWriter(t *testing.T) {
	f, ctx := newFS(t)
	w, err := f.fs.OpenWrite(ctx, "/trunc.txt", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("0123456789"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := f.fs.OpenRead(ctx, "/trunc.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Size() != 4 {
		t.Errorf("size after truncate: %d", r.Size())
	}
}

// fakePgError wraps a known SQLSTATE so the isUniqueViolation predicate can
// be reached without staging an actual database collision.
func TestErrorsOnInvalidPathBubbleThroughOpenWrite(t *testing.T) {
	f, ctx := newFS(t)
	if _, err := f.fs.OpenWrite(ctx, "/", storage.WriteOpts{Mode: storage.WriteCreate}); !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("write to root: %v", err)
	}
	if _, err := f.fs.OpenWrite(ctx, "no-leading-slash", storage.WriteOpts{Mode: storage.WriteCreate}); !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("relative write: %v", err)
	}
	// Unsupported mode.
	if _, err := f.fs.OpenWrite(ctx, "/x", storage.WriteOpts{Mode: storage.WriteOverwrite}); !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("overwrite mode: %v", err)
	}
}

func TestRenameNoOpSamePath(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/x.txt", []byte("x"))
	if err := f.fs.Rename(ctx, "/x.txt", "/x.txt"); err != nil {
		t.Errorf("same-name rename: %v", err)
	}
	if _, err := f.fs.Stat(ctx, "/x.txt"); err != nil {
		t.Errorf("file gone after no-op rename: %v", err)
	}
}

// TestListEmptyAndFileTarget covers the early-return paths of List.
func TestListEmptyAndFileTarget(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/x.txt", []byte("x"))

	if _, err := f.fs.List(ctx, "/x.txt"); !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("List file: %v", err)
	}
	entries, err := f.fs.List(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "x.txt" {
		t.Errorf("List /: %v", entries)
	}
}

// Make sure unused imports don't bite as the file grows.
var _ = strings.Builder{}
