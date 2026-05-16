package dbfs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// TestRestoreConflict: a deleted subtree can't be restored if the original
// name has since been reused. The trash entry must stay put for the operator
// to resolve manually.
func TestRestoreConflict(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/a.txt", []byte("first"))

	if err := f.fs.Remove(ctx, "/a.txt"); err != nil {
		t.Fatal(err)
	}
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("trash entries: %d", len(entries))
	}

	// Re-occupy the slot.
	writeFile(t, f.fs, "/a.txt", []byte("second"))

	err = f.fs.Restore(ctx, entries[0].TrashUUID)
	if !errors.Is(err, storage.ErrExists) {
		t.Errorf("expected ErrExists on Restore over occupied slot, got %v", err)
	}

	// Trash entry must remain — operator can still rename and retry.
	post, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(post) != 1 {
		t.Errorf("trash should still hold the entry after failed restore, got %d", len(post))
	}

	// Live tree still contains the new content, not the old one.
	r, err := f.fs.OpenRead(ctx, "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, r.Size())
	_, _ = r.ReadAt(buf, 0)
	if string(buf) != "second" {
		t.Errorf("live content overwritten by failed restore: %q", buf)
	}
}

// TestRenamePreservesBackendBinding: renaming a file (within or across
// directories) never alters the backend_kind / backend_ref columns. The
// content stays on disk where Allocate first put it; only the logical path
// in `nodes` changes.
func TestRenamePreservesBackendBinding(t *testing.T) {
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/src", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := f.fs.Mkdir(ctx, "/dst", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.fs, "/src/file.txt", []byte("payload"))

	var (
		beforeKind string
		beforeRef  string
		beforeID   string
	)
	if err := f.pool.QueryRow(ctx,
		`SELECT id::text, backend_kind, backend_ref FROM nodes WHERE name='file.txt' AND deleted_at IS NULL`,
	).Scan(&beforeID, &beforeKind, &beforeRef); err != nil {
		t.Fatal(err)
	}

	if err := f.fs.Rename(ctx, "/src/file.txt", "/dst/renamed.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	var (
		afterKind string
		afterRef  string
		afterID   string
	)
	if err := f.pool.QueryRow(ctx,
		`SELECT id::text, backend_kind, backend_ref FROM nodes WHERE name='renamed.txt' AND deleted_at IS NULL`,
	).Scan(&afterID, &afterKind, &afterRef); err != nil {
		t.Fatal(err)
	}

	if afterID != beforeID {
		t.Errorf("node uuid changed during rename: %s → %s", beforeID, afterID)
	}
	if afterKind != beforeKind {
		t.Errorf("backend_kind drifted: %q → %q", beforeKind, afterKind)
	}
	if afterRef != beforeRef {
		t.Errorf("backend_ref drifted: %q → %q (bytes were moved on disk, which is a bug for a logical rename)", beforeRef, afterRef)
	}

	// Bytes still live at the original on-disk location — Rename is a pure
	// metadata operation.
	originalDiskPath := filepath.Join(f.storageDir, filepath.FromSlash(beforeRef))
	if _, err := os.Stat(originalDiskPath); err != nil {
		t.Errorf("content should still be at %s after logical rename: %v", originalDiskPath, err)
	}
}

// TestStatPendingNodeIsHidden documents the behaviour callers should be able
// to rely on: a node that is mid-write (status='pending') must NOT show up
// through the public Stat path. If this test fails, resolveByPath needs to
// add a status filter.
func TestStatPendingNodeIsHidden(t *testing.T) {
	f, ctx := newFS(t)

	w, err := f.fs.OpenWrite(ctx, "/half-written.bin", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = w.Abort()
		_ = w.Close()
	})

	_, err = f.fs.Stat(ctx, "/half-written.bin")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("pending nodes must not leak through Stat, got err=%v", err)
	}
}

// TestStatRemovedNodeNotVisible: after Remove, Stat must return ErrNotFound
// from the live tree, but the entry should be visible via ListTrash.
func TestStatRemovedNodeNotVisible(t *testing.T) {
	f, ctx := newFS(t)
	writeFile(t, f.fs, "/bye.txt", []byte("gone"))
	if err := f.fs.Remove(ctx, "/bye.txt"); err != nil {
		t.Fatal(err)
	}

	if _, err := f.fs.Stat(ctx, "/bye.txt"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("removed node visible via Stat: %v", err)
	}
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("trash entry count: %d (want 1)", len(entries))
	}
}

// TestListLargeDirectoryIsStablyOrdered: with many siblings, two consecutive
// List calls must return entries in the same lexicographic order. Guards
// against regressions in the SQL ORDER BY clause.
func TestListLargeDirectoryIsStablyOrdered(t *testing.T) {
	const N = 256
	f, ctx := newFS(t)
	if err := f.fs.Mkdir(ctx, "/many", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	// Write in shuffled order to make sure insertion order doesn't accidentally
	// stand in for sort order. Names are zero-padded so lex == numeric.
	indices := make([]int, N)
	for i := 0; i < N; i++ {
		indices[i] = i
	}
	// Fisher-Yates against a deterministic seed-free permutation: just swap
	// every other entry to get non-trivial reordering.
	for i := 0; i+1 < N; i += 2 {
		indices[i], indices[i+1] = indices[i+1], indices[i]
	}
	for _, i := range indices {
		writeFile(t, f.fs, fmt.Sprintf("/many/f-%04d.bin", i), []byte{byte(i)})
	}

	first, err := f.fs.List(ctx, "/many")
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.fs.List(ctx, "/many")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != N {
		t.Fatalf("count: %d (want %d)", len(first), N)
	}
	if len(second) != len(first) {
		t.Fatalf("second-call count differs: %d vs %d", len(second), len(first))
	}
	for i := range first {
		if first[i].Name != second[i].Name {
			t.Fatalf("non-stable order at %d: %q vs %q", i, first[i].Name, second[i].Name)
		}
	}

	// And confirm it is lexicographic.
	want := make([]string, N)
	for i := range first {
		want[i] = first[i].Name
	}
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)
	for i := range want {
		if want[i] != sorted[i] {
			t.Fatalf("not lexicographic at %d: got %q want %q", i, want[i], sorted[i])
		}
	}
	_ = context.Background()
}
