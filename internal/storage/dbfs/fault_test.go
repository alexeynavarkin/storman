package dbfs_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/storagetest"
)

// errInjected is the sentinel hooks return to simulate a crash. The actual
// process doesn't die; the hook returns an error, the operation propagates
// it, and we then exercise the recovery path by calling RecoverPending.
var errInjected = errors.New("test: injected fault")

func injectOnce(point dbfs.HookPoint) dbfs.HookFn {
	var fired bool
	return func(p dbfs.HookPoint) error {
		if p == point && !fired {
			fired = true
			return errInjected
		}
		return nil
	}
}

// TestFaultAfterCreateTx: simulate a crash right after tx1 commits, before
// the backend writer can open. The pending node is left in the DB with no
// on-disk content. RecoverPending must drop it.
func TestFaultAfterCreateTx(t *testing.T) {
	f, ctx := newFS(t)
	layout := layoutOf(f)

	f.fs.SetTestHook(injectOnce(dbfs.HookAfterCreateTx))
	_, err := f.fs.OpenWrite(ctx, "/ghost.bin", storage.WriteOpts{Mode: storage.WriteCreate})
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	f.fs.SetTestHook(nil)

	// OpenWrite's rollbackPending should have already cleaned up; recovery
	// is the safety net for the case where the rollback itself crashed.
	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	_ = acted // may be 0 if rollback already archived the row.

	if _, err := f.fs.Stat(ctx, "/ghost.bin"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("pending node leaked after recovery: %v", err)
	}
	storagetest.AssertConsistent(t, f.pool, layout)
}

// TestFaultAfterBackendCommit: the bytes are durable on disk; tx2 never ran.
// RecoverPending must promote the node to 'ready' and archive the outbox.
func TestFaultAfterBackendCommit(t *testing.T) {
	f, ctx := newFS(t)
	layout := layoutOf(f)

	f.fs.SetTestHook(injectOnce(dbfs.HookAfterBackendCommit))

	w, err := f.fs.OpenWrite(ctx, "/halfway.bin", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("halfway"), 0); err != nil {
		t.Fatal(err)
	}
	commitErr := w.Commit()
	if !errors.Is(commitErr, errInjected) {
		t.Fatalf("expected injected error from Commit, got %v", commitErr)
	}
	// A real process crash does NOT execute Close — Close would invoke Abort
	// which rolls the pending node back, defeating the purpose of this test.
	// We intentionally leak the writer to mimic a hard crash; RecoverPending
	// is the only thing allowed to drive the state forward.
	f.fs.SetTestHook(nil)

	// Before recovery: node must still be pending and bytes must already be
	// at the target path (no orphan blob accusation — bytes belong to the
	// pending node that the consistency check intentionally ignores).
	var status string
	if err := f.pool.QueryRow(ctx,
		`SELECT status::text FROM nodes WHERE name='halfway.bin'`,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Errorf("status: %s (want pending)", status)
	}
	if _, err := os.Stat(filepath.Join(f.storageDir, "halfway.bin")); err != nil {
		t.Errorf("bytes should be at target: %v", err)
	}

	// Recover.
	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 1 {
		t.Errorf("acted=%d (want 1)", acted)
	}

	// After recovery: node ready, bytes intact, public Stat works.
	info, err := f.fs.Stat(ctx, "/halfway.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len("halfway")) {
		t.Errorf("size: %d", info.Size)
	}
	storagetest.AssertConsistent(t, f.pool, layout)
}

// TestFaultBeforeTrashRename: Remove's tx committed; we crash before the
// physical move to trash. RecoverPending must finish the move and write
// the meta sidecar.
func TestFaultBeforeTrashRename(t *testing.T) {
	f, ctx := newFS(t)
	layout := layoutOf(f)

	writeFile(t, f.fs, "/doomed.txt", []byte("payload"))

	f.fs.SetTestHook(injectOnce(dbfs.HookBeforeTrashRename))

	err := f.fs.Remove(ctx, "/doomed.txt")
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected injected error from Remove, got %v", err)
	}
	f.fs.SetTestHook(nil)

	// After tx commit + before rename: node is deleted in DB, bytes still at
	// the live path. Trash dir does not exist yet.
	var deleted int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM nodes WHERE name='doomed.txt' AND deleted_at IS NOT NULL`,
	).Scan(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 soft-deleted node, got %d", deleted)
	}
	if _, err := os.Stat(filepath.Join(f.storageDir, "doomed.txt")); err != nil {
		t.Errorf("live bytes should still be present pre-recovery: %v", err)
	}

	// Recover finishes the trash move.
	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 1 {
		t.Errorf("acted=%d (want 1)", acted)
	}

	// Bytes moved to trash, live path empty.
	if _, err := os.Stat(filepath.Join(f.storageDir, "doomed.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("live bytes should be gone after recovery: %v", err)
	}
	entries, err := f.fs.ListTrash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("trash entries: %d (want 1)", len(entries))
	}
	storagetest.AssertConsistent(t, f.pool, layout)
}

// TestFaultMultipleCascade: simulate a process that crashed during a Write,
// restarted, then crashed during the Remove of an unrelated file. Two
// pending outbox rows of different ops. RecoverPending must clear both.
func TestFaultMultipleCascade(t *testing.T) {
	f, ctx := newFS(t)
	layout := layoutOf(f)

	// Stage 1: crash mid-write.
	f.fs.SetTestHook(injectOnce(dbfs.HookAfterBackendCommit))
	w, err := f.fs.OpenWrite(ctx, "/a.bin", storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("a"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); !errors.Is(err, errInjected) {
		t.Fatalf("stage1 commit: %v", err)
	}
	// Leak the writer — simulating crash; see TestFaultAfterBackendCommit.
	f.fs.SetTestHook(nil)

	// Independent successful write so something exists to trash.
	writeFile(t, f.fs, "/b.bin", []byte("b"))

	// Stage 2: crash mid-Remove of b.
	f.fs.SetTestHook(injectOnce(dbfs.HookBeforeTrashRename))
	if err := f.fs.Remove(ctx, "/b.bin"); !errors.Is(err, errInjected) {
		t.Fatalf("stage2 remove: %v", err)
	}
	f.fs.SetTestHook(nil)

	// Both pending outbox rows should now be present.
	var pending int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE status='pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 2 {
		t.Fatalf("expected 2 pending outbox rows, got %d", pending)
	}

	acted, err := f.fs.RecoverPending(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if acted != 2 {
		t.Errorf("acted=%d (want 2)", acted)
	}

	// a.bin promoted to ready, b.bin in trash.
	if info, err := f.fs.Stat(ctx, "/a.bin"); err != nil {
		t.Errorf("a.bin Stat: %v", err)
	} else if info.Size != 1 {
		t.Errorf("a.bin size: %d", info.Size)
	}
	if _, err := f.fs.Stat(ctx, "/b.bin"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("b.bin should be gone: %v", err)
	}

	// Idempotency: second recover finds nothing.
	if again, err := f.fs.RecoverPending(ctx); err != nil || again != 0 {
		t.Errorf("idempotency: acted=%d err=%v", again, err)
	}
	storagetest.AssertConsistent(t, f.pool, layout)
}

func layoutOf(f *fixture) storagetest.Layout {
	return storagetest.Layout{
		StorageDir: f.storageDir,
		UploadsDir: f.uploadsDir,
		TrashDir:   f.trashDir,
	}
}

// ensure unused-import suppression doesn't bite if I reshuffle.
var _ = fmt.Sprintf
