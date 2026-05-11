package flat

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/alexnav/storman/internal/storage"
)

func newTestBackend(t *testing.T) (*Backend, string, string) {
	t.Helper()
	root := t.TempDir()
	storageDir := filepath.Join(root, "flat-storage")
	uploadsDir := filepath.Join(root, "meta-storage", "uploads")
	for _, d := range []string{storageDir, uploadsDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return New(storageDir, uploadsDir), storageDir, uploadsDir
}

func TestAllocateValidatesPath(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()

	good := []string{"a.txt", "photos/2024/01.jpg", "deep/path/x.bin"}
	for _, p := range good {
		ref, err := b.Allocate(ctx, storage.AllocHint{NodeID: uuid.New(), LogicalPath: p})
		if err != nil {
			t.Errorf("allocate %q: %v", p, err)
			continue
		}
		if ref.Kind != BackendName {
			t.Errorf("kind: got %q", ref.Kind)
		}
	}

	bad := []string{"", "..", "../escape", "a/../../b", "/abs", "./x", "a//b"}
	for _, p := range bad {
		_, err := b.Allocate(ctx, storage.AllocHint{NodeID: uuid.New(), LogicalPath: p})
		if !errors.Is(err, storage.ErrInvalidPath) {
			t.Errorf("allocate %q: expected ErrInvalidPath, got %v", p, err)
		}
	}
}

func TestWriteCommitRoundtrip(t *testing.T) {
	b, storageDir, uploadsDir := newTestBackend(t)
	ctx := context.Background()

	ref, err := b.Allocate(ctx, storage.AllocHint{LogicalPath: "docs/hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello, storman")
	if _, err := w.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close after commit: %v", err)
	}

	target := filepath.Join(storageDir, "docs", "hello.txt")
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("content mismatch: %q vs %q", got, data)
	}

	// Staging should have been cleaned.
	entries, _ := os.ReadDir(uploadsDir)
	if len(entries) != 0 {
		t.Errorf("staging not cleaned: %v", entries)
	}

	r, err := b.OpenRead(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Size() != int64(len(data)) {
		t.Errorf("size: %d vs %d", r.Size(), len(data))
	}
	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 7) // "storman"[0..5] == "storm"
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "storm" {
		t.Errorf("ReadAt: %q", buf[:n])
	}
}

func TestCreateRejectsExisting(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})

	mustCommit(t, b, ref, storage.WriteCreate, []byte("first"))

	w, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("second"), 0); err != nil {
		t.Fatal(err)
	}
	err = w.Commit()
	if !errors.Is(err, storage.ErrExists) {
		t.Fatalf("expected ErrExists, got %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
}

func TestOverwriteReplaces(t *testing.T) {
	b, storageDir, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})
	mustCommit(t, b, ref, storage.WriteCreate, []byte("first"))
	mustCommit(t, b, ref, storage.WriteOverwrite, []byte("second"))

	got, err := os.ReadFile(filepath.Join(storageDir, "x"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Errorf("content: %q", got)
	}
}

func TestModifyPreservesExisting(t *testing.T) {
	b, storageDir, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})

	mustCommit(t, b, ref, storage.WriteCreate, []byte("AAAAAAAAAA"))

	w, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteModify})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("ZZ"), 3); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	got, _ := os.ReadFile(filepath.Join(storageDir, "x"))
	if string(got) != "AAAZZAAAAA" {
		t.Errorf("modify result: %q", got)
	}
}

func TestModifyMissingTargetIsNotFound(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "missing"})

	_, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteModify})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestAbortCleansStaging(t *testing.T) {
	b, storageDir, uploadsDir := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})

	w, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close after abort: %v", err)
	}

	if _, err := os.Stat(filepath.Join(storageDir, "x")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("target was published despite Abort: %v", err)
	}
	entries, _ := os.ReadDir(uploadsDir)
	if len(entries) != 0 {
		t.Errorf("staging not cleaned: %v", entries)
	}
}

func TestCloseWithoutFinalizeReturnsErrUnfinalized(t *testing.T) {
	b, _, uploadsDir := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})

	w, err := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("dangling"), 0); err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if !errors.Is(err, storage.ErrUnfinalized) {
		t.Fatalf("expected ErrUnfinalized, got %v", err)
	}
	// Staging must still be cleaned to avoid leaks.
	entries, _ := os.ReadDir(uploadsDir)
	if len(entries) != 0 {
		t.Errorf("staging leaked on unfinalized Close: %v", entries)
	}
}

func TestTruncate(t *testing.T) {
	b, storageDir, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})

	w, _ := b.OpenWrite(ctx, ref, storage.WriteOpts{Mode: storage.WriteCreate})
	if _, err := w.WriteAt([]byte("0123456789"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	got, _ := os.ReadFile(filepath.Join(storageDir, "x"))
	if string(got) != "0123" {
		t.Errorf("truncate result: %q", got)
	}
}

func TestDeleteAndStat(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()
	ref, _ := b.Allocate(ctx, storage.AllocHint{LogicalPath: "x"})
	mustCommit(t, b, ref, storage.WriteCreate, []byte("data"))

	st, err := b.Stat(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 4 {
		t.Errorf("stat size: %d", st.Size)
	}

	if err := b.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat(ctx, ref); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Delete, got %v", err)
	}
	if err := b.Delete(ctx, ref); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on double Delete, got %v", err)
	}
}

func TestReadMissingIsNotFound(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()
	_, err := b.OpenRead(ctx, storage.BackendRef{Kind: BackendName, Data: "missing"})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestWrongBackendKindRejected(t *testing.T) {
	b, _, _ := newTestBackend(t)
	ctx := context.Background()
	_, err := b.OpenRead(ctx, storage.BackendRef{Kind: "cdc", Data: "x"})
	if !errors.Is(err, storage.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func mustCommit(t *testing.T, b *Backend, ref storage.BackendRef, mode storage.WriteMode, data []byte) {
	t.Helper()
	w, err := b.OpenWrite(context.Background(), ref, storage.WriteOpts{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
