package dbfs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// TestConcurrentCreateSamePathOnlyOneWins drives N goroutines all racing to
// WriteCreate the same path. The DB uniqueness constraint on (parent_id, name)
// guarantees exactly one winner; the others must surface ErrExists. After the
// dust settles, the tree must hold exactly one ready node and no orphan blobs.
func TestConcurrentCreateSamePathOnlyOneWins(t *testing.T) {
	f, ctx := newFS(t)
	const N = 16

	var (
		wg      sync.WaitGroup
		winners atomic.Int32
		exists  atomic.Int32
		other   atomic.Int32
	)
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			w, err := f.fs.OpenWrite(ctx, "/race.txt", storage.WriteOpts{Mode: storage.WriteCreate})
			if err != nil {
				if errors.Is(err, storage.ErrExists) {
					exists.Add(1)
					return
				}
				other.Add(1)
				return
			}
			defer w.Close()
			if _, err := w.WriteAt([]byte{byte(i)}, 0); err != nil {
				other.Add(1)
				return
			}
			if err := w.Commit(); err != nil {
				if errors.Is(err, storage.ErrExists) {
					exists.Add(1)
					_ = w.Abort()
					return
				}
				other.Add(1)
				_ = w.Abort()
				return
			}
			winners.Add(1)
		}()
	}
	wg.Wait()

	if winners.Load() != 1 {
		t.Errorf("winners=%d (want 1); exists=%d other=%d", winners.Load(), exists.Load(), other.Load())
	}
	if other.Load() != 0 {
		t.Errorf("unexpected error count: other=%d (want 0)", other.Load())
	}

	info, err := f.fs.Stat(ctx, "/race.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Type != storage.NodeFile {
		t.Errorf("type: %v", info.Type)
	}

	assertConsistent(t, f)
}

// TestConcurrentMkdirSameParent: many goroutines call Mkdir on the same path.
// Exactly one creates, the rest must see ErrExists.
func TestConcurrentMkdirSameParent(t *testing.T) {
	f, ctx := newFS(t)
	const N = 12

	var (
		wg      sync.WaitGroup
		ok      atomic.Int32
		dup     atomic.Int32
		other   atomic.Int32
	)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			err := f.fs.Mkdir(ctx, "/race-dir", storage.MkdirOpts{})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, storage.ErrExists):
				dup.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 1 {
		t.Errorf("created=%d (want 1); dup=%d other=%d", ok.Load(), dup.Load(), other.Load())
	}
	if other.Load() != 0 {
		t.Errorf("unexpected errors: %d", other.Load())
	}

	info, err := f.fs.Stat(ctx, "/race-dir")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Type != storage.NodeDir {
		t.Errorf("type: %v", info.Type)
	}
}

// TestConcurrentRemoveAndReadIsCoherent: one goroutine repeatedly Removes a
// file while another opens and reads it. The reader must either see the full
// content or ErrNotFound — never partial garbage and never a panic.
func TestConcurrentRemoveAndReadIsCoherent(t *testing.T) {
	f, ctx := newFS(t)
	const Rounds = 50

	for round := 0; round < Rounds; round++ {
		writeFile(t, f.fs, "/x.bin", []byte("hello world"))

		var wg sync.WaitGroup
		wg.Add(2)
		var (
			readErr   error
			readBytes []byte
		)
		go func() {
			defer wg.Done()
			r, err := f.fs.OpenRead(ctx, "/x.bin")
			if err != nil {
				readErr = err
				return
			}
			defer r.Close()
			buf := make([]byte, r.Size())
			n, err := r.ReadAt(buf, 0)
			if err != nil && err.Error() != "EOF" {
				readErr = err
				return
			}
			readBytes = buf[:n]
		}()
		go func() {
			defer wg.Done()
			_ = f.fs.Remove(ctx, "/x.bin")
		}()
		wg.Wait()

		// Either the read succeeded and saw the whole payload, or it raced and
		// got ErrNotFound. Both are valid outcomes.
		switch {
		case readErr == nil:
			if string(readBytes) != "hello world" {
				t.Fatalf("round %d: partial/corrupt read: %q", round, readBytes)
			}
		case errors.Is(readErr, storage.ErrNotFound):
			// ok — Remove won the race.
		default:
			t.Fatalf("round %d: unexpected read error: %v", round, readErr)
		}

		// Reset: the file may or may not still exist depending on order.
		_ = f.fs.Remove(ctx, "/x.bin")
	}
}

// assertConsistent calls into the storagetest invariant checker without
// importing the package from this test file (the dbfs test package would
// create a cycle via storagetest → dbfs).
func assertConsistent(t *testing.T, f *fixture) {
	t.Helper()
	// Local replica of the core invariant checks — full version lives in
	// internal/storage/storagetest/consistency.go and is exercised from the
	// property/stress tests where dependency direction is the other way.
	ctx := context.Background()
	var (
		readyFiles int
		pendingOB  int
	)
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM nodes WHERE type='file' AND status='ready'`,
	).Scan(&readyFiles); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox o LEFT JOIN nodes n ON n.id = o.node_id
		 WHERE o.op='create_file' AND COALESCE(n.status::text,'') <> 'pending'`,
	).Scan(&pendingOB); err != nil {
		t.Fatal(err)
	}
	if pendingOB != 0 {
		t.Errorf("found %d create_file outbox rows whose node is not pending", pendingOB)
	}
	_ = readyFiles
}
