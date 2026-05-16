package dbfs_test

import (
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/storagetest"
)

// stressDuration controls how long TestStress hammers the FS. Gated so the
// regular `go test` run skips this — opt in with `-stress=5m`.
var stressDuration = flag.Duration("stress", 0, "if >0, run concurrency stress tests for this duration")

// TestStressConcurrentSubtreeOps spins N goroutines, each owning a disjoint
// subtree under /stress/<id>/. They perform random Mkdir/Write/Read/Remove
// ops within their own subtree continuously for the configured duration.
// After all workers stop, RecoverPending drains anything left in flight, and
// AssertConsistent must pass strictly.
//
// Why no periodic mid-flight checker: the consistency invariants (no orphan
// blobs, all trash dirs have meta) are reached transiently mid-Remove or
// mid-Write — the checker would race and flag those benign in-progress
// states. Stress's job is to ensure the END state is correct under load.
//
// Run with: `go test -race -run=TestStress ./internal/storage/dbfs/... -stress=2m`
func TestStressConcurrentSubtreeOps(t *testing.T) {
	if *stressDuration <= 0 {
		t.Skip("set -stress=DURATION to run; default is skip")
	}
	f, ctx := newFS(t)
	layout := storagetest.Layout{
		StorageDir: f.storageDir,
		UploadsDir: f.uploadsDir,
		TrashDir:   f.trashDir,
	}

	if err := f.fs.Mkdir(ctx, "/stress", storage.MkdirOpts{}); err != nil {
		t.Fatal(err)
	}

	workers := runtime.GOMAXPROCS(0) * 4
	t.Logf("stress: workers=%d duration=%s", workers, *stressDuration)

	deadline := time.Now().Add(*stressDuration)

	var totalOps atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			root := fmt.Sprintf("/stress/w%d", w)
			if err := f.fs.Mkdir(ctx, root, storage.MkdirOpts{}); err != nil && !errors.Is(err, storage.ErrExists) {
				t.Errorf("worker %d mkdir root: %v", w, err)
				return
			}
			r := rand.New(rand.NewPCG(uint64(w)+1, uint64(w)*7919+1))
			for {
				if time.Now().After(deadline) {
					return
				}
				op := r.IntN(4)
				idx := r.IntN(5) // small index pool keeps the subtree dense and conflict-y
				p := fmt.Sprintf("%s/f%d.bin", root, idx)

				switch op {
				case 0:
					_ = f.fs.Mkdir(ctx, fmt.Sprintf("%s/d%d", root, idx), storage.MkdirOpts{})
				case 1:
					_ = propWriteFile(f.fs, p, []byte(fmt.Sprintf("w%d-%d", w, idx)))
				case 2:
					if rd, err := f.fs.OpenRead(ctx, p); err == nil {
						buf := make([]byte, rd.Size())
						_, _ = rd.ReadAt(buf, 0)
						rd.Close()
					}
				case 3:
					_ = f.fs.Remove(ctx, p)
				}
				totalOps.Add(1)
			}
		}()
	}
	wg.Wait()

	// Drain any work still parked in the outbox. RecoverPending is idempotent
	// so re-running until it reports 0 is the standard way to quiesce.
	for i := 0; i < 8; i++ {
		acted, err := f.fs.RecoverPending(ctx)
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		if acted == 0 {
			break
		}
	}

	storagetest.AssertConsistent(t, f.pool, layout)
	t.Logf("stress complete: %d ops across %d workers", totalOps.Load(), workers)
}
