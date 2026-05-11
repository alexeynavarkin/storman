package jobs_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/jobs"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
)

type fixture struct {
	pool     *pgxpool.Pool
	fs       *dbfs.DBFS
	flat     *flat.Backend
	jobs     *jobs.Service
	hasher   *jobs.Hasher
	dataDir  string
}

func newFixture(t *testing.T) (*fixture, context.Context) {
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
	svc := jobs.NewService(pool)
	hasher := jobs.NewHasher(pool, map[string]storage.FileBackend{flatBackend.Name(): flatBackend})
	return &fixture{pool: pool, fs: fs, flat: flatBackend, jobs: svc, hasher: hasher, dataDir: root}, ctx
}

func writeFile(t *testing.T, fs *dbfs.DBFS, path string, data []byte) {
	t.Helper()
	w, err := fs.OpenWrite(context.Background(), path, storage.WriteOpts{Mode: storage.WriteCreate})
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

func TestUploadEnqueuesHashJob(t *testing.T) {
	f, ctx := newFixture(t)
	writeFile(t, f.fs, "/hello.txt", []byte("hi"))

	// Lease should return the job that dbfs enqueued on Commit.
	job, err := f.jobs.Lease(ctx, jobs.KindHash)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if job.Kind != jobs.KindHash {
		t.Errorf("kind: %q", job.Kind)
	}
}

func TestHasherWritesSHA256(t *testing.T) {
	f, ctx := newFixture(t)
	payload := []byte("hash this please")
	writeFile(t, f.fs, "/h.txt", payload)

	job, err := f.jobs.Lease(ctx, jobs.KindHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hasher.Run(ctx, job); err != nil {
		t.Fatalf("hasher run: %v", err)
	}
	if err := f.jobs.Finish(ctx, job, jobs.FinalDone, nil); err != nil {
		t.Fatal(err)
	}

	info, err := f.fs.Stat(ctx, "/h.txt")
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(payload)
	if string(info.SHA256) != string(want[:]) {
		t.Errorf("sha mismatch: got=%x want=%x", info.SHA256, want)
	}

	// The live queue should be drained after Finish.
	if _, err := f.jobs.Lease(ctx, jobs.KindHash); !errors.Is(err, jobs.ErrNoJob) {
		t.Errorf("queue not drained: %v", err)
	}
}

func TestLeaseSkipsLockedConcurrently(t *testing.T) {
	f, ctx := newFixture(t)
	writeFile(t, f.fs, "/a.txt", []byte("a"))
	writeFile(t, f.fs, "/b.txt", []byte("b"))

	type result struct {
		job *jobs.Job
		err error
	}
	out := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			job, err := f.jobs.Lease(ctx, jobs.KindHash)
			out <- result{job, err}
		}()
	}
	r1 := <-out
	r2 := <-out
	if r1.err != nil || r2.err != nil {
		t.Fatalf("errors: %v %v", r1.err, r2.err)
	}
	if r1.job.ID == r2.job.ID {
		t.Errorf("both leases got same job id %d", r1.job.ID)
	}
}

func TestPoolDrainsQueue(t *testing.T) {
	f, ctx := newFixture(t)
	const n = 4
	for i := 0; i < n; i++ {
		writeFile(t, f.fs, fmt.Sprintf("/p%d.txt", i), []byte(fmt.Sprintf("payload-%d", i)))
	}

	poolCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := jobs.NewPool(f.jobs, jobs.KindHash, f.hasher, 2, 50*time.Millisecond, nil)
	wait := p.Start(poolCtx)

	// Wait until the live queue is empty, polling the row count directly so
	// we don't steal a job from the workers.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var remaining int
		if err := waitForEmpty(ctx, f, &remaining); err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wait()

	for i := 0; i < n; i++ {
		info, err := f.fs.Stat(ctx, fmt.Sprintf("/p%d.txt", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(info.SHA256) == 0 {
			t.Errorf("file %d: sha256 not populated", i)
		}
	}
}

// waitForEmpty reads the live jobs row count without consuming any rows.
func waitForEmpty(ctx context.Context, f *fixture, out *int) error {
	return f.pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE kind = $1 AND status = 'pending'`,
		jobs.KindHash).Scan(out)
}

// keep uuid import live if the file shrinks.
var _ = uuid.Nil
