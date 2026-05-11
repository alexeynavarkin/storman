package jobs

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Executor runs a single job. Implementations must be idempotent — the same
// job may be replayed after a crash or lease timeout. Return nil on success.
type Executor interface {
	Run(ctx context.Context, job *Job) error
}

// Pool runs N workers polling the jobs table for a single kind. The current
// MVP uses one pool per kind (Hash); extending to others is a matter of
// instantiating another Pool with the right executor.
type Pool struct {
	svc          *Service
	kind         string
	exec         Executor
	workers      int
	pollInterval time.Duration
	logger       *log.Logger
}

// NewPool wires a worker pool. workers<=0 → 2; pollInterval<=0 → 5s.
func NewPool(svc *Service, kind string, exec Executor, workers int, pollInterval time.Duration, logger *log.Logger) *Pool {
	if workers <= 0 {
		workers = 2
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Pool{
		svc:          svc,
		kind:         kind,
		exec:         exec,
		workers:      workers,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

// Start launches the worker goroutines and returns immediately. Cancelling
// ctx stops every worker and the returned wait function blocks until they
// fully exit.
func (p *Pool) Start(ctx context.Context) (waitFn func()) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.workerLoop(ctx, id)
		}(i)
	}
	return wg.Wait
}

// workerLoop is the per-goroutine body. Poll/work/sleep until ctx is done.
func (p *Pool) workerLoop(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := p.svc.Lease(ctx, p.kind)
		if err != nil {
			if errors.Is(err, ErrNoJob) || errors.Is(err, context.Canceled) {
				if !sleep(ctx, p.pollInterval) {
					return
				}
				continue
			}
			p.logger.Printf("jobs[%s w%d]: lease error: %v", p.kind, id, err)
			if !sleep(ctx, p.pollInterval) {
				return
			}
			continue
		}
		p.runOne(ctx, id, job)
	}
}

// runOne executes one job and applies the finish/release logic.
func (p *Pool) runOne(ctx context.Context, id int, job *Job) {
	start := time.Now()
	err := p.exec.Run(ctx, job)
	if err == nil {
		if finishErr := p.svc.Finish(ctx, job, FinalDone, nil); finishErr != nil {
			p.logger.Printf("jobs[%s w%d]: finish #%d: %v", p.kind, id, job.ID, finishErr)
			return
		}
		p.logger.Printf("jobs[%s w%d]: #%d done node=%s in %s",
			p.kind, id, job.ID, job.NodeID, time.Since(start))
		return
	}
	// Treat "node gone" as terminal success — no work to do.
	if errors.Is(err, ErrNodeGone) {
		_ = p.svc.Finish(ctx, job, FinalDone, nil)
		return
	}
	if job.Attempts >= MaxAttempts {
		p.logger.Printf("jobs[%s w%d]: #%d giving up after %d attempts: %v",
			p.kind, id, job.ID, job.Attempts, err)
		_ = p.svc.Finish(ctx, job, FinalFailed, err)
		return
	}
	p.logger.Printf("jobs[%s w%d]: #%d attempt %d failed: %v (will retry)",
		p.kind, id, job.ID, job.Attempts, err)
	if rerr := p.svc.Release(ctx, job, err); rerr != nil {
		p.logger.Printf("jobs[%s w%d]: release #%d: %v", p.kind, id, job.ID, rerr)
	}
}

// sleep waits for d unless ctx is cancelled. Returns false on cancellation
// so callers can break out of their loops cleanly.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
