// Package jobs runs the async indexing pipeline (docs/arch/indexing.md). Jobs
// are picked from the jobs table with SELECT ... FOR UPDATE SKIP LOCKED, run
// in-process by per-kind executors, and archived to jobs_history on
// success or terminal failure.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job kinds. Add a new constant + executor when introducing a worker.
const (
	KindHash = "hash"
)

// Status values for the live jobs table. Terminal results ('done'/'failed')
// only ever exist in jobs_history — the live table is cleared on Finish.
const (
	StatusPending    = "pending"
	StatusInProgress = "in_progress"
	StatusFailed     = "failed"
	FinalDone        = "done"
	FinalFailed      = "failed"
)

// MaxAttempts is the cap before a job is archived as final_status='failed'.
// 5 retries with linear backoff (handled by the pool) is enough breathing room
// for transient disk hiccups without burning CPU on permanently broken refs.
const MaxAttempts = 5

// LeaseDuration is how long a worker holds an in-progress job before the
// sweeper considers it abandoned and another worker may pick it back up.
const LeaseDuration = 5 * time.Minute

// Job is the in-memory projection of a row from jobs.
type Job struct {
	ID         int64
	NodeID     uuid.UUID
	Kind       string
	Attempts   int
	EnqueuedAt time.Time
}

// Service is the only entry point for enqueueing and leasing jobs. The pool
// lives here; executors don't talk to the table directly.
type Service struct {
	pool *pgxpool.Pool
}

// NewService wires the service against a connection pool.
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// Enqueue inserts a pending job. Pass the live transaction so the job is
// committed atomically with the operation that triggered it; pass nil to use
// the service's own pool.
func (s *Service) Enqueue(ctx context.Context, tx pgx.Tx, nodeID uuid.UUID, kind string) error {
	var err error
	if tx != nil {
		_, err = tx.Exec(ctx,
			`INSERT INTO jobs (node_id, kind, status) VALUES ($1, $2, 'pending')`,
			nodeID, kind)
	} else {
		_, err = s.pool.Exec(ctx,
			`INSERT INTO jobs (node_id, kind, status) VALUES ($1, $2, 'pending')`,
			nodeID, kind)
	}
	if err != nil {
		return fmt.Errorf("enqueue %s job: %w", kind, err)
	}
	return nil
}

// Lease atomically picks the oldest available pending job of the requested
// kind. Returns ErrNoJob when the queue is empty (callers sleep and retry).
//
// Concurrent callers see different jobs thanks to FOR UPDATE SKIP LOCKED —
// the row update happens inside the row-lock window so the next Lease never
// sees the same job until the lease expires.
func (s *Service) Lease(ctx context.Context, kind string) (*Job, error) {
	var j Job
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx,
			`SELECT id FROM jobs
			 WHERE kind = $1
			   AND status = 'pending'
			   AND (locked_until IS NULL OR locked_until < now())
			 ORDER BY created_at
			 FOR UPDATE SKIP LOCKED
			 LIMIT 1`, kind).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoJob
		}
		if err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`UPDATE jobs
			 SET status = 'in_progress',
			     attempts = attempts + 1,
			     locked_until = now() + $2::interval,
			     updated_at = now()
			 WHERE id = $1
			 RETURNING id, node_id, kind, attempts, created_at`,
			id, fmt.Sprintf("%d seconds", int(LeaseDuration.Seconds())))
		return row.Scan(&j.ID, &j.NodeID, &j.Kind, &j.Attempts, &j.EnqueuedAt)
	})
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// Finish archives a finished job and removes it from the live table. Pass
// final=FinalDone on success, FinalFailed for a terminal error.
func (s *Service) Finish(ctx context.Context, job *Job, final string, lastErr error) error {
	var msg *string
	if lastErr != nil {
		m := lastErr.Error()
		msg = &m
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO jobs_history (id, node_id, kind, final_status, attempts, last_error, enqueued_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			job.ID, job.NodeID, job.Kind, final, job.Attempts, msg, job.EnqueuedAt)
		if err != nil {
			return fmt.Errorf("archive job: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, job.ID); err != nil {
			return fmt.Errorf("delete job: %w", err)
		}
		return nil
	})
}

// Release returns a job to the pending pool after a transient failure. The
// pool calls this when attempts are still below MaxAttempts; otherwise it
// calls Finish(FinalFailed).
func (s *Service) Release(ctx context.Context, job *Job, lastErr error) error {
	msg := lastErr.Error()
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = 'pending',
		     last_error = $2,
		     locked_until = NULL,
		     updated_at = now()
		 WHERE id = $1`,
		job.ID, msg)
	return err
}

// ErrNoJob signals an empty queue. Callers turn this into a sleep.
var ErrNoJob = errors.New("jobs: no work available")

