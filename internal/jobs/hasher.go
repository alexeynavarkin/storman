package jobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/storage"
)

// Hasher computes sha256 for a node's content and stores it in nodes.sha256.
// Used by the 'hash' kind worker.
type Hasher struct {
	pool     *pgxpool.Pool
	backends map[string]storage.FileBackend
}

// NewHasher wires the executor against the FS-level resources it needs.
// backends is the same registry dbfs uses — keyed by FileBackend.Name().
func NewHasher(pool *pgxpool.Pool, backends map[string]storage.FileBackend) *Hasher {
	return &Hasher{pool: pool, backends: backends}
}

// Run is invoked by the pool for one job. Successful runs persist sha256 +
// upsert the matching node_meta row (mirror — see docs/arch/indexing.md).
func (h *Hasher) Run(ctx context.Context, job *Job) error {
	kind, ref, status, err := h.loadNode(ctx, job.NodeID)
	if err != nil {
		return err
	}
	if status != "ready" {
		// Skip silently: the node was either deleted or never finalised. The
		// pool will Finish(FinalDone) so the entry stops looping.
		return nil
	}
	backend, ok := h.backends[kind]
	if !ok {
		return fmt.Errorf("backend %q not registered", kind)
	}
	r, err := backend.OpenRead(ctx, storage.BackendRef{Kind: kind, Data: ref})
	if err != nil {
		return fmt.Errorf("open for hashing: %w", err)
	}
	defer r.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, io.NewSectionReader(r, 0, r.Size())); err != nil {
		return fmt.Errorf("read for hashing: %w", err)
	}
	sum := hasher.Sum(nil)

	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE nodes SET sha256 = $1, updated_at = now() WHERE id = $2`,
			sum, job.NodeID); err != nil {
			return fmt.Errorf("store sha256: %w", err)
		}
		// node_meta is 1:1 with nodes; create the row if it doesn't exist
		// yet so future EXIF / AI workers can layer on top.
		if _, err := tx.Exec(ctx,
			`INSERT INTO node_meta (node_id) VALUES ($1)
			 ON CONFLICT (node_id) DO NOTHING`, job.NodeID); err != nil {
			return fmt.Errorf("touch node_meta: %w", err)
		}
		return nil
	})
}

// loadNode reads backend_kind / backend_ref / status for the given file node.
// Returns ErrNodeGone if the row was deleted (FK cascades may have removed
// the job entirely already; but a race window exists).
func (h *Hasher) loadNode(ctx context.Context, nodeID uuid.UUID) (kind, ref, status string, err error) {
	var kindPtr, refPtr *string
	err = h.pool.QueryRow(ctx,
		`SELECT backend_kind, backend_ref, status FROM nodes WHERE id = $1`,
		nodeID).Scan(&kindPtr, &refPtr, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", ErrNodeGone
	}
	if err != nil {
		return "", "", "", err
	}
	if kindPtr != nil {
		kind = *kindPtr
	}
	if refPtr != nil {
		ref = *refPtr
	}
	return kind, ref, status, nil
}

// ErrNodeGone says the node disappeared between enqueue and execution.
// Treated as success — the file is gone, no work to do.
var ErrNodeGone = errors.New("jobs: node no longer exists")
