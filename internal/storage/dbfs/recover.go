package dbfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// RecoverPending finalizes outbox rows left behind by a crashed process or
// abandoned writer. Runs at startup, before serving traffic. Returns the
// number of rows acted on.
//
// Currently handles op='create_file': consults the backend to see whether the
// staged content made it to the target path. If yes — mark the node ready and
// archive the row as done. If no — delete the pending node and archive as
// failed. Other op codes are left in place for follow-up slices.
func (fs *DBFS) RecoverPending(ctx context.Context) (int, error) {
	rows, err := fs.pool.Query(ctx,
		`SELECT id, op, payload FROM outbox WHERE status IN ('pending','in_progress')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type entry struct {
		ID      int64
		Op      string
		Payload []byte
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.Op, &e.Payload); err != nil {
			return 0, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	acted := 0
	for _, e := range entries {
		switch e.Op {
		case opCreateFile:
			if err := fs.recoverCreateFile(ctx, e.ID, e.Payload); err != nil {
				return acted, fmt.Errorf("recover outbox %d (%s): %w", e.ID, e.Op, err)
			}
			acted++
		case opTrash:
			if err := fs.runTrashOutbox(ctx, e.ID, e.Payload); err != nil {
				return acted, fmt.Errorf("recover outbox %d (%s): %w", e.ID, e.Op, err)
			}
			acted++
		default:
			// Unknown op — leave it for a future executor; don't archive.
		}
	}
	return acted, nil
}

func (fs *DBFS) recoverCreateFile(ctx context.Context, outboxID int64, raw []byte) error {
	var p createFilePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	backend, ok := fs.backends[p.BackendKind]
	if !ok {
		return fmt.Errorf("backend %q not registered", p.BackendKind)
	}
	ref := storage.BackendRef{Kind: p.BackendKind, Data: p.BackendRef}

	st, statErr := backend.Stat(ctx, ref)
	if statErr != nil && !errors.Is(statErr, storage.ErrNotFound) {
		return fmt.Errorf("backend stat: %w", statErr)
	}

	if errors.Is(statErr, storage.ErrNotFound) {
		// File never landed. Drop the pending node + archive as failed.
		reason := "content missing after crash"
		return fs.rollbackPending(ctx, p.NodeID, outboxID, reason)
	}

	// File is on disk. Promote the node to ready, enqueue indexing, archive.
	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE nodes
			 SET status='ready', size=$1, mtime=now(), updated_at=now()
			 WHERE id=$2 AND status='pending'`,
			st.Size, p.NodeID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO jobs (node_id, kind, status) VALUES ($1, 'hash', 'pending')`,
			p.NodeID); err != nil {
			return err
		}
		return archiveOutbox(ctx, tx, outboxID, "done", nil)
	})
}
