package dbfs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Outbox op codes.
const (
	opCreateFile = "create_file"
	opTrash      = "trash"
)

// createFilePayload is the JSON shape stored in outbox.payload for create_file.
type createFilePayload struct {
	NodeID      uuid.UUID `json:"node_id"`
	BackendKind string    `json:"backend_kind"`
	BackendRef  string    `json:"backend_ref"`
}

// trashPayload is the JSON shape stored in outbox.payload for trash.
// Permissions stay on the soft-deleted nodes (no DELETE happens until purge),
// so no acl_snapshot is recorded here — restore re-uses live ACL rows.
type trashPayload struct {
	TrashUUID       uuid.UUID `json:"trash_uuid"`
	RootNodeID      uuid.UUID `json:"root_node_id"`
	RootLogicalPath string    `json:"root_logical_path"`
	RootNodeType    string    `json:"root_node_type"`  // "file" | "dir"
	RootBackendKind string    `json:"root_backend_kind"`
	RootBackendRef  string    `json:"root_backend_ref"` // for dirs: same relative path as logical, minus leading slash
	DeletedBy       uuid.UUID `json:"deleted_by"`
	DeletedAt       time.Time `json:"deleted_at"`
}

func insertOutbox(ctx context.Context, tx pgx.Tx, op string, nodeID *uuid.UUID, payload any) (int64, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal outbox payload: %w", err)
	}
	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO outbox (op, node_id, payload, status)
		 VALUES ($1, $2, $3::jsonb, 'pending')
		 RETURNING id`,
		op, nodeID, data,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert outbox: %w", err)
	}
	return id, nil
}

// archiveOutbox moves an outbox row to outbox_history with the given terminal
// status. Both operations happen in the supplied transaction.
func archiveOutbox(ctx context.Context, tx pgx.Tx, id int64, finalStatus string, lastErr *string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_history
		     (id, op, node_id, payload, final_status, attempts, last_error, enqueued_at, finished_at)
		 SELECT id, op, node_id, payload, $2, attempts, $3, created_at, now()
		 FROM outbox WHERE id = $1`,
		id, finalStatus, lastErr)
	if err != nil {
		return fmt.Errorf("archive outbox: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete outbox: %w", err)
	}
	return nil
}

// rollbackPending removes a pending node and archives its outbox row as failed.
// Used when OpenWrite cannot continue past the initial transaction (e.g., the
// backend rejects the writer open after the DB row is already committed).
func (fs *DBFS) rollbackPending(ctx context.Context, nodeID uuid.UUID, outboxID int64, reason string) error {
	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM nodes WHERE id = $1 AND status = 'pending'`, nodeID); err != nil {
			return fmt.Errorf("delete pending node: %w", err)
		}
		return archiveOutbox(ctx, tx, outboxID, "failed", &reason)
	})
}
