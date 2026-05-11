package dbfs

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// Remove soft-deletes the node at path together with every active descendant.
// The whole subtree is marked status='deleted' in a single UPDATE driven by
// ltree containment, and a single op='trash' outbox row is enqueued for the
// root — the executor (handleTrashRow) physically moves the subtree into
// <data-dir>/meta-storage/trash/<uuid>/ once the transaction commits. See
// ADR-0002 and docs/arch/trash.md.
//
// Permission gating happens at the protocol layer on the root only:
// down-inheritance guarantees that holding Remove on the root implies Remove
// on every descendant.
//
// Rejects removing the tree root. Rejects targets already in trash.
func (fs *DBFS) Remove(ctx context.Context, path string) error {
	segments, err := splitPath(path)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errf(storage.ErrInvalidPath, "cannot remove the root")
	}

	deletedBy := storage.ActorFromContext(ctx)

	var (
		outboxID int64
		payload  trashPayload
	)
	err = pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		root, err := fs.resolveByPath(ctx, tx, path)
		if err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			`UPDATE nodes
			 SET status='deleted', deleted_at=now(), updated_at=now()
			 WHERE path <@ $1::ltree AND deleted_at IS NULL`,
			root.Path); err != nil {
			return err
		}

		payload = trashPayload{
			TrashUUID:       uuid.New(),
			RootNodeID:      root.ID,
			RootLogicalPath: path,
			RootNodeType:    string(root.Type),
			RootBackendKind: backendKindOfRoot(root),
			RootBackendRef:  backendRefOfRoot(root, path),
			DeletedBy:       deletedBy,
			DeletedAt:       time.Now().UTC(),
		}
		id, err := insertOutbox(ctx, tx, opTrash, &root.ID, payload)
		if err != nil {
			return err
		}
		outboxID = id
		return nil
	})
	if err != nil {
		return err
	}

	// Drive the on-disk move synchronously now that the DB commit is durable.
	// If this fails or the process crashes here, RecoverPending will pick the
	// row up at next startup. The executor is idempotent — re-running is safe.
	raw, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		// We already committed; surface the error so callers know the entry
		// is parked in the outbox awaiting recovery.
		return marshalErr
	}
	return fs.runTrashOutbox(ctx, outboxID, raw)
}

// backendKindOfRoot picks the backend that owns the root of the subtree being
// trashed. For files this is nodes.backend_kind (NOT NULL per schema). For
// directories the column is the inheritance policy and may be NULL — fall
// back to the FS default so the executor knows which backend to dispatch to.
func backendKindOfRoot(n nodeRow) string {
	if n.BackendKind != nil && *n.BackendKind != "" {
		return *n.BackendKind
	}
	return "flat"
}

// backendRefOfRoot returns the backend-specific ref for the subtree root. For
// FlatFile, directories live on disk under the same relative path that their
// logical path describes (minus the leading slash). For other backends in the
// future, this helper will need per-backend logic.
func backendRefOfRoot(n nodeRow, logicalPath string) string {
	if n.BackendRef != nil && *n.BackendRef != "" {
		return *n.BackendRef
	}
	return strings.TrimPrefix(logicalPath, "/")
}
