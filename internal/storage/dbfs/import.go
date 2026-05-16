package dbfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/flat"
)

// ImportPath adopts an already-on-disk file at srcAbsPath as the content of
// the new node at logical. Same lifecycle as OpenWrite/Commit, but instead
// of streaming through a FileWriter the content is moved into the backend
// via os.Rename (atomic within the data-dir filesystem).
//
// Used by:
//   - tus resumable uploads (the file is already fully assembled in
//     meta-storage/uploads/<id>/data).
//   - recover-from-disk for the rare case where ingestion needs to be
//     re-driven through the same code path.
//
// Returns ErrExists when the target is already occupied.
func (fs *DBFS) ImportPath(ctx context.Context, logical, srcAbsPath string) error {
	segments, err := splitPath(logical)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errf(storage.ErrInvalidPath, "cannot import into root")
	}
	parentPath := joinPath(segments[:len(segments)-1])
	name := segments[len(segments)-1]

	srcInfo, err := os.Stat(srcAbsPath)
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}
	if srcInfo.IsDir() {
		return errf(storage.ErrInvalidPath, "source is a directory")
	}

	var (
		nodeID   uuid.UUID
		outboxID int64
		backend  *flat.Backend
		dstAbs   string
		ref      storage.BackendRef
	)

	err = pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		parent, err := fs.resolveByPath(ctx, tx, parentPath)
		if err != nil {
			return err
		}
		if parent.Type != storage.NodeDir {
			return errf(storage.ErrInvalidPath, "%q is not a directory", parentPath)
		}
		if _, err := fs.childNode(ctx, tx, parent.ID, name); err == nil {
			return errf(storage.ErrExists, "%q already exists", logical)
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}

		kind, err := fs.effectiveBackendKind(ctx, tx, parent)
		if err != nil {
			return err
		}
		// Import only knows how to land bytes via the FlatFile path layout.
		// Other backends (future CDC) would need their own ingest hook.
		b, ok := fs.backends[kind]
		if !ok {
			return errf(storage.ErrUnsupported, "backend %q not registered", kind)
		}
		fb, ok := b.(*flat.Backend)
		if !ok {
			return errf(storage.ErrUnsupported, "ImportPath only supports FlatFile backends, got %q", kind)
		}
		backend = fb

		nodeID = uuid.New()
		logicalRel := strings.Join(segments, "/")
		ref, err = backend.Allocate(ctx, storage.AllocHint{
			NodeID:       nodeID,
			LogicalPath:  logicalRel,
			ExpectedSize: srcInfo.Size(),
		})
		if err != nil {
			return fmt.Errorf("allocate ref: %w", err)
		}
		dstAbs, err = backend.ResolvePath(ref)
		if err != nil {
			return err
		}

		nodePath := ltreeAppend(parent.Path, ltreeLabel(nodeID))
		if _, err := tx.Exec(ctx,
			`INSERT INTO nodes (id, parent_id, path, name, type, backend_kind, backend_ref, status)
			 VALUES ($1, $2, $3::ltree, $4, 'file', $5, $6, 'pending')`,
			nodeID, parent.ID, nodePath, name, ref.Kind, ref.Data); err != nil {
			if isUniqueViolation(err) {
				return errf(storage.ErrExists, "%q already exists", logical)
			}
			return fmt.Errorf("insert pending node: %w", err)
		}
		payload := createFilePayload{NodeID: nodeID, BackendKind: ref.Kind, BackendRef: ref.Data}
		outboxID, err = insertOutbox(ctx, tx, opCreateFile, &nodeID, payload)
		return err
	})
	if err != nil {
		return err
	}

	// Atomically publish the staged file into flat-storage.
	if err := os.MkdirAll(filepath.Dir(dstAbs), 0o700); err != nil {
		_ = fs.rollbackPending(context.Background(), nodeID, outboxID, "mkdir parent: "+err.Error())
		return fmt.Errorf("mkdir parent: %w", err)
	}
	if err := os.Rename(srcAbsPath, dstAbs); err != nil {
		_ = fs.rollbackPending(context.Background(), nodeID, outboxID, "rename: "+err.Error())
		return fmt.Errorf("rename %s → %s: %w", srcAbsPath, dstAbs, err)
	}

	// Promote to ready + enqueue hash + archive outbox in one transaction.
	st, err := backend.Stat(ctx, ref)
	if err != nil {
		return fmt.Errorf("stat after import: %w", err)
	}
	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE nodes
			 SET status = 'ready', size = $1, mtime = now(), updated_at = now()
			 WHERE id = $2 AND status = 'pending'`,
			st.Size, nodeID); err != nil {
			return fmt.Errorf("mark ready: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO jobs (node_id, kind, status) VALUES ($1, 'hash', 'pending')`,
			nodeID); err != nil {
			return fmt.Errorf("enqueue hash job: %w", err)
		}
		return archiveOutbox(ctx, tx, outboxID, "done", nil)
	})
}
