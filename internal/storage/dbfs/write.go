package dbfs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// OpenWrite creates a new file at path (Create mode only in this slice).
// Lifecycle per ADR-0002 / docs/arch/storage.md: tx1 inserts a pending node + outbox row, returns a
// FileWriter wrapping the backend's writer. Caller writes content; on Commit
// the backend publishes the bytes and tx2 marks the node ready + archives the
// outbox row.
func (fs *DBFS) OpenWrite(ctx context.Context, path string, opts storage.WriteOpts) (storage.FileWriter, error) {
	if opts.Mode != storage.WriteCreate {
		return nil, errf(storage.ErrUnsupported, "only WriteCreate is supported in MVP")
	}
	segments, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errf(storage.ErrInvalidPath, "cannot write to root")
	}
	parentPath := joinPath(segments[:len(segments)-1])
	name := segments[len(segments)-1]

	var (
		nodeID   uuid.UUID
		outboxID int64
		ref      storage.BackendRef
		backend  storage.FileBackend
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
			return errf(storage.ErrExists, "%q already exists", path)
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}

		kind, err := fs.effectiveBackendKind(ctx, tx, parent)
		if err != nil {
			return err
		}
		b, ok := fs.backends[kind]
		if !ok {
			return errf(storage.ErrUnsupported, "backend %q not registered", kind)
		}
		backend = b

		nodeID = uuid.New()
		logical := strings.Join(segments, "/")
		ref, err = backend.Allocate(ctx, storage.AllocHint{
			NodeID:       nodeID,
			LogicalPath:  logical,
			ExpectedSize: opts.ExpectedSize,
		})
		if err != nil {
			return fmt.Errorf("allocate ref: %w", err)
		}

		nodePath := ltreeAppend(parent.Path, ltreeLabel(nodeID))
		if _, err := tx.Exec(ctx,
			`INSERT INTO nodes (id, parent_id, path, name, type, backend_kind, backend_ref, status)
			 VALUES ($1, $2, $3::ltree, $4, 'file', $5, $6, 'pending')`,
			nodeID, parent.ID, nodePath, name, ref.Kind, ref.Data); err != nil {
			if isUniqueViolation(err) {
				return errf(storage.ErrExists, "%q already exists", path)
			}
			return fmt.Errorf("insert pending node: %w", err)
		}

		payload := createFilePayload{NodeID: nodeID, BackendKind: ref.Kind, BackendRef: ref.Data}
		outboxID, err = insertOutbox(ctx, tx, opCreateFile, &nodeID, payload)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Crash injection point (test-only).
	if err := fs.fireHook(HookAfterCreateTx); err != nil {
		_ = fs.rollbackPending(context.Background(), nodeID, outboxID, "hook AfterCreateTx: "+err.Error())
		return nil, err
	}

	inner, err := backend.OpenWrite(ctx, ref, opts)
	if err != nil {
		_ = fs.rollbackPending(context.Background(), nodeID, outboxID, "backend OpenWrite failed: "+err.Error())
		return nil, err
	}

	return &fsWriter{
		fs:       fs,
		nodeID:   nodeID,
		outboxID: outboxID,
		backend:  backend,
		ref:      ref,
		inner:    inner,
	}, nil
}

// fsWriter wraps a backend FileWriter with the FS-level commit lifecycle: on
// Commit it both finalizes the backend write and transitions the node to
// 'ready' while archiving the outbox row.
type fsWriter struct {
	fs       *DBFS
	nodeID   uuid.UUID
	outboxID int64
	backend  storage.FileBackend
	ref      storage.BackendRef
	inner    storage.FileWriter

	finalized bool
	aborted   bool
	closed    bool
}

func (w *fsWriter) WriteAt(p []byte, off int64) (int, error) {
	return w.inner.WriteAt(p, off)
}

func (w *fsWriter) Truncate(size int64) error {
	return w.inner.Truncate(size)
}

func (w *fsWriter) Commit() error {
	if w.finalized {
		return nil
	}
	if w.aborted {
		return fmt.Errorf("storage: commit after abort")
	}

	if err := w.inner.Commit(); err != nil {
		return err
	}
	// inner.Close after Commit is a no-op per FlatFile semantics; call it to
	// release the fd.
	if err := w.inner.Close(); err != nil {
		return err
	}

	// Crash injection point (test-only). At this moment the bytes are at
	// the target path but the node is still 'pending' — exactly the state
	// RecoverPending must heal on the next startup.
	if err := w.fs.fireHook(HookAfterBackendCommit); err != nil {
		return err
	}

	ctx := context.Background()
	st, err := w.backend.Stat(ctx, w.ref)
	if err != nil {
		return fmt.Errorf("stat after commit: %w", err)
	}

	err = pgx.BeginFunc(ctx, w.fs.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE nodes
			 SET status = 'ready', size = $1, mtime = now(), updated_at = now()
			 WHERE id = $2 AND status = 'pending'`,
			st.Size, w.nodeID); err != nil {
			return fmt.Errorf("mark ready: %w", err)
		}
		// Enqueue async indexing in the same transaction so the job is
		// committed atomically with the node becoming 'ready'. See PLAN
		// §«Индексация».
		if _, err := tx.Exec(ctx,
			`INSERT INTO jobs (node_id, kind, status) VALUES ($1, 'hash', 'pending')`,
			w.nodeID); err != nil {
			return fmt.Errorf("enqueue hash job: %w", err)
		}
		return archiveOutbox(ctx, tx, w.outboxID, "done", nil)
	})
	if err != nil {
		return err
	}
	w.finalized = true
	return nil
}

func (w *fsWriter) Abort() error {
	if w.aborted {
		return nil
	}
	if w.finalized {
		return fmt.Errorf("storage: abort after commit")
	}
	w.aborted = true
	_ = w.inner.Abort()
	_ = w.inner.Close()
	return w.fs.rollbackPending(context.Background(), w.nodeID, w.outboxID, "aborted by caller")
}

func (w *fsWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.finalized || w.aborted {
		return nil
	}
	_ = w.Abort()
	return storage.ErrUnfinalized
}
