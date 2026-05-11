package dbfs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// TrashEntry is the public projection of a single trash item, suitable for
// API responses and the GC worker.
type TrashEntry struct {
	TrashUUID       uuid.UUID
	RootNodeID      uuid.UUID
	RootLogicalPath string
	RootNodeType    string
	DeletedBy       uuid.UUID
	DeletedAt       time.Time
	SizeBytes       int64
}

// ListTrash enumerates every entry whose trash.meta.json could be loaded.
// Half-formed directories (mid-rename, missing meta) are skipped silently —
// the executor will finish them on the next pass.
func (fs *DBFS) ListTrash(_ context.Context) ([]TrashEntry, error) {
	if fs.trashDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(fs.trashDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read trash dir: %w", err)
	}
	out := make([]TrashEntry, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		entryDir := filepath.Join(fs.trashDir, e.Name())
		meta, err := readTrashMeta(entryDir)
		if err != nil {
			// Skip half-formed entries; they'll surface once the executor
			// finishes writing the sidecar.
			continue
		}
		size, _ := dirSize(filepath.Join(entryDir, "payload"))
		out = append(out, TrashEntry{
			TrashUUID:       meta.TrashUUID,
			RootNodeID:      meta.RootNodeID,
			RootLogicalPath: meta.RootLogicalPath,
			RootNodeType:    meta.RootNodeType,
			DeletedBy:       meta.DeletedBy,
			DeletedAt:       meta.DeletedAt,
			SizeBytes:       size,
		})
	}
	return out, nil
}

// dirSize returns the total bytes occupied by the file (or recursive subtree)
// at path. Missing path → 0 with no error.
func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return total, err
}

// Restore brings a trashed subtree back into the active tree. Steps:
//
//  1. Load trash.meta.json (must exist).
//  2. Rename trash payload back to its original on-disk location (atomically
//     within the data-dir filesystem). Missing payload is tolerated when the
//     subtree was an empty directory that never materialized on FlatFile.
//  3. In one transaction: clear deleted_at on the whole subtree (path <@ root)
//     so it becomes active again. Fail with ErrExists if the original name is
//     now occupied by a new sibling.
func (fs *DBFS) Restore(ctx context.Context, trashUUID uuid.UUID) error {
	entryDir := filepath.Join(fs.trashDir, trashUUID.String())
	meta, err := readTrashMeta(entryDir)
	if err != nil {
		return err
	}

	// Fetch the root node from DB so we know its current ltree path and parent.
	var rootPath string
	var parentID uuid.UUID
	var rootName string
	err = fs.pool.QueryRow(ctx,
		`SELECT path::text, parent_id, name FROM nodes WHERE id = $1`,
		meta.RootNodeID).Scan(&rootPath, &parentID, &rootName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errf(storage.ErrNotFound, "trashed node %s no longer exists in DB", meta.RootNodeID)
		}
		return fmt.Errorf("load trashed root: %w", err)
	}

	// Check that the original parent is still active and the name is free.
	var conflictID uuid.UUID
	err = fs.pool.QueryRow(ctx,
		`SELECT id FROM nodes WHERE parent_id = $1 AND name = $2 AND deleted_at IS NULL LIMIT 1`,
		parentID, rootName).Scan(&conflictID)
	if err == nil && conflictID != meta.RootNodeID {
		return errf(storage.ErrExists, "restore target %q is occupied", meta.RootLogicalPath)
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check restore conflict: %w", err)
	}

	// Move payload back into flat-storage (if any).
	payloadPath := filepath.Join(entryDir, "payload")
	if _, statErr := os.Lstat(payloadPath); statErr == nil {
		dstAbs, err := fs.resolveBackendPath(meta.RootBackendKind, storage.BackendRef{
			Kind: meta.RootBackendKind,
			Data: meta.RootBackendRef,
		})
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dstAbs), 0o700); err != nil {
			return fmt.Errorf("mkdir restore parent: %w", err)
		}
		if err := os.Rename(payloadPath, dstAbs); err != nil {
			return fmt.Errorf("rename trash → flat: %w", err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("lstat trash payload: %w", statErr)
	}

	// Resurrect the subtree in DB.
	err = pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE nodes
			 SET status = 'ready'::node_status,
			     deleted_at = NULL,
			     updated_at = now()
			 WHERE path <@ $1::ltree AND deleted_at IS NOT NULL`,
			rootPath)
		return err
	})
	if err != nil {
		return err
	}

	// Drop the (now empty) trash entry dir + meta file.
	if err := os.RemoveAll(entryDir); err != nil {
		return fmt.Errorf("remove trash entry: %w", err)
	}
	return nil
}

// Purge permanently deletes a trash entry. Both DB rows (ON DELETE CASCADE
// removes permissions/node_meta) and on-disk content are dropped. Idempotent:
// missing entry returns ErrNotFound.
func (fs *DBFS) Purge(ctx context.Context, trashUUID uuid.UUID) error {
	entryDir := filepath.Join(fs.trashDir, trashUUID.String())
	meta, err := readTrashMeta(entryDir)
	if err != nil {
		return err
	}

	// Find the root's path so we can DELETE the whole subtree. If the DB row
	// is already gone, skip the SQL step and just clean the disk.
	var rootPath *string
	err = fs.pool.QueryRow(ctx, `SELECT path::text FROM nodes WHERE id = $1`, meta.RootNodeID).Scan(&rootPath)
	switch {
	case err == nil:
		// fall through
	case errors.Is(err, pgx.ErrNoRows):
		rootPath = nil
	default:
		return fmt.Errorf("load root path: %w", err)
	}

	if rootPath != nil {
		_, err = fs.pool.Exec(ctx, `DELETE FROM nodes WHERE path <@ $1::ltree`, *rootPath)
		if err != nil {
			return fmt.Errorf("delete subtree: %w", err)
		}
	}

	if err := os.RemoveAll(entryDir); err != nil {
		return fmt.Errorf("remove trash entry: %w", err)
	}
	return nil
}

// RunGC purges every entry whose DeletedAt is older than retention. Returns
// the number of entries purged. Errors on individual entries are logged via
// the provided logger (or stdlib log if nil) and do not stop iteration —
// transient failure on one entry must not stall the whole sweep.
func (fs *DBFS) RunGC(ctx context.Context, retention time.Duration, logger *log.Logger) (int, error) {
	if logger == nil {
		logger = log.Default()
	}
	if retention <= 0 {
		return 0, nil
	}
	entries, err := fs.ListTrash(ctx)
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-retention)
	purged := 0
	for _, e := range entries {
		if e.DeletedAt.After(cutoff) {
			continue
		}
		if err := fs.Purge(ctx, e.TrashUUID); err != nil {
			logger.Printf("trash gc: purge %s: %v", e.TrashUUID, err)
			continue
		}
		purged++
	}
	return purged, nil
}

// StartGC launches the background trash GC loop. It returns immediately; the
// loop terminates when ctx is cancelled. Pass retentionDays<=0 to disable.
func (fs *DBFS) StartGC(ctx context.Context, interval time.Duration, retentionDays int, logger *log.Logger) {
	if interval <= 0 || retentionDays <= 0 {
		return
	}
	if logger == nil {
		logger = log.Default()
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		retention := time.Duration(retentionDays) * 24 * time.Hour
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := fs.RunGC(ctx, retention, logger); err != nil {
					logger.Printf("trash gc: sweep failed: %v", err)
				} else if n > 0 {
					logger.Printf("trash gc: purged %d entries past retention", n)
				}
			}
		}
	}()
}
