package dbfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/flat"
)

// TrashMetaFile is the per-entry sidecar inside <trash>/<uuid>/. It lets
// `storman recover --from-disk` rebuild basic trash awareness even without
// the database — see docs/arch/trash.md.
const TrashMetaFile = "trash.meta.json"

// trashMeta is the on-disk shape of trash.meta.json. Kept stable so external
// tools (and recovery code) can read trash entries without the live db.
type trashMeta struct {
	TrashUUID       uuid.UUID `json:"trash_uuid"`
	RootNodeID      uuid.UUID `json:"root_node_id"`
	RootLogicalPath string    `json:"root_logical_path"`
	RootNodeType    string    `json:"root_node_type"`
	RootBackendKind string    `json:"root_backend_kind"`
	RootBackendRef  string    `json:"root_backend_ref"`
	DeletedBy       uuid.UUID `json:"deleted_by"`
	DeletedAt       time.Time `json:"deleted_at"`
}

// fromPayload builds the on-disk sidecar shape from the outbox payload. The
// two structs are kept separate so the outbox JSON format can evolve without
// touching the persisted trash.meta.json schema.
func trashMetaFromPayload(p trashPayload) trashMeta {
	return trashMeta{
		TrashUUID:       p.TrashUUID,
		RootNodeID:      p.RootNodeID,
		RootLogicalPath: p.RootLogicalPath,
		RootNodeType:    p.RootNodeType,
		RootBackendKind: p.RootBackendKind,
		RootBackendRef:  p.RootBackendRef,
		DeletedBy:       p.DeletedBy,
		DeletedAt:       p.DeletedAt,
	}
}

// runTrashOutbox is invoked synchronously by Remove right after its DB
// transaction commits. The op is also picked up by RecoverPending at startup
// if the process crashed before reaching here, so this function must be
// idempotent: a rerun on an already-moved subtree finishes the meta sidecar
// and archives the row.
func (fs *DBFS) runTrashOutbox(ctx context.Context, outboxID int64, raw []byte) error {
	var p trashPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("unmarshal trash payload: %w", err)
	}
	if err := fs.applyTrash(ctx, p); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		return archiveOutbox(ctx, tx, outboxID, "done", nil)
	})
}

// applyTrash performs the disk-side move (or finishes one that was started
// before a crash). Steps, in order:
//
//  1. Ensure <trash>/<uuid>/ exists.
//  2. If <trash>/<uuid>/payload is missing, rename the live content into it.
//     Missing source is tolerated only when payload already exists (a prior
//     run succeeded and we are now finishing the sidecar).
//  3. Write trash.meta.json (overwrite-safe).
func (fs *DBFS) applyTrash(_ context.Context, p trashPayload) error {
	entryDir := filepath.Join(fs.trashDir, p.TrashUUID.String())
	payloadPath := filepath.Join(entryDir, "payload")

	if err := os.MkdirAll(entryDir, 0o700); err != nil {
		return fmt.Errorf("mkdir trash entry: %w", err)
	}

	if _, err := os.Lstat(payloadPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("lstat trash payload: %w", err)
		}
		// Payload not yet moved — do the rename now.
		srcAbs, err := fs.resolveBackendPath(p.RootBackendKind, storage.BackendRef{
			Kind: p.RootBackendKind,
			Data: p.RootBackendRef,
		})
		if err != nil {
			return err
		}
		if _, err := os.Lstat(srcAbs); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Source already gone AND payload missing — nothing to move.
				// Could be an empty directory (never materialized on FlatFile)
				// or a never-published pending node. Proceed to write meta so
				// the entry exists for audit, with empty payload.
			} else {
				return fmt.Errorf("lstat source: %w", err)
			}
		} else {
			if err := os.Rename(srcAbs, payloadPath); err != nil {
				return fmt.Errorf("rename %q → %q: %w", srcAbs, payloadPath, err)
			}
		}
	}

	return writeTrashMeta(entryDir, trashMetaFromPayload(p))
}

// resolveBackendPath returns an absolute on-disk path for ref. Today only the
// FlatFile backend exposes a path; other backends (CDC, …) will need their
// own trash handler since their content is not a single file. Returning
// ErrUnsupported keeps the failure mode loud rather than silent.
func (fs *DBFS) resolveBackendPath(kind string, ref storage.BackendRef) (string, error) {
	b, ok := fs.backends[kind]
	if !ok {
		return "", errf(storage.ErrUnsupported, "backend %q not registered", kind)
	}
	flatB, ok := b.(*flat.Backend)
	if !ok {
		return "", errf(storage.ErrUnsupported, "backend %q does not support disk-level trash move", kind)
	}
	return flatB.ResolvePath(ref)
}

// writeTrashMeta serializes meta into trash.meta.json with atomic semantics:
// write tmp + rename. Overwrite is fine — meta is idempotent.
func writeTrashMeta(entryDir string, m trashMeta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trash meta: %w", err)
	}
	tmp, err := os.CreateTemp(entryDir, ".trash.meta.*.tmp")
	if err != nil {
		return fmt.Errorf("create trash meta tmp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write trash meta: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("fsync trash meta: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close trash meta: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(entryDir, TrashMetaFile)); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename trash meta: %w", err)
	}
	return nil
}

// readTrashMeta loads trash.meta.json from an entry directory. Returns
// ErrNotFound if the sidecar is missing — useful for the GC worker to skip
// half-formed entries that may still be transitioning.
func readTrashMeta(entryDir string) (trashMeta, error) {
	data, err := os.ReadFile(filepath.Join(entryDir, TrashMetaFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return trashMeta{}, storage.ErrNotFound
		}
		return trashMeta{}, fmt.Errorf("read trash meta: %w", err)
	}
	var m trashMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return trashMeta{}, fmt.Errorf("unmarshal trash meta: %w", err)
	}
	return m, nil
}
