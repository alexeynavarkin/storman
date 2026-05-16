package dbfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// Mkdir creates a directory at path. With opts.Parents=true, missing intermediate
// directories are created and the call is a no-op if the target already exists
// as a directory.
func (fs *DBFS) Mkdir(ctx context.Context, path string, opts storage.MkdirOpts) error {
	segments, err := splitPath(path)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		// Root always exists.
		return nil
	}

	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		cur, err := fs.rootNode(ctx, tx)
		if err != nil {
			return err
		}
		for i, seg := range segments {
			child, err := fs.childNode(ctx, tx, cur.ID, seg)
			switch {
			case err == nil:
				// Segment exists. Final segment must already be a directory (and
				// caller must allow that via Parents=true), intermediate
				// segments must be directories regardless.
				if child.Type != storage.NodeDir {
					return errf(storage.ErrExists, "%q is a file", joinPath(segments[:i+1]))
				}
				if i == len(segments)-1 && !opts.Parents {
					return errf(storage.ErrExists, "%q already exists", path)
				}
				cur = child
			case errors.Is(err, storage.ErrNotFound):
				if i < len(segments)-1 && !opts.Parents {
					return errf(storage.ErrNotFound, "parent %q is missing (use Parents=true)", joinPath(segments[:i+1]))
				}
				created, err := fs.insertDir(ctx, tx, cur, seg)
				if err != nil {
					return err
				}
				cur = created
			default:
				return err
			}
		}
		return nil
	})
}

// insertDir inserts a new directory node as child of parent. Concurrent
// Mkdirs for the same path race past the in-tx childNode check; the partial
// unique index on (parent_id, name) catches the second one and we map the
// SQL error back to ErrExists for a single canonical signal.
func (fs *DBFS) insertDir(ctx context.Context, tx pgx.Tx, parent nodeRow, name string) (nodeRow, error) {
	id := uuid.New()
	childPath := ltreeAppend(parent.Path, ltreeLabel(id))
	_, err := tx.Exec(ctx,
		`INSERT INTO nodes (id, parent_id, path, name, type, status)
		 VALUES ($1, $2, $3::ltree, $4, 'dir', 'ready')`,
		id, parent.ID, childPath, name)
	if err != nil {
		if isUniqueViolation(err) {
			return nodeRow{}, errf(storage.ErrExists, "%q already exists", name)
		}
		return nodeRow{}, fmt.Errorf("insert dir %q: %w", name, err)
	}
	return nodeRow{
		ID:       id,
		ParentID: &parent.ID,
		Path:     childPath,
		Name:     name,
		Type:     storage.NodeDir,
		Status:   "ready",
	}, nil
}
