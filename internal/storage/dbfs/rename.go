package dbfs

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// Rename moves the node at oldPath to newPath. The node keeps its backend_kind
// and backend_ref — physical content is not touched. Descendants of a renamed
// directory have their ltree paths rewritten in the same transaction.
//
// Rejects:
//   - renaming root,
//   - newPath already occupied by an active sibling,
//   - moving a directory into its own subtree.
func (fs *DBFS) Rename(ctx context.Context, oldPath, newPath string) error {
	oldSegs, err := splitPath(oldPath)
	if err != nil {
		return err
	}
	newSegs, err := splitPath(newPath)
	if err != nil {
		return err
	}
	if len(oldSegs) == 0 {
		return errf(storage.ErrInvalidPath, "cannot rename the root")
	}
	if len(newSegs) == 0 {
		return errf(storage.ErrInvalidPath, "cannot rename onto the root")
	}

	return pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		old, err := fs.resolveByPath(ctx, tx, oldPath)
		if err != nil {
			return err
		}

		newParent, err := fs.resolveByPath(ctx, tx, joinPath(newSegs[:len(newSegs)-1]))
		if err != nil {
			return err
		}
		if newParent.Type != storage.NodeDir {
			return errf(storage.ErrInvalidPath, "new parent %q is not a directory", joinPath(newSegs[:len(newSegs)-1]))
		}

		newName := newSegs[len(newSegs)-1]
		if existing, err := fs.childNode(ctx, tx, newParent.ID, newName); err == nil {
			if existing.ID == old.ID {
				// same-name same-parent rename → no-op.
				return nil
			}
			return errf(storage.ErrExists, "%q already exists", newPath)
		} else if !errors.Is(err, storage.ErrNotFound) {
			return err
		}

		// Loop check: refuse to move a directory into its own subtree. We use
		// ltree: new parent's path must not be descendant-or-equal of old path.
		if old.Type == storage.NodeDir {
			isInside, err := pathIsInside(ctx, tx, newParent.Path, old.Path)
			if err != nil {
				return err
			}
			if isInside {
				return errf(storage.ErrInvalidPath, "cannot move %q into its own subtree", oldPath)
			}
		}

		newNodePath := ltreeAppend(newParent.Path, lastLabel(old.Path))

		if _, err := tx.Exec(ctx,
			`UPDATE nodes SET parent_id=$1, name=$2, path=$3::ltree, updated_at=now() WHERE id=$4`,
			newParent.ID, newName, newNodePath, old.ID); err != nil {
			return err
		}

		if old.Type == storage.NodeDir {
			// Rewrite descendant paths: new = newNodePath || subpath(path, nlevel(oldPath))
			if _, err := tx.Exec(ctx,
				`UPDATE nodes
				 SET path = ($1::ltree || subpath(path, nlevel($2::ltree))),
				     updated_at = now()
				 WHERE path <@ $2::ltree AND id <> $3`,
				newNodePath, old.Path, old.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// pathIsInside reports whether candidate is a descendant of (or equal to) ancestor.
func pathIsInside(ctx context.Context, q querier, candidate, ancestor string) (bool, error) {
	var inside bool
	err := q.QueryRow(ctx, `SELECT $1::ltree <@ $2::ltree`, candidate, ancestor).Scan(&inside)
	return inside, err
}

// lastLabel returns the last dot-separated segment of an ltree path.
func lastLabel(path string) string {
	if i := strings.LastIndexByte(path, '.'); i >= 0 {
		return path[i+1:]
	}
	return path
}
