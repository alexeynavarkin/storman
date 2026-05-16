package dbfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// PathByID resolves a node's UUID to its logical path under the active tree.
// Used by share-links (which key on node id, not path) so the protocol layer
// can dispatch into FileSystem methods that take a path. Soft-deleted nodes
// are not visible.
//
// This is a non-interface method on the same model as ImportPath / ListTrash:
// the FileSystem interface deliberately models the tree by path; surfacing
// id-based lookup keeps the boundary intact (ADR-0001 § Exception) without
// adding ID semantics to the public FS contract.
func (fs *DBFS) PathByID(ctx context.Context, id uuid.UUID) (string, error) {
	var (
		logical string
		found   bool
	)
	err := fs.pool.QueryRow(ctx, `
		WITH RECURSIVE chain AS (
		    SELECT id, parent_id, name, 0 AS depth
		      FROM nodes
		     WHERE id = $1 AND deleted_at IS NULL
		    UNION ALL
		    SELECT n.id, n.parent_id, n.name, c.depth + 1
		      FROM nodes n
		      JOIN chain c ON c.parent_id = n.id
		     WHERE n.deleted_at IS NULL
		)
		SELECT
		    EXISTS(SELECT 1 FROM chain) AS found,
		    COALESCE('/' || string_agg(name, '/' ORDER BY depth DESC), '/') AS path
		FROM chain WHERE name <> ''`, id).Scan(&found, &logical)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", errf(storage.ErrNotFound, "node %s not found", id)
		}
		return "", fmt.Errorf("locate node %s: %w", id, err)
	}
	if !found {
		return "", errf(storage.ErrNotFound, "node %s not found", id)
	}
	if logical == "" {
		logical = "/"
	}
	return logical, nil
}
