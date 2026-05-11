package dbfs

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// effectiveBackendKind returns the backend that should host new files created
// under parent: the nearest ancestor (including parent itself) with a non-NULL
// nodes.backend_kind. Root is bootstrapped with kind = defaultBackendKind, so
// in a healthy tree this query always returns a row.
func (fs *DBFS) effectiveBackendKind(ctx context.Context, q querier, parent nodeRow) (string, error) {
	var kind string
	err := q.QueryRow(ctx,
		`SELECT backend_kind FROM nodes
		 WHERE path @> $1::ltree
		   AND backend_kind IS NOT NULL
		   AND type = 'dir'
		   AND deleted_at IS NULL
		 ORDER BY nlevel(path) DESC
		 LIMIT 1`,
		parent.Path,
	).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return fs.defaultBackendKind, nil
	}
	if err != nil {
		return "", err
	}
	return kind, nil
}
