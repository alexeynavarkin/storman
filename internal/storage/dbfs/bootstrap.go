package dbfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alexnav/storman/internal/storage"
)

// Bootstrap ensures the root node exists. Idempotent: if a root already exists
// (parent_id IS NULL and not deleted), the call is a no-op.
//
// The root carries a default backend policy of "flat" so new files inherit it
// unless an intermediate directory overrides backend_kind. See docs/arch/storage.md.
func (fs *DBFS) Bootstrap(ctx context.Context) (created bool, err error) {
	err = pgx.BeginFunc(ctx, fs.pool, func(tx pgx.Tx) error {
		_, rerr := fs.rootNode(ctx, tx)
		if rerr == nil {
			return nil
		}
		if !errors.Is(rerr, storage.ErrNotFound) {
			return rerr
		}
		id := uuid.New()
		if _, err := tx.Exec(ctx,
			`INSERT INTO nodes (id, parent_id, path, name, type, backend_kind, status)
			 VALUES ($1, NULL, $2::ltree, '', 'dir', $3, 'ready')`,
			id, ltreeLabel(id), fs.defaultBackendKind); err != nil {
			return fmt.Errorf("insert root: %w", err)
		}
		created = true
		return nil
	})
	return created, err
}
