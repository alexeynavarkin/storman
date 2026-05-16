package dbfs

import (
	"context"

	"github.com/alexnav/storman/internal/storage"
)

func (fs *DBFS) Stat(ctx context.Context, path string) (*storage.NodeInfo, error) {
	n, err := fs.resolveByPath(ctx, fs.pool, path)
	if err != nil {
		return nil, err
	}
	// Hide nodes mid-lifecycle (pending writes, broken backend refs) from
	// public callers; only fully-committed entries are visible. Internal
	// callers (OpenWrite collision check, recovery) go through resolveByPath
	// directly and still see those rows.
	if n.Status != "ready" {
		return nil, errf(storage.ErrNotFound, "%q is not ready (status=%s)", path, n.Status)
	}
	info := n.toInfo(path)
	if n.Type == storage.NodeDir {
		var count int
		err := fs.pool.QueryRow(ctx,
			`SELECT count(*) FROM nodes
			 WHERE parent_id = $1 AND deleted_at IS NULL AND status = 'ready'`,
			n.ID).Scan(&count)
		if err != nil {
			return nil, err
		}
		info.Children = count
	}
	return info, nil
}
