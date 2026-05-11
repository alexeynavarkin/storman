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
	info := n.toInfo(path)
	if n.Type == storage.NodeDir {
		var count int
		err := fs.pool.QueryRow(ctx,
			`SELECT count(*) FROM nodes WHERE parent_id = $1 AND deleted_at IS NULL`,
			n.ID).Scan(&count)
		if err != nil {
			return nil, err
		}
		info.Children = count
	}
	return info, nil
}
