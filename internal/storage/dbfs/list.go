package dbfs

import (
	"context"
	"strings"

	"github.com/alexnav/storman/internal/storage"
)

func (fs *DBFS) List(ctx context.Context, path string) ([]storage.NodeInfo, error) {
	parent, err := fs.resolveByPath(ctx, fs.pool, path)
	if err != nil {
		return nil, err
	}
	if parent.Type != storage.NodeDir {
		return nil, errf(storage.ErrInvalidPath, "%q is not a directory", path)
	}

	rows, err := fs.pool.Query(ctx,
		`SELECT `+nodeCols+`
		 FROM nodes
		 WHERE parent_id = $1 AND deleted_at IS NULL AND status = 'ready'
		 ORDER BY type DESC, name ASC`,
		parent.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prefix := strings.TrimRight(path, "/") + "/"
	out := make([]storage.NodeInfo, 0)
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		info := n.toInfo(prefix + n.Name)
		out = append(out, *info)
	}
	return out, rows.Err()
}
