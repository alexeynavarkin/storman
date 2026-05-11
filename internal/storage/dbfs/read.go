package dbfs

import (
	"context"

	"github.com/alexnav/storman/internal/storage"
)

// OpenRead resolves the node at path and dispatches to its FileBackend.
// Files with status != 'ready' (e.g. an in-flight upload) are not readable.
func (fs *DBFS) OpenRead(ctx context.Context, path string) (storage.FileReader, error) {
	n, err := fs.resolveByPath(ctx, fs.pool, path)
	if err != nil {
		return nil, err
	}
	if n.Type != storage.NodeFile {
		return nil, errf(storage.ErrInvalidPath, "%q is not a file", path)
	}
	if n.Status != "ready" {
		return nil, errf(storage.ErrNotFound, "%q is not ready (status=%s)", path, n.Status)
	}
	if n.BackendKind == nil || n.BackendRef == nil {
		return nil, errf(storage.ErrNotFound, "%q has no backend ref", path)
	}
	backend, ok := fs.backends[*n.BackendKind]
	if !ok {
		return nil, errf(storage.ErrUnsupported, "backend %q not registered", *n.BackendKind)
	}
	return backend.OpenRead(ctx, storage.BackendRef{Kind: *n.BackendKind, Data: *n.BackendRef})
}
