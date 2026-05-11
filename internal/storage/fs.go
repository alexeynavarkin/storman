package storage

import "context"

// FileSystem is the single facade over the node tree for all frontend
// protocols (Web API, FTP, FUSE, WebDAV, S3). There is one implementation,
// backed by the nodes table; it dispatches file content I/O to the
// appropriate FileBackend.
type FileSystem interface {
	// Tree operations.
	Stat(ctx context.Context, path string) (*NodeInfo, error)
	List(ctx context.Context, path string) ([]NodeInfo, error)
	Mkdir(ctx context.Context, path string, opts MkdirOpts) error
	// Remove soft-deletes the node, moving content to trash.
	Remove(ctx context.Context, path string) error
	// Rename moves a node within the tree. Nodes keep their backend_kind;
	// content is not converted between backends.
	Rename(ctx context.Context, oldPath, newPath string) error

	// Content operations.
	OpenRead(ctx context.Context, path string) (FileReader, error)
	OpenWrite(ctx context.Context, path string, opts WriteOpts) (FileWriter, error)
}
