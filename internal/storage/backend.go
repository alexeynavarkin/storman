package storage

import "context"

// FileBackend stores the content of a single file. The FileSystem dispatches
// to a backend by nodes.backend_kind + nodes.backend_ref. Backends never see
// the node tree — they deal only in BackendRef.
type FileBackend interface {
	// Name returns the backend's kind, matching BackendRef.Kind.
	Name() string

	// Allocate reserves a BackendRef for a new file. Called before the writer
	// is opened so the FS can persist (backend_kind, backend_ref) and the
	// outbox entry in the same transaction.
	Allocate(ctx context.Context, hint AllocHint) (BackendRef, error)

	OpenRead(ctx context.Context, ref BackendRef) (FileReader, error)
	OpenWrite(ctx context.Context, ref BackendRef, opts WriteOpts) (FileWriter, error)

	Delete(ctx context.Context, ref BackendRef) error
	Stat(ctx context.Context, ref BackendRef) (BackendStat, error)
}
