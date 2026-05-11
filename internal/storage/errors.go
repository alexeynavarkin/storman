package storage

import "errors"

var (
	// ErrUnfinalized is returned by FileWriter.Close when the caller did not
	// invoke Commit or Abort first. Finalization must be explicit.
	ErrUnfinalized = errors.New("storage: writer closed without Commit or Abort")

	// ErrNotFound indicates the referenced node or backend ref does not exist.
	ErrNotFound = errors.New("storage: not found")

	// ErrExists indicates a WriteCreate target already exists at Commit time.
	ErrExists = errors.New("storage: target already exists")

	// ErrInvalidPath indicates the given logical path is malformed (absolute,
	// traversal, or non-canonical).
	ErrInvalidPath = errors.New("storage: invalid path")

	// ErrUnsupported indicates the backend does not support the requested operation.
	ErrUnsupported = errors.New("storage: unsupported operation")
)
