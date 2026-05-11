package storage

import "io"

// FileReader is the read handle returned by OpenRead. Backends implement it
// over whatever physical storage they use.
type FileReader interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// FileWriter is the write handle returned by OpenWrite. Finalization is
// explicit: callers must invoke Commit or Abort before Close. Close without
// either returns ErrUnfinalized.
type FileWriter interface {
	io.WriterAt
	io.Closer
	Truncate(size int64) error
	Commit() error
	Abort() error
}
