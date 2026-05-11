package flat

import (
	"errors"
	"fmt"
	"os"

	"github.com/alexnav/storman/internal/storage"
)

// flatWriter buffers writes into a staging tmpfile and publishes them to the
// target path on Commit via fsync + rename (atomic within the data-dir FS).
type flatWriter struct {
	file        *os.File
	stagingPath string
	targetPath  string
	mode        storage.WriteMode
	finalized   bool
	aborted     bool
	closed      bool
}

func (w *flatWriter) WriteAt(p []byte, off int64) (int, error) {
	if w.finalized || w.aborted {
		return 0, fmt.Errorf("storage: write after finalize")
	}
	return w.file.WriteAt(p, off)
}

func (w *flatWriter) Truncate(size int64) error {
	if w.finalized || w.aborted {
		return fmt.Errorf("storage: truncate after finalize")
	}
	return w.file.Truncate(size)
}

func (w *flatWriter) Commit() error {
	if w.finalized {
		return nil
	}
	if w.aborted {
		return fmt.Errorf("storage: commit after abort")
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync staging: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close staging: %w", err)
	}

	switch w.mode {
	case storage.WriteCreate:
		// Atomic exclusive create: hard-link tmp → target, then unlink tmp.
		if err := os.Link(w.stagingPath, w.targetPath); err != nil {
			if errors.Is(err, os.ErrExist) {
				// Leave staging in place for the caller's Abort/cleanup path —
				// dropping it here would mask the conflict cause.
				return fmt.Errorf("%w: %s", storage.ErrExists, w.targetPath)
			}
			return fmt.Errorf("link to target: %w", err)
		}
		if err := os.Remove(w.stagingPath); err != nil {
			return fmt.Errorf("remove staging after link: %w", err)
		}
	case storage.WriteOverwrite, storage.WriteModify:
		if err := os.Rename(w.stagingPath, w.targetPath); err != nil {
			return fmt.Errorf("rename staging: %w", err)
		}
	default:
		return fmt.Errorf("storage: unknown write mode %d", w.mode)
	}

	w.finalized = true
	return nil
}

func (w *flatWriter) Abort() error {
	if w.aborted {
		return nil
	}
	if w.finalized {
		return fmt.Errorf("storage: abort after commit")
	}
	w.aborted = true
	_ = w.file.Close()
	if err := os.Remove(w.stagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove staging: %w", err)
	}
	return nil
}

func (w *flatWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.finalized || w.aborted {
		return nil
	}
	// Caller forgot to finalize — make sure we don't leak the staging file,
	// but report the misuse so the bug is visible.
	_ = w.file.Close()
	_ = os.Remove(w.stagingPath)
	return storage.ErrUnfinalized
}
