// Package flat implements the FlatFile FileBackend: user files are stored
// on disk as-is, one logical file == one regular file. Writes go through a
// staging tmp file in meta-storage/uploads/ and are published via fsync+rename
// on Commit (atomic within the data-dir filesystem). See PLAN.md §2.
package flat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alexnav/storman/internal/storage"
)

const BackendName = "flat"

// Backend implements storage.FileBackend over a pair of on-disk directories.
type Backend struct {
	storageDir string // <data-dir>/flat-storage
	uploadsDir string // <data-dir>/meta-storage/uploads
}

// New constructs a FlatFile backend rooted at the given directories. Both
// directories must exist (datadir.Bootstrap takes care of that).
func New(storageDir, uploadsDir string) *Backend {
	return &Backend{storageDir: storageDir, uploadsDir: uploadsDir}
}

func (b *Backend) Name() string { return BackendName }

func (b *Backend) Allocate(_ context.Context, hint storage.AllocHint) (storage.BackendRef, error) {
	rel := strings.TrimSpace(hint.LogicalPath)
	if rel == "" {
		return storage.BackendRef{}, fmt.Errorf("%w: logical path is empty", storage.ErrInvalidPath)
	}
	cleaned, err := cleanRel(rel)
	if err != nil {
		return storage.BackendRef{}, err
	}
	return storage.BackendRef{Kind: BackendName, Data: cleaned}, nil
}

func (b *Backend) OpenRead(_ context.Context, ref storage.BackendRef) (storage.FileReader, error) {
	target, err := b.resolve(ref)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, ref.Data)
		}
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &flatReader{file: f, size: info.Size()}, nil
}

func (b *Backend) OpenWrite(_ context.Context, ref storage.BackendRef, opts storage.WriteOpts) (storage.FileWriter, error) {
	target, err := b.resolve(ref)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir parent: %w", err)
	}

	stagingPath, err := b.allocStaging()
	if err != nil {
		return nil, err
	}

	// Pre-populate staging from existing target when Modify is requested.
	if opts.Mode == storage.WriteModify {
		if err := copyFile(target, stagingPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				os.Remove(stagingPath)
				return nil, fmt.Errorf("%w: cannot modify missing target %s", storage.ErrNotFound, ref.Data)
			}
			os.Remove(stagingPath)
			return nil, fmt.Errorf("seed modify staging: %w", err)
		}
	}

	f, err := os.OpenFile(stagingPath, os.O_RDWR, 0o600)
	if err != nil {
		os.Remove(stagingPath)
		return nil, err
	}
	return &flatWriter{
		file:        f,
		stagingPath: stagingPath,
		targetPath:  target,
		mode:        opts.Mode,
	}, nil
}

func (b *Backend) Delete(_ context.Context, ref storage.BackendRef) error {
	target, err := b.resolve(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", storage.ErrNotFound, ref.Data)
		}
		return err
	}
	return nil
}

func (b *Backend) Stat(_ context.Context, ref storage.BackendRef) (storage.BackendStat, error) {
	target, err := b.resolve(ref)
	if err != nil {
		return storage.BackendStat{}, err
	}
	info, err := os.Stat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storage.BackendStat{}, fmt.Errorf("%w: %s", storage.ErrNotFound, ref.Data)
		}
		return storage.BackendStat{}, err
	}
	return storage.BackendStat{Size: info.Size(), MTime: info.ModTime()}, nil
}

// StorageDir returns the on-disk root where this backend keeps published file
// content. Exposed for higher-level operations (trash move, restore) that
// rename across data-dir subdirectories.
func (b *Backend) StorageDir() string { return b.storageDir }

// ResolvePath returns the absolute on-disk path for ref. The path may or may
// not exist; callers must Stat it themselves. Exposed so dbfs can move
// content between flat-storage/ and meta-storage/trash/ atomically via
// os.Rename.
func (b *Backend) ResolvePath(ref storage.BackendRef) (string, error) {
	return b.resolve(ref)
}

// resolve maps a BackendRef to an absolute on-disk path under storageDir,
// re-validating the ref to catch tampered values.
func (b *Backend) resolve(ref storage.BackendRef) (string, error) {
	if ref.Kind != BackendName {
		return "", fmt.Errorf("%w: backend kind %q not handled by flat", storage.ErrUnsupported, ref.Kind)
	}
	cleaned, err := cleanRel(ref.Data)
	if err != nil {
		return "", err
	}
	return filepath.Join(b.storageDir, filepath.FromSlash(cleaned)), nil
}

func (b *Backend) allocStaging() (string, error) {
	var name [16]byte
	if _, err := rand.Read(name[:]); err != nil {
		return "", err
	}
	staging := filepath.Join(b.uploadsDir, hex.EncodeToString(name[:])+".tmp")
	f, err := os.OpenFile(staging, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create staging: %w", err)
	}
	f.Close()
	return staging, nil
}

// cleanRel validates a slash-separated relative path. The input must already
// be canonical: no leading/trailing slash, no `.`/`..` segments, no empty
// segments. Refusing to silently normalize keeps the backend ref a faithful
// echo of the logical path.
func cleanRel(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("%w: empty", storage.ErrInvalidPath)
	}
	if strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("%w: leading slash", storage.ErrInvalidPath)
	}
	if strings.HasSuffix(rel, "/") {
		return "", fmt.Errorf("%w: trailing slash", storage.ErrInvalidPath)
	}
	for _, part := range strings.Split(rel, "/") {
		switch part {
		case "":
			return "", fmt.Errorf("%w: empty segment", storage.ErrInvalidPath)
		case ".":
			return "", fmt.Errorf("%w: current-dir segment", storage.ErrInvalidPath)
		case "..":
			return "", fmt.Errorf("%w: parent traversal", storage.ErrInvalidPath)
		}
	}
	return rel, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
