package web

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/alexnav/storman/internal/storage"
)

// nodeFileInfo adapts storage.NodeInfo to os.FileInfo for the webdav package.
// Mode bits are synthetic: WebDAV doesn't model unix perms, just file vs dir.
type nodeFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func toFileInfo(n storage.NodeInfo) os.FileInfo {
	return nodeFileInfo{
		name:    n.Name,
		size:    n.Size,
		modTime: n.MTime,
		isDir:   n.Type == storage.NodeDir,
	}
}

func (i nodeFileInfo) Name() string       { return i.name }
func (i nodeFileInfo) Size() int64        { return i.size }
func (i nodeFileInfo) ModTime() time.Time { return i.modTime }
func (i nodeFileInfo) IsDir() bool        { return i.isDir }
func (i nodeFileInfo) Sys() any           { return nil }
func (i nodeFileInfo) Mode() fs.FileMode {
	if i.isDir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}

// davReader serves WebDAV GET. storage.FileReader exposes ReadAt + Size; we
// track a cursor so io.Copy / http.ServeContent can read sequentially or
// seek for Range requests.
type davReader struct {
	r    storage.FileReader
	info storage.NodeInfo
	pos  int64
}

func (f *davReader) Read(p []byte) (int, error) {
	if f.pos >= f.r.Size() {
		return 0, io.EOF
	}
	n, err := f.r.ReadAt(p, f.pos)
	f.pos += int64(n)
	// ReadAt returns io.EOF when the read crossed EOF mid-buffer; that's not
	// an error for sequential Read — but if we got data, surface it first.
	if errors.Is(err, io.EOF) && n > 0 {
		return n, nil
	}
	return n, err
}

func (f *davReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		abs = f.r.Size() + offset
	default:
		return 0, errors.New("davReader: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("davReader: negative position")
	}
	f.pos = abs
	return abs, nil
}

func (f *davReader) Write(p []byte) (int, error) { return 0, errors.New("davReader: read-only") }
func (f *davReader) Readdir(int) ([]os.FileInfo, error) {
	return nil, errors.New("davReader: not a directory")
}
func (f *davReader) Stat() (os.FileInfo, error) { return toFileInfo(f.info), nil }
func (f *davReader) Close() error                { return f.r.Close() }

// davWriter serves WebDAV PUT. storage.FileWriter is WriteAt-based and needs
// explicit Commit/Abort before Close. We track sequential offset for Write
// and propagate Seek so clients (rclone, some macOS Finder paths) can patch
// at arbitrary offsets within the same upload. Any error during writes flips
// `failed` so Close aborts instead of committing — keeps storage consistent
// when an upload dies mid-stream.
type davWriter struct {
	w      storage.FileWriter
	info   storage.NodeInfo
	pos    int64
	failed bool
	closed bool
}

func (f *davWriter) Write(p []byte) (int, error) {
	n, err := f.w.WriteAt(p, f.pos)
	f.pos += int64(n)
	if err != nil {
		f.failed = true
	}
	return n, err
}

func (f *davWriter) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		// FileWriter doesn't expose current size before Commit; SeekEnd would
		// require backend-specific stat. WebDAV clients don't legitimately
		// use SeekEnd on a writer — return an error.
		return 0, errors.New("davWriter: SeekEnd unsupported")
	default:
		return 0, errors.New("davWriter: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("davWriter: negative position")
	}
	f.pos = abs
	return abs, nil
}

func (f *davWriter) Read([]byte) (int, error) { return 0, errors.New("davWriter: write-only") }
func (f *davWriter) Readdir(int) ([]os.FileInfo, error) {
	return nil, errors.New("davWriter: not a directory")
}
func (f *davWriter) Stat() (os.FileInfo, error) {
	// Reflect committed size if available; otherwise running pos.
	info := f.info
	if f.pos > info.Size {
		info.Size = f.pos
	}
	return toFileInfo(info), nil
}

func (f *davWriter) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if f.failed {
		_ = f.w.Abort()
		return f.w.Close()
	}
	if err := f.w.Commit(); err != nil {
		_ = f.w.Close()
		return err
	}
	if err := f.w.Close(); err != nil && !errors.Is(err, storage.ErrUnfinalized) {
		return err
	}
	return nil
}

// davDir serves PROPFIND on a directory: the webdav package calls Readdir on
// the opened directory to enumerate children. We materialize the listing on
// first Readdir() call; subsequent calls drain in batches per the io.Reader
// convention for Readdir.
type davDir struct {
	info  storage.NodeInfo
	items []os.FileInfo
	pos   int
}

func (d *davDir) Read([]byte) (int, error) { return 0, errors.New("davDir: cannot read directory") }
func (d *davDir) Write([]byte) (int, error) {
	return 0, errors.New("davDir: cannot write directory")
}
func (d *davDir) Seek(int64, int) (int64, error) {
	return 0, errors.New("davDir: seek unsupported")
}
func (d *davDir) Close() error                  { return nil }
func (d *davDir) Stat() (os.FileInfo, error)    { return toFileInfo(d.info), nil }

func (d *davDir) Readdir(count int) ([]os.FileInfo, error) {
	remaining := len(d.items) - d.pos
	if remaining <= 0 {
		if count <= 0 {
			return []os.FileInfo{}, nil
		}
		return nil, io.EOF
	}
	if count <= 0 || count > remaining {
		count = remaining
	}
	out := d.items[d.pos : d.pos+count]
	d.pos += count
	return out, nil
}
