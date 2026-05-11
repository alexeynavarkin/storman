package ftpsrv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	ftpserver "github.com/fclairamb/ftpserverlib"

	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
)

// GetHandle implements ftpserver.ClientDriverExtentionFileTransfer. It lets
// the driver hand back a Reader/Writer/Seeker/Closer triplet without
// implementing the full afero.File contract.
//
// flags carries os.O_RDONLY for downloads, os.O_WRONLY for uploads (possibly
// |os.O_CREATE or |os.O_APPEND). offset is the result of a prior REST command
// — used to resume downloads from a byte position.
func (c *clientFS) GetHandle(name string, flags int, offset int64) (ftpserver.FileTransfer, error) {
	ctx, cancel := c.ctx()
	clean := normalizePath(name)

	if flags&os.O_WRONLY != 0 {
		return c.openUpload(ctx, cancel, clean, flags)
	}
	return c.openDownload(ctx, cancel, clean, offset)
}

// openDownload covers RETR.
func (c *clientFS) openDownload(ctx context.Context, cancel context.CancelFunc, clean string, offset int64) (ftpserver.FileTransfer, error) {
	info, err := c.fs.Stat(ctx, clean)
	if err != nil {
		cancel()
		return nil, mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, info.ID, rbac.Read); err != nil {
		cancel()
		return nil, mapErr(err)
	}
	r, err := c.fs.OpenRead(ctx, clean)
	if err != nil {
		cancel()
		return nil, mapErr(err)
	}
	return &downloadHandle{
		ctx:    ctx,
		cancel: cancel,
		r:      r,
		size:   r.Size(),
		off:    offset,
	}, nil
}

// openUpload covers STOR. We only support create-new in this slice — overwrite
// would need WriteOverwrite which dbfs doesn't expose yet for the protocol
// layer. ftpserverlib retries with O_TRUNC for overwrites; we accept that by
// removing the existing target first only when ACL allows.
func (c *clientFS) openUpload(ctx context.Context, cancel context.CancelFunc, clean string, flags int) (ftpserver.FileTransfer, error) {
	parent, err := c.fs.Stat(ctx, parentDir(clean))
	if err != nil {
		cancel()
		return nil, mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, parent.ID, rbac.Write); err != nil {
		cancel()
		return nil, mapErr(err)
	}

	// If the target exists and the client wants to overwrite or append,
	// remove the existing node first. Append (O_APPEND) is approximated as
	// overwrite — true resume requires WriteModify in dbfs which is parked.
	if flags&(os.O_TRUNC|os.O_APPEND) != 0 {
		if existing, statErr := c.fs.Stat(ctx, clean); statErr == nil {
			if err := c.perms.Check(ctx, c.user.ID, existing.ID, rbac.Remove); err != nil {
				cancel()
				return nil, mapErr(err)
			}
			if err := c.fs.Remove(ctx, clean); err != nil {
				cancel()
				return nil, mapErr(err)
			}
		}
	}

	w, err := c.fs.OpenWrite(ctx, clean, storage.WriteOpts{Mode: storage.WriteCreate, ExpectedSize: -1})
	if err != nil {
		cancel()
		return nil, mapErr(err)
	}
	return &uploadHandle{
		ctx:    ctx,
		cancel: cancel,
		w:      w,
	}, nil
}

// downloadHandle streams a dbfs FileReader to the client. ReadAt powers
// Read/Seek; Write is rejected.
type downloadHandle struct {
	ctx    context.Context
	cancel context.CancelFunc
	r      storage.FileReader
	size   int64
	off    int64
	closed bool
}

func (d *downloadHandle) Read(p []byte) (int, error) {
	if d.off >= d.size {
		return 0, io.EOF
	}
	n, err := d.r.ReadAt(p, d.off)
	d.off += int64(n)
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil
	}
	return n, err
}

func (d *downloadHandle) Write(_ []byte) (int, error) {
	return 0, errors.New("storman/ftp: write to download handle")
}

func (d *downloadHandle) Seek(offset int64, whence int) (int64, error) {
	var newOff int64
	switch whence {
	case io.SeekStart:
		newOff = offset
	case io.SeekCurrent:
		newOff = d.off + offset
	case io.SeekEnd:
		newOff = d.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if newOff < 0 {
		return 0, errors.New("negative seek")
	}
	d.off = newOff
	return d.off, nil
}

func (d *downloadHandle) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	err := d.r.Close()
	d.cancel()
	return err
}

// uploadHandle streams writes from the client into a dbfs FileWriter. Reads
// and Seeks are unsupported (FTP STOR doesn't need them; for resumable
// upload through REST/APPE we'd need WriteModify, which is parked).
type uploadHandle struct {
	ctx    context.Context
	cancel context.CancelFunc
	w      storage.FileWriter
	off    int64
	closed bool
	failed bool
}

func (u *uploadHandle) Read(_ []byte) (int, error) {
	return 0, errors.New("storman/ftp: read from upload handle")
}

func (u *uploadHandle) Write(p []byte) (int, error) {
	n, err := u.w.WriteAt(p, u.off)
	u.off += int64(n)
	if err != nil {
		u.failed = true
	}
	return n, err
}

// Seek on an upload supports only the trivial cases ftpserverlib actually
// uses (querying the current offset and seeking to the same).
func (u *uploadHandle) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = u.off + offset
	default:
		return 0, fmt.Errorf("upload: unsupported whence %d", whence)
	}
	if target != u.off {
		return 0, errors.New("upload: non-sequential writes are not supported")
	}
	return u.off, nil
}

// Close finalizes (Commit) or rolls back (Abort) the upload. ftpserverlib
// calls Close once the client transitions out of the data phase — success
// for the whole transfer means Commit, anything else means Abort.
func (u *uploadHandle) Close() error {
	if u.closed {
		return nil
	}
	u.closed = true
	defer u.cancel()
	if u.failed {
		_ = u.w.Abort()
		_ = u.w.Close()
		return errors.New("upload aborted after write error")
	}
	if err := u.w.Commit(); err != nil {
		_ = u.w.Close()
		return err
	}
	if err := u.w.Close(); err != nil && !errors.Is(err, storage.ErrUnfinalized) {
		return err
	}
	return nil
}

// shut up unused-imports lint when build tags trim references.
var _ = time.Second
