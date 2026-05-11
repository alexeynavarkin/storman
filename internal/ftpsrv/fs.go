// Package ftpsrv exposes the storman tree over FTPS. It is a thin adapter
// layer: every operation eventually delegates to dbfs.DBFS and gates each
// call through rbac.PermissionService with the per-connection user.
//
// Plain FTP is never accepted — the server is configured with mandatory TLS
// (AUTH TLS / explicit FTPS). See PLAN.md §«Интерфейсы».
package ftpsrv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

// clientFS is the afero.Fs implementation handed back to ftpserverlib once a
// client has authenticated. It captures the user so every call can be ACL-
// checked, and routes to the shared dbfs.DBFS / rbac.PermissionService.
type clientFS struct {
	user  *auth.User
	fs    *dbfs.DBFS
	perms *rbac.PermissionService
}

// ctx returns a per-call context with the actor tag set so dbfs records the
// user in trash payloads. Two-minute timeout keeps a stuck transfer from
// pinning the goroutine forever.
func (c *clientFS) ctx() (context.Context, context.CancelFunc) {
	base, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	return storage.WithActor(base, c.user.ID), cancel
}

// Name identifies the filesystem in afero diagnostics.
func (c *clientFS) Name() string { return "storman-ftp" }

// Chmod / Chown / Chtimes — storman doesn't model POSIX permissions or
// arbitrary timestamps from clients; report success so MFMT-style commands
// from polite clients don't kill the session.
func (c *clientFS) Chmod(_ string, _ os.FileMode) error           { return nil }
func (c *clientFS) Chown(_ string, _, _ int) error                { return nil }
func (c *clientFS) Chtimes(_ string, _, _ time.Time) error        { return nil }

// normalizePath maps an FTP-side path (which may be relative or contain
// `.`/`..` segments) into the canonical absolute path dbfs expects. The FTP
// server already maintains a current directory and prefixes relative paths,
// so most calls arrive absolute — but path.Clean still strips empty/dot
// segments defensively.
func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if cleaned == "." {
		return "/"
	}
	return cleaned
}

// Stat returns an os.FileInfo for the node at name. Required by ftpserverlib
// before any GET/LIST/MLST so the protocol can announce file type/size.
func (c *clientFS) Stat(name string) (os.FileInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	info, err := c.fs.Stat(ctx, normalizePath(name))
	if err != nil {
		return nil, mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, info.ID, rbac.Read); err != nil {
		return nil, mapErr(err)
	}
	return nodeStat{info: *info}, nil
}

// ReadDir is an ftpserverlib extension that lets the driver answer LIST/NLST
// without producing an afero.File. Equivalent to Open(name).Readdir(-1) but
// avoids the file-handle dance.
func (c *clientFS) ReadDir(name string) ([]os.FileInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	parent, err := c.fs.Stat(ctx, normalizePath(name))
	if err != nil {
		return nil, mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, parent.ID, rbac.Read); err != nil {
		return nil, mapErr(err)
	}
	entries, err := c.fs.List(ctx, normalizePath(name))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]os.FileInfo, 0, len(entries))
	for i := range entries {
		out = append(out, nodeStat{info: entries[i]})
	}
	return out, nil
}

// Mkdir creates a directory. ftpserverlib's MKD maps here.
func (c *clientFS) Mkdir(name string, _ os.FileMode) error {
	ctx, cancel := c.ctx()
	defer cancel()
	clean := normalizePath(name)
	parent, err := c.fs.Stat(ctx, parentDir(clean))
	if err != nil {
		return mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, parent.ID, rbac.Write); err != nil {
		return mapErr(err)
	}
	return mapErr(c.fs.Mkdir(ctx, clean, storage.MkdirOpts{Parents: false}))
}

// MkdirAll is rarely used by FTP clients but afero requires it.
func (c *clientFS) MkdirAll(name string, _ os.FileMode) error {
	ctx, cancel := c.ctx()
	defer cancel()
	clean := normalizePath(name)
	parent, err := c.deepestExistingParent(ctx, clean)
	if err != nil {
		return mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, parent.ID, rbac.Write); err != nil {
		return mapErr(err)
	}
	return mapErr(c.fs.Mkdir(ctx, clean, storage.MkdirOpts{Parents: true}))
}

// Remove handles both DELE (file) and RMD (directory) — the driver is
// notified via the ClientDriverExtensionRemoveDir hook to keep error codes
// distinct. We treat both identically here: dbfs.Remove recursively trashes
// the subtree.
func (c *clientFS) Remove(name string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	clean := normalizePath(name)
	target, err := c.fs.Stat(ctx, clean)
	if err != nil {
		return mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, target.ID, rbac.Remove); err != nil {
		return mapErr(err)
	}
	return mapErr(c.fs.Remove(ctx, clean))
}

// RemoveAll behaves like Remove since dbfs.Remove is already recursive.
func (c *clientFS) RemoveAll(name string) error { return c.Remove(name) }

// RemoveDir is the ClientDriverExtensionRemoveDir hook. Same body as Remove
// but lets ftpserverlib reply with the directory-specific status code.
func (c *clientFS) RemoveDir(name string) error { return c.Remove(name) }

// Rename maps to dbfs.Rename — both clients (MOVE/RNFR+RNTO from FTP, PATCH
// from Web) end up at the same operation.
func (c *clientFS) Rename(oldname, newname string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	oldClean := normalizePath(oldname)
	newClean := normalizePath(newname)

	src, err := c.fs.Stat(ctx, oldClean)
	if err != nil {
		return mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, src.ID, rbac.Write); err != nil {
		return mapErr(err)
	}
	dstParent, err := c.fs.Stat(ctx, parentDir(newClean))
	if err != nil {
		return mapErr(err)
	}
	if err := c.perms.Check(ctx, c.user.ID, dstParent.ID, rbac.Write); err != nil {
		return mapErr(err)
	}
	return mapErr(c.fs.Rename(ctx, oldClean, newClean))
}

// Create / Open / OpenFile route through the simpler GetHandle extension —
// these stubs exist only so the afero.Fs interface is satisfied. Returning
// errPathNoFile keeps ftpserverlib from accidentally relying on them.
func (c *clientFS) Create(_ string) (afero.File, error) {
	return nil, errPathNoFile
}
func (c *clientFS) Open(_ string) (afero.File, error) {
	return nil, errPathNoFile
}
func (c *clientFS) OpenFile(_ string, _ int, _ os.FileMode) (afero.File, error) {
	return nil, errPathNoFile
}

// deepestExistingParent walks `clean` upward until it hits an existing node.
// Returned info points at that ancestor.
func (c *clientFS) deepestExistingParent(ctx context.Context, clean string) (*storage.NodeInfo, error) {
	for {
		parent := parentDir(clean)
		info, err := c.fs.Stat(ctx, parent)
		if err == nil {
			return info, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if parent == "/" {
			return nil, err
		}
		clean = parent
	}
}

func parentDir(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	dir := path.Dir(p)
	if dir == "" {
		return "/"
	}
	return dir
}

// errPathNoFile is the sentinel returned by the unused afero.File entry points.
var errPathNoFile = errors.New("storman/ftp: afero.File API not used — driver opens via GetHandle")

// nodeStat adapts a storage.NodeInfo to os.FileInfo for FTP listings.
type nodeStat struct {
	info storage.NodeInfo
}

func (n nodeStat) Name() string       { return n.info.Name }
func (n nodeStat) Size() int64        { return n.info.Size }
func (n nodeStat) ModTime() time.Time { return n.info.MTime }
func (n nodeStat) IsDir() bool        { return n.info.Type == storage.NodeDir }
func (n nodeStat) Sys() any           { return nil }
func (n nodeStat) Mode() os.FileMode {
	if n.info.Type == storage.NodeDir {
		return os.ModeDir | 0o750
	}
	return 0o640
}

// mapErr translates storage sentinels into the os errors ftpserverlib knows
// how to convert into FTP reply codes. Unknown errors pass through.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrNotFound):
		return os.ErrNotExist
	case errors.Is(err, storage.ErrExists):
		return os.ErrExist
	case errors.Is(err, storage.ErrInvalidPath):
		return fmt.Errorf("invalid path: %w", err)
	case errors.Is(err, rbac.ErrDenied):
		return os.ErrPermission
	default:
		return err
	}
}

