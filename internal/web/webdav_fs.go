package web

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/net/webdav"

	"github.com/alexnav/storman/internal/audit"
	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/rbac"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

// davFS adapts dbfs.DBFS to webdav.FileSystem. Every method:
//   1. normalizes the request path to an absolute storman path,
//   2. resolves the target (or parent, for create-style ops) via fs.Stat,
//   3. enforces ACL with PermissionService.Check,
//   4. delegates to dbfs and maps errors to os.* sentinels so the webdav
//      package can emit the right HTTP status (403/404/409).
//
// Audit events for mutations carry Details["channel"]="webdav" so an admin
// can filter the audit log by surface.
type davFS struct {
	fs      *dbfs.DBFS
	perms   *rbac.PermissionService
	auditFn func(context.Context, audit.Event)
}

// normalize ensures a leading slash and collapses "." / "..".
func normalize(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

// callerFromCtx pulls the authenticated user attached by davAuth. Returns
// the zero-value user if there is none — the caller must check ok.
func callerFromCtx(ctx context.Context) (*auth.User, bool) {
	u := UserFromContext(ctx)
	return u, u != nil
}

// mapStorageErr translates storage.* errors into the os.* sentinels the
// webdav package recognizes for HTTP status mapping. rbac.ErrDenied → 403,
// storage.ErrNotFound → 404, storage.ErrExists → 409.
func mapStorageErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, rbac.ErrDenied):
		return os.ErrPermission
	case errors.Is(err, storage.ErrNotFound):
		return os.ErrNotExist
	case errors.Is(err, storage.ErrExists):
		return os.ErrExist
	default:
		return err
	}
}

func uuidPtr(id uuid.UUID) *uuid.UUID { return &id }

func (d *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	u, ok := callerFromCtx(ctx)
	if !ok {
		return nil, os.ErrPermission
	}
	p := normalize(name)
	info, err := d.fs.Stat(ctx, p)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if err := d.perms.Check(ctx, u.ID, info.ID, rbac.Read); err != nil {
		return nil, mapStorageErr(err)
	}
	return toFileInfo(*info), nil
}

func (d *davFS) Mkdir(ctx context.Context, name string, _ fs.FileMode) error {
	u, ok := callerFromCtx(ctx)
	if !ok {
		return os.ErrPermission
	}
	p := normalize(name)
	parent, err := d.fs.Stat(ctx, parentOf(p))
	if err != nil {
		return mapStorageErr(err)
	}
	if err := d.perms.Check(ctx, u.ID, parent.ID, rbac.Write); err != nil {
		return mapStorageErr(err)
	}
	if err := d.fs.Mkdir(ctx, p, storage.MkdirOpts{}); err != nil {
		return mapStorageErr(err)
	}
	d.auditFn(ctx, audit.Event{
		UserID:  uuidPtr(u.ID),
		Action:  audit.ActionMkdir,
		NodeID:  &parent.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": p, "channel": "webdav"},
	})
	return nil
}

func (d *davFS) RemoveAll(ctx context.Context, name string) error {
	u, ok := callerFromCtx(ctx)
	if !ok {
		return os.ErrPermission
	}
	p := normalize(name)
	if p == "/" {
		return os.ErrPermission
	}
	info, err := d.fs.Stat(ctx, p)
	if err != nil {
		return mapStorageErr(err)
	}
	if err := d.perms.Check(ctx, u.ID, info.ID, rbac.Remove); err != nil {
		return mapStorageErr(err)
	}
	if err := d.fs.Remove(storage.WithActor(ctx, u.ID), p); err != nil {
		return mapStorageErr(err)
	}
	d.auditFn(ctx, audit.Event{
		UserID:  uuidPtr(u.ID),
		Action:  audit.ActionDelete,
		NodeID:  &info.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": p, "type": string(info.Type), "channel": "webdav"},
	})
	return nil
}

func (d *davFS) Rename(ctx context.Context, oldName, newName string) error {
	u, ok := callerFromCtx(ctx)
	if !ok {
		return os.ErrPermission
	}
	src := normalize(oldName)
	dst := normalize(newName)
	if src == "/" || dst == "/" {
		return os.ErrPermission
	}
	srcInfo, err := d.fs.Stat(ctx, src)
	if err != nil {
		return mapStorageErr(err)
	}
	if err := d.perms.Check(ctx, u.ID, srcInfo.ID, rbac.Write); err != nil {
		return mapStorageErr(err)
	}
	dstParent, err := d.fs.Stat(ctx, parentOf(dst))
	if err != nil {
		return mapStorageErr(err)
	}
	if err := d.perms.Check(ctx, u.ID, dstParent.ID, rbac.Write); err != nil {
		return mapStorageErr(err)
	}
	if err := d.fs.Rename(ctx, src, dst); err != nil {
		return mapStorageErr(err)
	}
	d.auditFn(ctx, audit.Event{
		UserID:  uuidPtr(u.ID),
		Action:  audit.ActionRename,
		NodeID:  &srcInfo.ID,
		Result:  audit.ResultOK,
		Details: map[string]any{"from": src, "to": dst, "channel": "webdav"},
	})
	return nil
}

func (d *davFS) OpenFile(ctx context.Context, name string, flag int, _ fs.FileMode) (webdav.File, error) {
	u, ok := callerFromCtx(ctx)
	if !ok {
		return nil, os.ErrPermission
	}
	p := normalize(name)
	// webdav.Handler opens files for PUT with O_RDWR|O_CREATE|O_TRUNC even
	// though it never reads from the handle — Go's File interface demands
	// both halves. Treat any of WRONLY/RDWR/CREATE as a write intent; pure
	// O_RDONLY (or no flags) is a read.
	writing := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0

	if !writing {
		info, err := d.fs.Stat(ctx, p)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		if err := d.perms.Check(ctx, u.ID, info.ID, rbac.Read); err != nil {
			return nil, mapStorageErr(err)
		}
		if info.Type == storage.NodeDir {
			entries, err := d.fs.List(ctx, p)
			if err != nil {
				return nil, mapStorageErr(err)
			}
			items := make([]os.FileInfo, 0, len(entries))
			for _, e := range entries {
				items = append(items, toFileInfo(e))
			}
			return &davDir{info: *info, items: items}, nil
		}
		reader, err := d.fs.OpenRead(ctx, p)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		return &davReader{r: reader, info: *info}, nil
	}

	// Write path. dbfs.OpenWrite supports WriteCreate only — overwrite of an
	// existing file is implemented here as trash-then-create. The old version
	// lands in trash (recoverable through the admin UI) and a fresh node is
	// inserted at the same path.
	existing, statErr := d.fs.Stat(ctx, p)
	switch {
	case statErr == nil:
		if flag&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
		if existing.Type == storage.NodeDir {
			return nil, os.ErrInvalid
		}
		if err := d.perms.Check(ctx, u.ID, existing.ID, rbac.Write); err != nil {
			return nil, mapStorageErr(err)
		}
		if err := d.fs.Remove(storage.WithActor(ctx, u.ID), p); err != nil {
			return nil, mapStorageErr(err)
		}
	case errors.Is(statErr, storage.ErrNotFound):
		parent, perr := d.fs.Stat(ctx, parentOf(p))
		if perr != nil {
			return nil, mapStorageErr(perr)
		}
		if err := d.perms.Check(ctx, u.ID, parent.ID, rbac.Write); err != nil {
			return nil, mapStorageErr(err)
		}
	default:
		return nil, mapStorageErr(statErr)
	}

	writer, err := d.fs.OpenWrite(storage.WithActor(ctx, u.ID), p, storage.WriteOpts{
		Mode:         storage.WriteCreate,
		ExpectedSize: -1,
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	d.auditFn(ctx, audit.Event{
		UserID:  uuidPtr(u.ID),
		Action:  audit.ActionUpload,
		Result:  audit.ResultOK,
		Details: map[string]any{"path": p, "channel": "webdav"},
	})
	return &davWriter{
		w:    writer,
		info: storage.NodeInfo{Name: pathBase(p), Path: p, Type: storage.NodeFile},
	}, nil
}

// pathBase returns the final element of p — same as path.Base, inlined here
// so the file doesn't accidentally shadow the path package when both are used.
func pathBase(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}
