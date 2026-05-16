// Package dbfs implements storage.FileSystem backed by the PostgreSQL nodes
// table. It dispatches file content I/O to the appropriate FileBackend (see
// internal/storage). Tree operations (Stat/List/Mkdir/Rename/Remove) are owned
// here; content lifecycle (OpenWrite/OpenRead) coordinates with the outbox
// for atomicity between disk and DB — see docs/arch/storage.md and ADR-0002.
package dbfs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/storage"
)

// querier abstracts the subset of pgx that dbfs uses, so callers can pass
// either a pool or a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// DBFS is the single FileSystem implementation. Holds the connection pool and
// a registry of FileBackends keyed by their Name().
type DBFS struct {
	pool     *pgxpool.Pool
	backends map[string]storage.FileBackend
	// defaultBackendKind is the kind used when no ancestor directory carries
	// a policy. Defaults to "flat" per docs/arch/storage.md.
	defaultBackendKind string
	// trashDir is <data-dir>/meta-storage/trash; subtrees being soft-deleted
	// are renamed into trashDir/<trash_uuid>/payload by the trash executor.
	trashDir string
	// hook is the test-only fault-injection callback (see hooks.go). nil in
	// production — the fireHook fast path is a single nil check.
	hook HookFn
}

// New constructs a DBFS. The provided backends are registered by their Name();
// at least one must match defaultBackendKind ("flat") or the FS will reject
// content operations. trashDir is the absolute path of
// <data-dir>/meta-storage/trash where soft-deleted content is parked.
func New(pool *pgxpool.Pool, trashDir string, backends ...storage.FileBackend) *DBFS {
	reg := make(map[string]storage.FileBackend, len(backends))
	for _, b := range backends {
		reg[b.Name()] = b
	}
	return &DBFS{
		pool:               pool,
		backends:           reg,
		defaultBackendKind: "flat",
		trashDir:           trashDir,
	}
}

// TrashDir reports the absolute path of the trash root configured for this
// FS. Exposed for the GC worker (it walks trashDir to find entries past
// retention) and for the trash HTTP endpoints (listing/restore/purge).
func (fs *DBFS) TrashDir() string { return fs.trashDir }

// Compile-time assertion that DBFS satisfies the FileSystem interface (some
// methods are stubs for now and will be filled in a follow-up slice).
var _ storage.FileSystem = (*DBFS)(nil)

// ltreeLabel renders a UUID as an ltree-safe label: 32 hex chars, no dashes.
func ltreeLabel(id uuid.UUID) string {
	return strings.ReplaceAll(id.String(), "-", "")
}

// ltreeAppend appends a label to an ltree path: "a.b" + "c" → "a.b.c".
func ltreeAppend(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// errf wraps a sentinel error with formatted context.
func errf(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{sentinel}, args...)...)
}

// isUniqueViolation reports whether err is a Postgres unique_violation
// (SQLSTATE 23505). The partial unique index on nodes(parent_id, name)
// fires when two concurrent operations race for the same logical path
// past the in-tx childNode check; callers translate that into ErrExists
// so the FS surfaces a single canonical signal.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
