// Package testpg provides a shared helper for integration tests that need a
// real PostgreSQL. Tests are skipped unless TEST_POSTGRES_DSN is set.
//
// Each Pool(t) returns a fresh, isolated database cloned from a process-wide
// template via `CREATE DATABASE … TEMPLATE …`. Migrations run once per test
// binary (against the template), so subsequent Pool calls cost a few
// milliseconds. Tests can therefore use t.Parallel() and the test runner can
// drop `-p 1`.
package testpg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/db"
)

const envVar = "TEST_POSTGRES_DSN"

// adminDB is the database used for CREATE/DROP DATABASE statements. Postgres
// requires those commands to run against some database that is not the target.
const adminDB = "postgres"

// dbNamePrefix is the common prefix used for both the per-process template
// and per-test clones — leftovers can be pruned by name on the next run.
const dbNamePrefix = "storman_test_"

var (
	templateOnce sync.Once
	templateErr  error
	templateName string
	adminDSN     string
	baseURL      *url.URL

	// createMu serialises CREATE DATABASE … TEMPLATE within a process. Postgres
	// refuses to clone a template that has active sessions, and parallel
	// CREATEs occasionally race against transient autovacuum / stats workers
	// holding momentary handles. The lock is held only for the duration of
	// that single statement.
	createMu sync.Mutex
)

// Pool returns a fresh pgxpool connected to a unique per-test database, cloned
// from a process-shared template that was migrated once. The pool and the
// database are torn down on t.Cleanup. Skips the test if TEST_POSTGRES_DSN
// is unset.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, cleanup, err := acquireDB()
	if err != nil {
		if err == errSkip {
			t.Skipf("set %s to a postgres DSN to run integration tests", envVar)
		}
		t.Fatalf("acquire test db: %v", err)
	}
	t.Cleanup(cleanup)
	return pool
}

// AcquireDB is the TB-agnostic counterpart to Pool, returning the pool and an
// explicit cleanup function. Useful inside property tests where the harness
// (rapid) does not expose *testing.T and helper utilities need to register
// their own cleanups. Returns (nil, nil, ErrSkip) when TEST_POSTGRES_DSN is
// unset so callers can decide how to react.
func AcquireDB() (*pgxpool.Pool, func(), error) {
	return acquireDB()
}

// ErrSkip mirrors the t.Skip code path Pool uses; callers of AcquireDB may
// treat it as "no test DB configured; skip me".
var ErrSkip = errSkip

var errSkip = errSkipSentinel{}

type errSkipSentinel struct{}

func (errSkipSentinel) Error() string { return "TEST_POSTGRES_DSN not set" }

func acquireDB() (*pgxpool.Pool, func(), error) {
	dsn := os.Getenv(envVar)
	if dsn == "" {
		return nil, nil, errSkip
	}
	if err := ensureTemplate(dsn); err != nil {
		return nil, nil, fmt.Errorf("ensure template: %w", err)
	}
	testDB := dbNamePrefix + randHex(8)
	if err := createFromTemplate(testDB); err != nil {
		return nil, nil, fmt.Errorf("create test db %q: %w", testDB, err)
	}
	pool, err := db.NewPool(context.Background(), withDatabase(baseURL, testDB))
	if err != nil {
		_ = dropDB(testDB)
		return nil, nil, fmt.Errorf("open pool: %w", err)
	}
	cleanup := func() {
		pool.Close()
		_ = dropDB(testDB)
	}
	return pool, cleanup, nil
}

func ensureTemplate(dsn string) error {
	templateOnce.Do(func() {
		u, err := url.Parse(dsn)
		if err != nil {
			templateErr = fmt.Errorf("parse dsn: %w", err)
			return
		}
		baseURL = u
		adminDSN = withDatabase(u, adminDB)

		// Each test binary gets its own template database, name keyed by PID
		// and random suffix to avoid colliding with concurrent `go test`
		// invocations targeting the same Postgres. Leftover databases from
		// crashed runs are intentionally NOT pruned here — pruning concurrently
		// with another running test process would race against its template.
		// Run `make pg-clean` to drop accumulated leaks.
		templateName = fmt.Sprintf("%stemplate_%d_%s", dbNamePrefix, os.Getpid(), randHex(4))
		if err := createDB(templateName); err != nil {
			templateErr = fmt.Errorf("create template db: %w", err)
			return
		}
		if _, err := db.MigrateUp(withDatabase(u, templateName)); err != nil {
			// Leave the failed template in place — pruneStale on the next run
			// will collect it. We can't reliably reuse it for a retry inside
			// this process anyway.
			templateErr = fmt.Errorf("migrate template: %w", err)
			return
		}
	})
	return templateErr
}

func createDB(name string) error {
	return execAdmin(fmt.Sprintf(`CREATE DATABASE %s`, quoteIdent(name)))
}

func createFromTemplate(name string) error {
	createMu.Lock()
	defer createMu.Unlock()
	return execAdmin(fmt.Sprintf(
		`CREATE DATABASE %s TEMPLATE %s`,
		quoteIdent(name), quoteIdent(templateName),
	))
}

func dropDB(name string) error {
	// Kick any lingering connections; ignore the return value.
	_ = execAdmin(fmt.Sprintf(
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()`,
		quoteLit(name),
	))
	return execAdmin(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, quoteIdent(name)))
}

func execAdmin(sqlText string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("admin connect: %w", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, sqlText); err != nil {
		return fmt.Errorf("admin exec: %w", err)
	}
	return nil
}

func withDatabase(u *url.URL, dbname string) string {
	clone := *u
	clone.Path = "/" + dbname
	return clone.String()
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func randHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
