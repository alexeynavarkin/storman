// Package testpg provides a shared helper for integration tests that need a
// real PostgreSQL. Tests are skipped unless TEST_POSTGRES_DSN is set.
//
// The helper drops and recreates the `public` schema on each call, then runs
// migrations from scratch. Tests therefore cannot run in parallel against the
// same DSN — use `go test -p 1` or point each package at a distinct DB.
package testpg

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexnav/storman/internal/db"
)

const envVar = "TEST_POSTGRES_DSN"

// Pool returns a fresh pgxpool connected to the test database. The pool is
// closed automatically on test cleanup. Skips the test if envVar is unset.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(envVar)
	if dsn == "" {
		t.Skipf("set %s to a postgres DSN to run integration tests", envVar)
	}
	if err := resetSchema(dsn); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	if _, err := db.MigrateUp(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := db.NewPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func resetSchema(dsn string) error {
	ctx := context.Background()
	pool, err := db.NewPool(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;`)
	return err
}
