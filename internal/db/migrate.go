package db

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/alexnav/storman/internal/migrations"
)

// MigrateResult reports schema state before and after a migration run.
type MigrateResult struct {
	From    uint
	To      uint
	NoOp    bool
	HadDirt bool
}

// MigrateUp applies all pending migrations.
func MigrateUp(dsn string) (MigrateResult, error) {
	return runMigration(dsn, func(m *migrate.Migrate) error { return m.Up() })
}

// MigrateDown rolls back n migrations. n <= 0 is treated as 1.
func MigrateDown(dsn string, n int) (MigrateResult, error) {
	if n <= 0 {
		n = 1
	}
	return runMigration(dsn, func(m *migrate.Migrate) error { return m.Steps(-n) })
}

func runMigration(dsn string, action func(*migrate.Migrate) error) (MigrateResult, error) {
	src, err := iofs.New(migrations.FS(), ".")
	if err != nil {
		return MigrateResult{}, fmt.Errorf("iofs source: %w", err)
	}

	// golang-migrate accepts a pgx5 DSN via the "pgx5://" scheme.
	url, err := toPgx5URL(dsn)
	if err != nil {
		return MigrateResult{}, err
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return MigrateResult{}, fmt.Errorf("migrate init: %w", err)
	}
	defer m.Close()

	before, dirty, _ := m.Version()
	if dirty {
		return MigrateResult{From: before, HadDirt: true}, fmt.Errorf("schema is dirty at version %d — manual recovery required", before)
	}

	noOp := false
	if err := action(m); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			noOp = true
		} else {
			return MigrateResult{From: before}, err
		}
	}

	after, _, _ := m.Version()
	return MigrateResult{From: before, To: after, NoOp: noOp}, nil
}

// toPgx5URL rewrites a standard "postgres://" DSN to the "pgx5://" scheme
// that golang-migrate's pgx/v5 driver expects.
func toPgx5URL(dsn string) (string, error) {
	switch {
	case len(dsn) >= len("postgres://") && dsn[:len("postgres://")] == "postgres://":
		return "pgx5://" + dsn[len("postgres://"):], nil
	case len(dsn) >= len("postgresql://") && dsn[:len("postgresql://")] == "postgresql://":
		return "pgx5://" + dsn[len("postgresql://"):], nil
	case len(dsn) >= len("pgx5://") && dsn[:len("pgx5://")] == "pgx5://":
		return dsn, nil
	default:
		return "", fmt.Errorf("unsupported DSN scheme: %s", dsn)
	}
}
