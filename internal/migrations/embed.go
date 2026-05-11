// Package migrations exposes the embedded SQL migration files.
package migrations

import (
	"embed"
	"io/fs"
)

//go:embed sql/*.sql
var sqlFS embed.FS

// FS returns the migration files rooted at the directory containing the *.sql files.
func FS() fs.FS {
	sub, err := fs.Sub(sqlFS, "sql")
	if err != nil {
		// Embedded path is constant — failure here is a programmer error.
		panic(err)
	}
	return sub
}
