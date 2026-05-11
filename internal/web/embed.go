package web

import (
	"embed"
	"io/fs"
)

//go:embed all:embedded
var embeddedAssets embed.FS

// EmbeddedSPA returns the SPA assets compiled into the binary. The Makefile
// copies the Vite build output into internal/web/embedded/ before `go build`,
// so on a freshly built binary this contains the production SPA.
//
// Returns (fs, true) when index.html exists (i.e. a real build), or
// (nil, false) on a fresh clone where only placeholder.html is staged —
// callers fall back to API-only mode or honour --ui-dir.
func EmbeddedSPA() (fs.FS, bool) {
	sub, err := fs.Sub(embeddedAssets, "embedded")
	if err != nil {
		return nil, false
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil, false
	}
	return sub, true
}
