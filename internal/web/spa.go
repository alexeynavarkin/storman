package web

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// SPAFromDir builds an http.Handler that serves a single-page application
// from the on-disk directory dir. The SPA-fallback behaviour: asset files
// served as-is; anything else (deep links into the SPA's client-side router)
// returns dir/index.html with a 200 status.
//
// Requests under /api/* are never reached here — the router checks them first.
func SPAFromDir(dir string) (http.Handler, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("ui-dir is not a directory")
	}
	if _, err := os.Stat(filepath.Join(dir, "index.html")); err != nil {
		return nil, errors.New("ui-dir does not contain index.html — did you run 'npm run build'?")
	}
	return SPAFromFS(os.DirFS(dir)), nil
}

// SPAFromFS is the shared SPA handler that works with any fs.FS — os.DirFS
// for `--ui-dir`, or embed.FS for the in-binary build. Asset files (anything
// fs.Stat resolves) are served as-is via http.FileServer; anything else
// falls back to index.html so the SPA's client-side router can take over.
func SPAFromFS(root fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := path.Clean(r.URL.Path)
		rel := strings.TrimPrefix(clean, "/")
		if rel == "" {
			serveIndexFS(w, root)
			return
		}
		if info, err := fs.Stat(root, rel); err == nil && !info.IsDir() {
			fileServer.ServeHTTP(w, r)
			return
		}
		serveIndexFS(w, root)
	})
}

// serveIndexFS writes index.html with no-cache headers; hashed asset files
// next to it are still cached aggressively by http.FileServer's ETag flow.
func serveIndexFS(w http.ResponseWriter, root fs.FS) {
	f, err := root.Open("index.html")
	if err != nil {
		http.Error(w, "index.html not available", http.StatusInternalServerError)
		return
	}
	defer f.Close()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Buffer so http.ServeContent can do ETag/range — but for a small shell
	// we just copy. The cost is dwarfed by template hydration on the client.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		http.Error(w, "failed to read index.html", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Length", itoa(buf.Len()))
	_, _ = w.Write(buf.Bytes())
}

func itoa(n int) string {
	// Minimal int→string; avoiding strconv keeps this file dep-free.
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
