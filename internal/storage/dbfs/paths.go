package dbfs

import (
	"fmt"
	"strings"

	"github.com/alexnav/storman/internal/storage"
)

// splitPath validates and splits a logical absolute path into segments.
//
//	"/"               → []          (root)
//	"/a"              → ["a"]
//	"/a/b/c"          → ["a","b","c"]
//
// The input must start with '/', must not contain "", ".", or ".." segments,
// and must not have a trailing slash (except for the root itself).
func splitPath(p string) ([]string, error) {
	if p == "" || p[0] != '/' {
		return nil, fmt.Errorf("%w: path must be absolute", storage.ErrInvalidPath)
	}
	if p == "/" {
		return nil, nil
	}
	if strings.HasSuffix(p, "/") {
		return nil, fmt.Errorf("%w: trailing slash", storage.ErrInvalidPath)
	}
	parts := strings.Split(p[1:], "/")
	for _, part := range parts {
		switch part {
		case "":
			return nil, fmt.Errorf("%w: empty segment", storage.ErrInvalidPath)
		case ".":
			return nil, fmt.Errorf("%w: current-dir segment", storage.ErrInvalidPath)
		case "..":
			return nil, fmt.Errorf("%w: parent traversal", storage.ErrInvalidPath)
		}
	}
	return parts, nil
}

// joinPath rebuilds a logical path from segments. nil/empty returns "/".
func joinPath(segments []string) string {
	if len(segments) == 0 {
		return "/"
	}
	return "/" + strings.Join(segments, "/")
}
