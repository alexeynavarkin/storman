package dbfs

import (
	"errors"
	"strings"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// FuzzSplitPath feeds arbitrary strings to the public logical-path parser
// and asserts the basic safety properties:
//
//   - never panics;
//   - on a successful split, joinPath(segments) reproduces the input
//     (round-trip), and no segment is empty, ".", or "..";
//   - on rejection, the error wraps ErrInvalidPath.
//
// The parser is the single chokepoint between user-supplied paths and the
// rest of the storage layer; if anything ever leaks through, ltree / disk
// layout invariants downstream break.
func FuzzSplitPath(f *testing.F) {
	f.Add("/")
	f.Add("/a")
	f.Add("/a/b/c")
	f.Add("")
	f.Add("a")
	f.Add("/a/")
	f.Add("/./x")
	f.Add("/../x")
	f.Add("//double")
	f.Add("/\x00null")
	f.Add("/" + strings.Repeat("x", 4096))

	f.Fuzz(func(t *testing.T, in string) {
		segs, err := splitPath(in)
		if err != nil {
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Fatalf("splitPath(%q) returned non-ErrInvalidPath error: %v", in, err)
			}
			return
		}
		// segs may be empty (root). Anything non-empty must be a clean segment.
		for i, s := range segs {
			if s == "" || s == "." || s == ".." {
				t.Fatalf("splitPath(%q) yielded forbidden segment %q at %d", in, s, i)
			}
			if strings.Contains(s, "/") {
				t.Fatalf("splitPath(%q) yielded segment containing slash: %q", in, s)
			}
		}
		// Round-trip: joining back must reproduce the input verbatim.
		if got := joinPath(segs); got != in {
			t.Fatalf("round-trip mismatch: in=%q out=%q segs=%v", in, got, segs)
		}
	})
}
