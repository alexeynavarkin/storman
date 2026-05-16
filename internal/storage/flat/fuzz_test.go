package flat

import (
	"errors"
	"strings"
	"testing"

	"github.com/alexnav/storman/internal/storage"
)

// FuzzCleanRel hammers the relative-path validator in flat.Backend. The
// invariants this test guards:
//
//   - never panics for any input;
//   - on success, the returned path is byte-equal to the input (cleanRel
//     refuses to normalise silently — a faithful echo lets backend_ref stay
//     a literal carbon copy of the logical path);
//   - on success, the result contains no "/./", "/../", leading or trailing
//     slash, or empty segment;
//   - on failure, the error wraps ErrInvalidPath.
//
// This is the last line of defence against path-traversal — flat resolves
// every BackendRef.Data through cleanRel before touching disk, so anything
// that slips past here can escape the data root.
func FuzzCleanRel(f *testing.F) {
	f.Add("a.txt")
	f.Add("photos/2024/01.jpg")
	f.Add("")
	f.Add("/abs")
	f.Add("a/")
	f.Add("a//b")
	f.Add("./x")
	f.Add("../x")
	f.Add("a/../escape")
	f.Add("\x00")
	f.Add(strings.Repeat("seg/", 1000) + "leaf")

	f.Fuzz(func(t *testing.T, in string) {
		out, err := cleanRel(in)
		if err != nil {
			if !errors.Is(err, storage.ErrInvalidPath) {
				t.Fatalf("cleanRel(%q): unexpected error type: %v", in, err)
			}
			return
		}
		if out != in {
			t.Fatalf("cleanRel(%q) accepted but rewrote to %q (must be byte-identical on success)", in, out)
		}
		if strings.HasPrefix(out, "/") || strings.HasSuffix(out, "/") {
			t.Fatalf("cleanRel(%q) accepted leading/trailing slash: %q", in, out)
		}
		for _, seg := range strings.Split(out, "/") {
			if seg == "" || seg == "." || seg == ".." {
				t.Fatalf("cleanRel(%q) accepted forbidden segment %q", in, seg)
			}
		}
	})
}
