package dbfs_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/alexnav/storman/internal/db/testpg"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
	"github.com/alexnav/storman/internal/storage/storagetest"
)

// fsModel is an in-memory model of the FileSystem tree used as an oracle for
// property-based testing. Trash is intentionally NOT modelled — the rapid
// commands here exercise live-tree operations only.
type fsModel struct {
	nodes map[string]modelNode
}

type modelNode struct {
	isDir bool
	bytes []byte
}

func newModel() *fsModel { return &fsModel{nodes: map[string]modelNode{}} }

func (m *fsModel) exists(p string) bool {
	if p == "/" {
		return true
	}
	_, ok := m.nodes[p]
	return ok
}

func (m *fsModel) isDir(p string) bool {
	if p == "/" {
		return true
	}
	n, ok := m.nodes[p]
	return ok && n.isDir
}

func (m *fsModel) parentOf(p string) string {
	parent := path.Dir(p)
	if parent == "." {
		return "/"
	}
	return parent
}

func (m *fsModel) descendants(p string) []string {
	var out []string
	prefix := strings.TrimSuffix(p, "/") + "/"
	for k := range m.nodes {
		if k == p || strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// nameGen is intentionally tiny so collisions are likely.
var nameGen = rapid.SampledFrom([]string{"a", "b", "c", "x", "y", "z", "node", "leaf", "doc.txt", "file.bin", "img.jpg"})

func drawPath(t *rapid.T, model *fsModel, allowRoot bool) string {
	existing := []string{"/"}
	for k := range model.nodes {
		existing = append(existing, k)
	}
	sort.Strings(existing)

	if rapid.IntRange(0, 9).Draw(t, "ex") < 7 && len(existing) > 0 {
		idx := rapid.IntRange(0, len(existing)-1).Draw(t, "idx")
		p := existing[idx]
		if !allowRoot && p == "/" {
			p = "/" + nameGen.Draw(t, "leaf")
		}
		return p
	}

	var dirs []string
	for _, e := range existing {
		if model.isDir(e) {
			dirs = append(dirs, e)
		}
	}
	parent := dirs[rapid.IntRange(0, len(dirs)-1).Draw(t, "parent")]
	leaf := nameGen.Draw(t, "leaf")
	if parent == "/" {
		return "/" + leaf
	}
	return parent + "/" + leaf
}

// propertyEnv holds the per-iteration state for a rapid property run: a fresh
// DB, a fresh on-disk layout, and a fully bootstrapped FS. cleanup() releases
// all of those; the property body registers it via rapid.T.Cleanup.
type propertyEnv struct {
	fs     *dbfs.DBFS
	pool   interface {
		// only the consistency checker needs the pool here; we use the
		// concrete *pgxpool.Pool via storagetest.CheckConsistent below.
	}
	layout storagetest.Layout
}

func TestPropertyStateMachine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property test in -short mode")
	}
	if _, _, err := testpg.AcquireDB(); err != nil {
		// Drop one acquisition just to honour the env-var skip semantics.
		if errors.Is(err, testpg.ErrSkip) {
			t.Skipf("set TEST_POSTGRES_DSN to a postgres DSN to run integration tests")
		}
		t.Fatalf("acquire probe: %v", err)
	}

	rapid.Check(t, func(t *rapid.T) {
		pool, dbCleanup, err := testpg.AcquireDB()
		if err != nil {
			t.Fatalf("acquire db: %v", err)
		}
		t.Cleanup(dbCleanup)

		root, err := os.MkdirTemp("", "storman-property-*")
		if err != nil {
			t.Fatalf("temp dir: %v", err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(root) })

		layout := storagetest.Layout{
			StorageDir: filepath.Join(root, "flat-storage"),
			UploadsDir: filepath.Join(root, "meta-storage", "uploads"),
			TrashDir:   filepath.Join(root, "meta-storage", "trash"),
		}
		for _, d := range []string{layout.StorageDir, layout.UploadsDir, layout.TrashDir} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				t.Fatalf("mkdir %s: %v", d, err)
			}
		}

		backend := flat.New(layout.StorageDir, layout.UploadsDir)
		fs := dbfs.New(pool, layout.TrashDir, backend)
		ctx := context.Background()
		if _, err := fs.Bootstrap(ctx); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
		model := newModel()

		check := func() {
			if problems := storagetest.CheckConsistent(pool, layout); len(problems) > 0 {
				t.Fatalf("consistency violations:\n%s", strings.Join(problems, "\n"))
			}
		}

		t.Repeat(map[string]func(*rapid.T){
			"mkdir": func(t *rapid.T) {
				p := drawPath(t, model, false)
				err := fs.Mkdir(ctx, p, storage.MkdirOpts{})
				switch {
				case model.exists(p):
					if !errors.Is(err, storage.ErrExists) {
						t.Fatalf("Mkdir(%q): want ErrExists, got %v", p, err)
					}
				case !model.isDir(model.parentOf(p)):
					if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrInvalidPath) {
						t.Fatalf("Mkdir(%q): want ErrNotFound/ErrInvalidPath (no parent), got %v", p, err)
					}
				default:
					if err != nil {
						t.Fatalf("Mkdir(%q): unexpected %v", p, err)
					}
					model.nodes[p] = modelNode{isDir: true}
				}
			},
			"write": func(t *rapid.T) {
				p := drawPath(t, model, false)
				data := []byte("data-for-" + p)
				err := propWriteFile(fs, p, data)
				switch {
				case model.exists(p):
					if !errors.Is(err, storage.ErrExists) {
						t.Fatalf("Write(%q): want ErrExists, got %v", p, err)
					}
				case !model.isDir(model.parentOf(p)):
					if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrInvalidPath) {
						t.Fatalf("Write(%q): want ErrNotFound/ErrInvalidPath, got %v", p, err)
					}
				default:
					if err != nil {
						t.Fatalf("Write(%q): unexpected %v", p, err)
					}
					model.nodes[p] = modelNode{bytes: data}
				}
			},
			"read": func(t *rapid.T) {
				p := drawPath(t, model, true)
				r, err := fs.OpenRead(ctx, p)
				if err != nil {
					if n, ok := model.nodes[p]; ok && !n.isDir {
						t.Fatalf("Read(%q): want success (model has file), got %v", p, err)
					}
					return
				}
				defer r.Close()
				buf := make([]byte, r.Size())
				if _, err := r.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("Read(%q): ReadAt failed: %v", p, err)
				}
				want, ok := model.nodes[p]
				if !ok || want.isDir {
					t.Fatalf("Read(%q): success but model has no file at this path", p)
				}
				if string(buf) != string(want.bytes) {
					t.Fatalf("Read(%q): content mismatch:\n  real:  %q\n  model: %q", p, buf, want.bytes)
				}
			},
			"remove": func(t *rapid.T) {
				p := drawPath(t, model, false)
				err := fs.Remove(ctx, p)
				switch {
				case !model.exists(p):
					if !errors.Is(err, storage.ErrNotFound) {
						t.Fatalf("Remove(%q): want ErrNotFound, got %v", p, err)
					}
				default:
					if err != nil {
						t.Fatalf("Remove(%q): unexpected %v", p, err)
					}
					for _, k := range model.descendants(p) {
						delete(model.nodes, k)
					}
				}
			},
			"stat": func(t *rapid.T) {
				p := drawPath(t, model, true)
				info, err := fs.Stat(ctx, p)
				if !model.exists(p) {
					if !errors.Is(err, storage.ErrNotFound) {
						t.Fatalf("Stat(%q): want ErrNotFound, got info=%+v err=%v", p, info, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Stat(%q): unexpected %v", p, err)
				}
				wantDir := model.isDir(p)
				gotDir := info.Type == storage.NodeDir
				if wantDir != gotDir {
					t.Fatalf("Stat(%q): type mismatch — model dir=%v real dir=%v", p, wantDir, gotDir)
				}
			},
			"": func(t *rapid.T) { check() },
		})
	})
}

// propWriteFile mirrors writeFileNoFatal but doesn't depend on the in-package
// fixture struct so it can be shared by the stress test.
func propWriteFile(fs *dbfs.DBFS, path string, data []byte) error {
	w, err := fs.OpenWrite(context.Background(), path, storage.WriteOpts{Mode: storage.WriteCreate})
	if err != nil {
		return err
	}
	if _, err := w.WriteAt(data, 0); err != nil {
		_ = w.Abort()
		_ = w.Close()
		return err
	}
	if err := w.Commit(); err != nil {
		_ = w.Abort()
		_ = w.Close()
		return err
	}
	return w.Close()
}
