package storagetest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Layout describes the on-disk dirs that AssertConsistent walks. Mirrors the
// pair passed to `flat.New(storageDir, uploadsDir)` plus the trash dir that
// `dbfs.New` configures.
type Layout struct {
	StorageDir string // <data-dir>/flat-storage
	UploadsDir string // <data-dir>/meta-storage/uploads
	TrashDir   string // <data-dir>/meta-storage/trash
}

// AssertConsistent walks the DB and the on-disk layout and asserts the
// durability invariants of the storage layer. Wrapper around CheckConsistent
// that fails the test on any violation. Use CheckConsistent directly from
// property/state-machine tests where the test framework owns reporting.
//
// Invariants checked:
//
//   (R1) Every ready file node has a corresponding blob on disk of the right size.
//   (R2) No orphan regular files under StorageDir.
//   (R3) No 'create_file' outbox row whose node is not 'pending'.
//   (R4) Every ltree-recorded child path equals parent.path || label(child.id).
//   (R5) Every trash directory <TrashDir>/<uuid>/ has a payload/ subtree.
func AssertConsistent(t testing.TB, pool *pgxpool.Pool, layout Layout) {
	t.Helper()
	problems := CheckConsistent(pool, layout)
	if len(problems) == 0 {
		return
	}
	sort.Strings(problems)
	t.Fatalf("storage consistency check failed (%d problem(s)):\n%s",
		len(problems), strings.Join(problems, "\n"))
}

// CheckConsistent runs the invariant scan and returns the collected
// violations. nil means everything is consistent. Errors that prevent the
// scan from running (e.g. a SQL query fails) are returned as a single
// "FATAL: …" entry — callers should treat the presence of any entry as a
// failure.
func CheckConsistent(pool *pgxpool.Pool, layout Layout) []string {
	ctx := context.Background()

	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, indent(formatf(format, args...)))
	}
	fatal := func(format string, args ...any) {
		problems = append(problems, indent("FATAL: "+formatf(format, args...)))
	}

	// --- (R1) ready file nodes vs disk -------------------------------------
	type fileNode struct {
		ID          string
		Name        string
		BackendKind string
		BackendRef  string
		Size        int64
	}
	var ready []fileNode
	rows, err := pool.Query(ctx, `
		SELECT id::text, name, COALESCE(backend_kind,''), COALESCE(backend_ref,''), COALESCE(size, -1)
		FROM nodes
		WHERE type='file' AND status='ready'`)
	if err != nil {
		fatal("query ready files: %v", err)
		return problems
	}
	for rows.Next() {
		var n fileNode
		if err := rows.Scan(&n.ID, &n.Name, &n.BackendKind, &n.BackendRef, &n.Size); err != nil {
			rows.Close()
			fatal("scan: %v", err)
			return problems
		}
		ready = append(ready, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		fatal("rows err: %v", err)
		return problems
	}

	expected := make(map[string]int64, len(ready))
	for _, n := range ready {
		if n.BackendKind != "flat" {
			continue
		}
		if n.BackendRef == "" {
			add("ready file node %s (name=%q) has empty backend_ref", n.ID, n.Name)
			continue
		}
		abs := filepath.Join(layout.StorageDir, filepath.FromSlash(n.BackendRef))
		expected[abs] = n.Size

		info, err := os.Stat(abs)
		if err != nil {
			add("ready file node %s expects blob at %s but Stat failed: %v", n.ID, abs, err)
			continue
		}
		if info.IsDir() {
			add("ready file node %s expects regular file at %s but it is a directory", n.ID, abs)
			continue
		}
		if n.Size >= 0 && info.Size() != n.Size {
			add("ready file node %s size mismatch: db=%d disk=%d (path=%s)", n.ID, n.Size, info.Size(), abs)
		}
	}

	// --- (R2) orphan blobs under StorageDir --------------------------------
	if layout.StorageDir != "" {
		if err := filepath.Walk(layout.StorageDir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if _, ok := expected[p]; !ok {
				add("orphan blob on disk: %s (not referenced by any ready file node)", p)
			}
			return nil
		}); err != nil && !os.IsNotExist(err) {
			fatal("walk %s: %v", layout.StorageDir, err)
			return problems
		}
	}

	// --- (R3) outbox / node-status invariant -------------------------------
	stuckRows, err := pool.Query(ctx, `
		SELECT o.id, o.op, COALESCE(n.status::text, '<no-node>')
		FROM outbox o LEFT JOIN nodes n ON n.id = o.node_id
		WHERE o.op = 'create_file' AND COALESCE(n.status::text,'') <> 'pending'`)
	if err != nil {
		fatal("query outbox: %v", err)
		return problems
	}
	for stuckRows.Next() {
		var id int64
		var op, status string
		if err := stuckRows.Scan(&id, &op, &status); err != nil {
			stuckRows.Close()
			fatal("scan outbox: %v", err)
			return problems
		}
		add("outbox row %d (op=%s) refers to node in non-pending status %q (recovery did not archive)",
			id, op, status)
	}
	stuckRows.Close()

	// --- (R4) ltree path consistency ---------------------------------------
	pathRows, err := pool.Query(ctx, `
		SELECT c.id::text, c.path::text, p.path::text
		FROM nodes c JOIN nodes p ON c.parent_id = p.id`)
	if err != nil {
		fatal("query ltree: %v", err)
		return problems
	}
	for pathRows.Next() {
		var cid, cpath, ppath string
		if err := pathRows.Scan(&cid, &cpath, &ppath); err != nil {
			pathRows.Close()
			fatal("scan ltree: %v", err)
			return problems
		}
		label := strings.ReplaceAll(cid, "-", "")
		want := label
		if ppath != "" {
			want = ppath + "." + label
		}
		if cpath != want {
			add("ltree mismatch for node %s: path=%q expected %q", cid, cpath, want)
		}
	}
	pathRows.Close()

	// --- (R5) trash directories shape --------------------------------------
	// Each <TrashDir>/<uuid>/ MUST have a trash.meta.json sidecar. payload/
	// is optional: an empty directory or a pending node has no on-disk bytes
	// to move, in which case applyTrash skips the rename but still writes
	// meta. See internal/storage/dbfs/trash.go.
	if layout.TrashDir != "" {
		entries, err := os.ReadDir(layout.TrashDir)
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					add("non-directory in trash root: %s", e.Name())
					continue
				}
				meta := filepath.Join(layout.TrashDir, e.Name(), "trash.meta.json")
				if _, err := os.Stat(meta); err != nil {
					add("trash dir %s has no trash.meta.json: %v", e.Name(), err)
				}
			}
		} else if !os.IsNotExist(err) {
			fatal("read trash: %v", err)
			return problems
		}
	}

	if len(problems) == 0 {
		return nil
	}
	return problems
}

func indent(s string) string { return "  - " + s }

func formatf(format string, args ...any) string { return sprintf(format, args...) }
