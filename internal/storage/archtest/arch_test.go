// Package archtest mechanically enforces ADR-0001: all delivery protocols
// go through storage.FileSystem (or, where the interface does not model the
// admin surface, through dbfs's non-interface methods). No protocol may
// import a concrete FileBackend or issue raw SQL against the storage tables.
//
// This is the test author's safety net for ADR-0001 — code review remains
// the first line of defence, but a regression here is what would break
// durability invariants invisibly.
package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const modulePath = "github.com/alexnav/storman"

// forbiddenImports maps each forbidden import path to the architectural reason
// the rule exists. Packages under internal/storage/ are exempt because they
// own the boundary; anyone else importing these is going around it.
var forbiddenImports = []string{
	modulePath + "/internal/storage/flat",
}

// storageAllowedPrefix names the prefix that owns the storage boundary. Any
// package under this prefix may freely import concrete backends and run raw
// SQL — that's its job.
const storageAllowedPrefix = "internal/storage/"

// forbiddenSQLRe matches raw SQL fragments that mutate or read the durability
// tables. Detection runs against every Go string literal outside the storage
// package. The pattern is intentionally narrow (specific SQL keywords +
// table) to avoid false positives on identifiers in unrelated text.
var forbiddenSQLRe = regexp.MustCompile(`(?i)\b(FROM|INTO|UPDATE|JOIN)\s+(nodes|outbox|outbox_history|jobs)\b`)

// TestNoProtocolImportsConcreteBackend walks every .go file under internal/,
// cmd/, and any future top-level package and asserts that nothing outside
// internal/storage/ imports internal/storage/flat (or any other concrete
// backend that lands later).
func TestNoProtocolImportsConcreteBackend(t *testing.T) {
	repoRoot := repoRoot(t)
	var violations []string

	walkGoFiles(t, repoRoot, func(rel string, file *ast.File) {
		if isStorageOwned(rel) {
			return
		}
		for _, imp := range file.Imports {
			pathLit := strings.Trim(imp.Path.Value, `"`)
			for _, forbidden := range forbiddenImports {
				if pathLit == forbidden {
					violations = append(violations,
						rel+": imports "+forbidden+
							" (ADR-0001 forbids protocol/admin packages from depending on concrete backends; "+
							"use storage.FileSystem instead)")
				}
			}
		}
	})

	if len(violations) > 0 {
		t.Fatalf("ADR-0001 import boundary violated:\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestNoProtocolHasRawSQLOnStorageTables scans Go string literals outside
// internal/storage/ for SQL that touches the durability tables (nodes,
// outbox, outbox_history, jobs). These tables are owned by dbfs and the
// indexing pipeline; raw SQL against them from protocol code bypasses the
// outbox / audit / ACL machinery.
//
// Opt-out: add the suffix `// arch:allow-sql` to the line that owns the
// literal (e.g. internal admin scripts that intentionally inspect state).
func TestNoProtocolHasRawSQLOnStorageTables(t *testing.T) {
	repoRoot := repoRoot(t)
	var violations []string

	fset := token.NewFileSet()
	walkGoFilesFset(t, repoRoot, fset, func(rel string, file *ast.File) {
		if isStorageOwned(rel) {
			return
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := unquoteLoose(lit.Value)
			if err != nil {
				return true
			}
			if !forbiddenSQLRe.MatchString(value) {
				return true
			}
			pos := fset.Position(lit.Pos())
			if lineHasAllowComment(pos.Filename, pos.Line) {
				return true
			}
			match := forbiddenSQLRe.FindString(value)
			violations = append(violations,
				pos.String()+": SQL literal touches storage table — "+match+
					" (ADR-0001: use storage.FileSystem)")
			return true
		})
	})

	if len(violations) > 0 {
		t.Fatalf("ADR-0001 SQL boundary violated:\n  %s", strings.Join(violations, "\n  "))
	}
}

// isStorageOwned reports whether the given repo-relative path lives in a
// package that is allowed to talk to storage internals. The set is small and
// each entry exists for a documented reason. New entries should be added
// only when ADR-0001 explicitly carves out the case — never to grandfather
// new shortcuts.
func isStorageOwned(rel string) bool {
	rel = filepath.ToSlash(rel)
	switch {
	case strings.HasPrefix(rel, storageAllowedPrefix):
		// The boundary itself.
		return true
	case strings.HasPrefix(rel, "internal/migrations/"):
		// Migrations carry SQL by definition.
		return true
	case strings.HasPrefix(rel, "internal/db/"):
		// Pool / migrate plumbing; nothing protocol-facing here.
		return true
	case strings.HasPrefix(rel, "internal/cli/"):
		// CLI is the composition root: `serve` wires the backends together,
		// `recover --from-disk` is the documented admin escape hatch.
		// Both are explicitly admin/recovery surfaces, not protocols.
		return true
	case strings.HasPrefix(rel, "internal/jobs/"):
		// The async indexing pipeline (hash, future EXIF/MIME). ADR-0001
		// lists it as a privileged internal subsystem that owns its own
		// SELECT FOR UPDATE SKIP LOCKED loop over the jobs table and
		// writes sha256 / mime back to nodes.
		return true
	case strings.HasPrefix(rel, "internal/rbac/"):
		// RBAC computes Traverse via ltree walks across nodes. This is a
		// read-only, cross-cutting concern wrapped around every storage
		// mutation by the protocol layer — not a backdoor write path.
		return true
	case strings.HasPrefix(rel, "internal/metrics/"):
		// Operational observability: read-only counters over outbox.
		return true
	}
	return false
}

func repoRoot(t *testing.T) string {
	t.Helper()
	// archtest lives at internal/storage/archtest/; repo root is three dirs up.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

// walkGoFiles invokes fn for every parseable .go file under root. _test.go
// files are skipped — they're allowed to do anything for testing purposes.
func walkGoFiles(t *testing.T, root string, fn func(rel string, file *ast.File)) {
	walkGoFilesFset(t, root, token.NewFileSet(), fn)
}

func walkGoFilesFset(t *testing.T, root string, fset *token.FileSet, fn func(rel string, file *ast.File)) {
	t.Helper()
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			name := info.Name()
			switch name {
			case "vendor", "node_modules", "ui", ".git", "dev-data", "deployments", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".go") {
			return nil
		}
		if strings.HasSuffix(info.Name(), "_test.go") {
			return nil
		}
		// Skip our own package — and storagetest, which deliberately constructs
		// helpers around the storage internals.
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, p, nil, parser.ImportsOnly|parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		// ImportsOnly is fine for import scans, but the SQL scan needs the
		// full body. Re-parse without ImportsOnly if a body inspection is
		// likely required.
		full, err := parser.ParseFile(fset, p, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse-full %s: %v", p, err)
		}
		_ = file
		fn(filepath.ToSlash(rel), full)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

// unquoteLoose is a lenient string-literal decoder: it returns the unquoted
// value for ordinary Go strings and the unwrapped value for raw strings.
// Invalid literals return an error so the caller can skip them.
func unquoteLoose(lit string) (string, error) {
	if len(lit) >= 2 && lit[0] == '`' && lit[len(lit)-1] == '`' {
		return lit[1 : len(lit)-1], nil
	}
	return strings.Trim(lit, `"`), nil
}

// lineHasAllowComment reads file at path and returns true if line contains
// the opt-out token. Used to escape false positives in code that knowingly
// touches storage tables (e.g. recover-from-disk admin tools).
func lineHasAllowComment(path string, line int) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	lines := strings.Split(string(data), "\n")
	if line-1 < 0 || line-1 >= len(lines) {
		return false
	}
	return strings.Contains(lines[line-1], "arch:allow-sql")
}
