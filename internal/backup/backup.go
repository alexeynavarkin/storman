// Package backup handles PostgreSQL dumps for the storman DR strategy
// (docs/arch/backup-dr.md and ADR-0005). A dump is a self-contained pg_dump --format=custom archive
// landed in <data-dir>/meta-storage/backups/<ISO-ts>.dump. Restore is
// pg_restore.
package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/alexnav/storman/internal/datadir"
)

// Tools controls how pg_dump and pg_restore are invoked. Default is the
// binaries on PATH; for dev setups with PostgreSQL in a container override to
// e.g. {"docker","exec","-i","storman-pg","pg_dump"}.
type Tools struct {
	DumpCmd    []string
	RestoreCmd []string
}

// DefaultTools returns the simplest invocation: bare binaries on PATH.
func DefaultTools() Tools {
	return Tools{DumpCmd: []string{"pg_dump"}, RestoreCmd: []string{"pg_restore"}}
}

// resolved returns the cmd argv after applying defaults. Empty/nil → default.
func (t Tools) dump() []string {
	if len(t.DumpCmd) == 0 {
		return []string{"pg_dump"}
	}
	return t.DumpCmd
}

func (t Tools) restore() []string {
	if len(t.RestoreCmd) == 0 {
		return []string{"pg_restore"}
	}
	return t.RestoreCmd
}

// Run takes a custom-format dump of the database at dsn and writes it to
// <data-dir>/meta-storage/backups/<ISO-ts>.dump. After a successful write it
// enforces retention by deleting older dumps so only `retention` newest files
// remain.
//
// Returns the absolute path of the new dump.
//
// retention<=0 disables the retention step (caller keeps every dump).
func Run(ctx context.Context, dataDir, dsn string, retention int, tools Tools) (string, error) {
	backupsDir := datadir.BackupsDir(dataDir)
	if err := os.MkdirAll(backupsDir, 0o700); err != nil {
		return "", fmt.Errorf("mkdir backups dir: %w", err)
	}

	name := time.Now().UTC().Format("2006-01-02T15-04-05Z") + ".dump"
	target := filepath.Join(backupsDir, name)
	tmp := target + ".tmp"

	argv := append([]string{}, tools.dump()...)
	// pg_dump writes the dump to stdout when --file is omitted. We avoid
	// `--file=-` because some pg_dump builds treat it as "no output".
	argv = append(argv, "--format=custom", dsn)

	tmpFile, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create tmp dump: %w", err)
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdout = tmpFile
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		tmpFile.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("pg_dump: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("fsync dump: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("publish dump: %w", err)
	}

	if retention > 0 {
		if err := enforceRetention(backupsDir, retention); err != nil {
			// Don't fail the backup over a retention sweep — log via returned
			// error but keep the new dump in place. Caller decides how to react.
			return target, fmt.Errorf("retention sweep: %w", err)
		}
	}
	return target, nil
}

// enforceRetention deletes the oldest .dump files until at most keep remain.
// Ordering is by mtime descending (newest first). Files without the .dump
// suffix are ignored.
func enforceRetention(dir string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type item struct {
		path  string
		mtime time.Time
	}
	var dumps []item
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".dump" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		dumps = append(dumps, item{filepath.Join(dir, e.Name()), info.ModTime()})
	}
	if len(dumps) <= keep {
		return nil
	}
	sort.Slice(dumps, func(i, j int) bool { return dumps[i].mtime.After(dumps[j].mtime) })
	for _, d := range dumps[keep:] {
		if err := os.Remove(d.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old dump %s: %w", filepath.Base(d.path), err)
		}
	}
	return nil
}

// LatestDump returns the newest .dump file in dataDir/meta-storage/backups,
// or ErrNoDump if the directory is empty/absent.
func LatestDump(dataDir string) (string, error) {
	dir := datadir.BackupsDir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNoDump
		}
		return "", err
	}
	var newest string
	var newestMTime time.Time
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".dump" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if newest == "" || info.ModTime().After(newestMTime) {
			newest = filepath.Join(dir, e.Name())
			newestMTime = info.ModTime()
		}
	}
	if newest == "" {
		return "", ErrNoDump
	}
	return newest, nil
}

// ErrNoDump is returned by LatestDump when the backups directory holds no
// dumps. CLI callers turn this into a useful error message.
var ErrNoDump = errors.New("no dumps found")

// Restore pipes a pg_restore invocation against dsn, replaying dumpPath.
// The dump is streamed to pg_restore's stdin so the invocation works
// transparently for both bare-binary and "docker exec" prefixes (the latter
// can't see the host filesystem). --clean --if-exists --no-owner --no-acl
// keep the restore safe over an existing schema.
func Restore(ctx context.Context, dsn, dumpPath string, tools Tools) error {
	f, err := os.Open(dumpPath)
	if err != nil {
		return fmt.Errorf("open dump: %w", err)
	}
	defer f.Close()

	argv := append([]string{}, tools.restore()...)
	argv = append(argv,
		"--clean", "--if-exists", "--no-owner", "--no-acl",
		"--dbname="+dsn,
	)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = f
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore: %w", err)
	}
	return nil
}
