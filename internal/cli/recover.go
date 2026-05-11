package cli

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/backup"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
	"github.com/alexnav/storman/internal/storage"
	"github.com/alexnav/storman/internal/storage/dbfs"
	"github.com/alexnav/storman/internal/storage/flat"
)

func newRecoverCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Disaster-recovery flows: --from-backup and --from-disk",
	}
	cmd.AddCommand(newRecoverFromBackupCmd(configPath), newRecoverFromDiskCmd(configPath))
	return cmd
}

func newRecoverFromBackupCmd(configPath *string) *cobra.Command {
	var dumpPath string
	cmd := &cobra.Command{
		Use:   "from-backup",
		Short: "Replay a pg_dump archive then fsck-reconcile against the disk",
		Long: `Runs pg_restore against the configured database with the given dump file
(or the newest one in meta-storage/backups if --path is omitted). After
restore, walks every file node in the resurrected schema and verifies the
backend can Stat its content. Missing content is marked status='broken' for
manual review.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(resolveConfigPath(*configPath))
			if err != nil {
				return err
			}
			if dumpPath == "" {
				dumpPath, err = backup.LatestDump(cfg.DataDir)
				if err != nil {
					return err
				}
			}
			tools := backup.Tools{DumpCmd: cfg.Backup.PGDumpCmd, RestoreCmd: cfg.Backup.PGRestoreCmd}
			cmd.Printf("restoring %s\n", dumpPath)
			if err := backup.Restore(cmd.Context(), cfg.Database.DSN, dumpPath, tools); err != nil {
				return err
			}
			cmd.Println("restore complete — running fsck-reconcile against the disk")
			broken, err := reconcile(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			if broken > 0 {
				cmd.Printf("warning: %d file node(s) marked status='broken' (content missing on disk)\n", broken)
			} else {
				cmd.Println("fsck-reconcile: every file node has matching content on disk")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dumpPath, "path", "", "specific .dump file to restore (default: newest in backups/)")
	return cmd
}

func newRecoverFromDiskCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "from-disk",
		Short: "Rebuild the node tree by walking flat-storage (last-line DR)",
		Long: `Walks <data-dir>/flat-storage/ and registers every file and directory into a
fresh node tree. This is the last-line disaster recovery: it uses no backup
and produces a minimal schema (no users, no permissions, no extended meta).

Refuses to run when the tree is not empty — restore over an existing tree
would create duplicate nodes. Run 'storman migrate' first to create the
schema, then this command, then 'storman useradd' to add an admin and grant
them ACL via the API.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(resolveConfigPath(*configPath))
			if err != nil {
				return err
			}
			return recoverFromDisk(cmd.Context(), cmd.OutOrStdout(), cfg)
		},
	}
	return cmd
}

// reconcile walks the nodes table after a restore and marks each missing
// file as status='broken'. Returns the count of broken rows.
func reconcile(ctx context.Context, cfg config.Config) (int, error) {
	pool, err := db.NewPool(ctx, cfg.Database.DSN)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	rows, err := pool.Query(ctx,
		`SELECT id, backend_ref FROM nodes
		 WHERE type='file' AND status='ready' AND deleted_at IS NULL`)
	if err != nil {
		return 0, err
	}
	type fileRow struct {
		ID  uuid.UUID
		Ref string
	}
	var files []fileRow
	for rows.Next() {
		var r fileRow
		var ref *string
		if err := rows.Scan(&r.ID, &ref); err != nil {
			rows.Close()
			return 0, err
		}
		if ref != nil {
			r.Ref = *ref
		}
		files = append(files, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	storageDir := datadir.StorageDir(cfg.DataDir)
	broken := 0
	for _, f := range files {
		abs := filepath.Join(storageDir, filepath.FromSlash(f.Ref))
		if _, statErr := os.Stat(abs); errors.Is(statErr, os.ErrNotExist) {
			if _, err := pool.Exec(ctx,
				`UPDATE nodes SET status='broken', updated_at=now() WHERE id=$1`, f.ID); err != nil {
				return broken, err
			}
			broken++
		}
	}
	return broken, nil
}

// recoverFromDisk walks flat-storage and rebuilds the node tree from scratch.
func recoverFromDisk(ctx context.Context, out io.Writer, cfg config.Config) error {
	pool, err := db.NewPool(ctx, cfg.Database.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	flatBackend := flat.New(datadir.StorageDir(cfg.DataDir), datadir.UploadsDir(cfg.DataDir))
	fs := dbfs.New(pool, datadir.TrashDir(cfg.DataDir), flatBackend)

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nodes`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("refusing to recover into a non-empty node tree (%d existing rows)", count)
	}

	if _, err := fs.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap root: %w", err)
	}

	storageDir := datadir.StorageDir(cfg.DataDir)
	dirs := 0
	files := 0
	walkErr := filepath.Walk(storageDir, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if absPath == storageDir {
			return nil
		}
		rel, err := filepath.Rel(storageDir, absPath)
		if err != nil {
			return err
		}
		logical := "/" + filepath.ToSlash(rel)
		if info.IsDir() {
			if err := fs.Mkdir(ctx, logical, storage.MkdirOpts{}); err != nil {
				return fmt.Errorf("mkdir %s: %w", logical, err)
			}
			dirs++
			return nil
		}
		hash, size, err := hashFile(absPath)
		if err != nil {
			return fmt.Errorf("hash %s: %w", logical, err)
		}
		mime, err := sniffMime(absPath)
		if err != nil {
			return fmt.Errorf("sniff %s: %w", logical, err)
		}
		if err := ingestFile(ctx, pool, logical, filepath.ToSlash(rel), hash, size, mime); err != nil {
			return fmt.Errorf("ingest %s: %w", logical, err)
		}
		files++
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	fmt.Fprintf(out, "recovered: %d directories, %d files\n", dirs, files)
	return nil
}

func hashFile(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}

func sniffMime(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, 512)
	n, _ := io.ReadFull(f, buf)
	return http.DetectContentType(buf[:n]), nil
}

// ingestFile inserts a file node by resolving its parent dir from the logical
// path and writing the row directly — skipping OpenWrite since content
// already exists on disk.
func ingestFile(ctx context.Context, pool *pgxpool.Pool, logicalPath, backendRef string, sha []byte, size int64, mime string) error {
	segments := strings.Split(strings.TrimPrefix(logicalPath, "/"), "/")
	name := segments[len(segments)-1]
	parentSegs := segments[:len(segments)-1]

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var parentID uuid.UUID
		var parentLtree string
		if err := tx.QueryRow(ctx,
			`SELECT id, path::text FROM nodes WHERE parent_id IS NULL`).Scan(&parentID, &parentLtree); err != nil {
			return fmt.Errorf("find root: %w", err)
		}
		for _, seg := range parentSegs {
			var childID uuid.UUID
			var childPath string
			if err := tx.QueryRow(ctx,
				`SELECT id, path::text FROM nodes WHERE parent_id=$1 AND name=$2 AND deleted_at IS NULL`,
				parentID, seg).Scan(&childID, &childPath); err != nil {
				return fmt.Errorf("resolve %q: %w", seg, err)
			}
			parentID = childID
			parentLtree = childPath
		}

		nodeID := uuid.New()
		label := strings.ReplaceAll(nodeID.String(), "-", "")
		nodePath := parentLtree + "." + label
		_, err := tx.Exec(ctx,
			`INSERT INTO nodes (id, parent_id, path, name, type, backend_kind, backend_ref, size, mime, mtime, sha256, status)
			 VALUES ($1, $2, $3::ltree, $4, 'file', 'flat', $5, $6, $7, now(), $8, 'ready')`,
			nodeID, parentID, nodePath, name, backendRef, size, mime, sha)
		return err
	})
}
