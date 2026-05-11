package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/backup"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
)

func newBackupCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Take a pg_dump snapshot into meta-storage/backups",
		Long: `Runs pg_dump --format=custom against the configured database and writes the
result to <data-dir>/meta-storage/backups/<ISO-ts>.dump. Retention from
config.backup.retention is applied after a successful write.

The pg_dump binary must be available — either on PATH or via the
backup.pg_dump_cmd argv prefix in config.json (useful when PostgreSQL runs
inside a container).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}
			tools := backup.Tools{DumpCmd: cfg.Backup.PGDumpCmd, RestoreCmd: cfg.Backup.PGRestoreCmd}
			path, err := backup.Run(cmd.Context(), cfg.DataDir, cfg.Database.DSN, cfg.Backup.Retention, tools)
			if err != nil {
				return err
			}
			cmd.Println(path)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}
