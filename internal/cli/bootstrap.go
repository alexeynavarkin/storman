package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

func newBootstrapCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Create the root node in the database (idempotent)",
		Long: `Creates the root directory node so the FileSystem has somewhere to attach
children. Run this once after 'storman migrate', before serving traffic.

Idempotent: safe to re-run.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}
			ctx := context.Background()
			pool, err := db.NewPool(ctx, cfg.Database.DSN)
			if err != nil {
				return err
			}
			defer pool.Close()

			fs := dbfs.New(pool, datadir.TrashDir(cfg.DataDir))
			created, err := fs.Bootstrap(ctx)
			if err != nil {
				return err
			}
			if created {
				cmd.Println("root node created")
			} else {
				cmd.Println("root node already present — no changes")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}
