package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
	"github.com/alexnav/storman/internal/storage/dbfs"
)

func newBootstrapCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Create the root node in the database (idempotent)",
		Long: `Creates the root directory node so the FileSystem has somewhere to attach
children. Run this once after 'storman migrate', before serving traffic.

Idempotent: safe to re-run.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(resolveConfigPath(*configPath))
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
	return cmd
}
