package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
)

// newBootCmd composes init → migrate → serve into a single command. It is the
// recommended entrypoint for container deployments where `docker compose up`
// should bring the service to a steady state without manual subcommand chains.
// Every step is idempotent, so re-running boot on an already-initialized
// directory is safe (it just logs the no-op outcome of each phase).
//
// Root-node creation lives inside `serve` (see internal/cli/serve.go), so this
// command does not call `bootstrap` explicitly — `serve` will create the root
// node on first start.
func newBootCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "boot",
		Short: "Initialize data dir, run migrations, and start the server",
		Long: `Runs init (idempotent — generates config.json on first start from env vars
STORMAN_DB_DSN/STORMAN_LISTEN_ADDR/STORMAN_TRUST_PROXY/STORMAN_SECURE_COOKIES),
then migrate (idempotent), then serve. Designed for container entrypoints.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}

			cmd.Println("==> init")
			ov, err := overridesFromEnv()
			if err != nil {
				return err
			}
			created, err := datadir.BootstrapWithOverrides(dataDir, ov)
			if err != nil {
				return fmt.Errorf("init: %w", err)
			}
			if created {
				cmd.Printf("    initialized %s\n", dataDir)
			} else {
				cmd.Printf("    %s already initialized\n", dataDir)
			}

			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}

			cmd.Println("==> migrate")
			result, err := db.MigrateUp(cfg.Database.DSN)
			if err != nil {
				return fmt.Errorf("migrate: %w", err)
			}
			if result.NoOp {
				cmd.Printf("    schema already at version %d\n", result.From)
			} else {
				cmd.Printf("    schema %d → %d\n", result.From, result.To)
			}

			cmd.Println("==> serve")
			return runServe(cmd.Context(), cfg, "")
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}
