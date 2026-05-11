package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
)

func newMigrateCmd() *cobra.Command {
	var dataDir string
	var down int
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply database migrations (up by default; use --down N to roll back)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}

			var result db.MigrateResult
			if down > 0 {
				result, err = db.MigrateDown(cfg.Database.DSN, down)
			} else {
				result, err = db.MigrateUp(cfg.Database.DSN)
			}
			if err != nil {
				return err
			}

			switch {
			case result.NoOp:
				cmd.Printf("schema already at version %d — no changes\n", result.From)
			default:
				cmd.Printf("schema %d → %d\n", result.From, result.To)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	cmd.Flags().IntVar(&down, "down", 0, "roll back this many migrations instead of applying up")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}
