package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/alexnav/storman/internal/datadir"
)

func newInitCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize the on-disk data directory layout",
		Long: `Creates the fixed layout under --data-dir:
  <data-dir>/flat-storage/   user files
  <data-dir>/meta-storage/   trash, uploads, backups
  <data-dir>/config.json     service configuration

Idempotent: running on an already initialized directory preserves config.json.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			created, err := datadir.Bootstrap(dataDir)
			if err != nil {
				return err
			}
			if created {
				cmd.Printf("initialized %s\n", dataDir)
				cmd.Printf("edit %s and set database.dsn before running 'storman migrate'\n", datadir.ConfigPath(dataDir))
			} else {
				cmd.Printf("%s already initialized — no changes\n", dataDir)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}
