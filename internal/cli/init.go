package cli

import (
	"fmt"
	"os"
	"strconv"

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

Idempotent: running on an already initialized directory preserves config.json.

When generating a fresh config.json, the following environment variables
override default values (ignored if config.json already exists):
  STORMAN_DB_DSN           database.dsn
  STORMAN_LISTEN_ADDR      web.listen_addr
  STORMAN_TRUST_PROXY      web.trust_proxy_headers (true/false)
  STORMAN_SECURE_COOKIES   web.secure_cookies (true/false)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" {
				return fmt.Errorf("--data-dir is required")
			}
			ov, err := overridesFromEnv()
			if err != nil {
				return err
			}
			created, err := datadir.BootstrapWithOverrides(dataDir, ov)
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

func overridesFromEnv() (datadir.Overrides, error) {
	ov := datadir.Overrides{
		DatabaseDSN: os.Getenv("STORMAN_DB_DSN"),
		ListenAddr:  os.Getenv("STORMAN_LISTEN_ADDR"),
	}
	if v := os.Getenv("STORMAN_TRUST_PROXY"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return datadir.Overrides{}, fmt.Errorf("STORMAN_TRUST_PROXY: %w", err)
		}
		ov.TrustProxyHeaders = &b
	}
	if v := os.Getenv("STORMAN_SECURE_COOKIES"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return datadir.Overrides{}, fmt.Errorf("STORMAN_SECURE_COOKIES: %w", err)
		}
		ov.SecureCookies = &b
	}
	return ov, nil
}
