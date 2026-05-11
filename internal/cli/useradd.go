package cli

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/alexnav/storman/internal/auth"
	"github.com/alexnav/storman/internal/config"
	"github.com/alexnav/storman/internal/datadir"
	"github.com/alexnav/storman/internal/db"
)

func newUserAddCmd() *cobra.Command {
	var (
		dataDir  string
		login    string
		password string
	)
	cmd := &cobra.Command{
		Use:   "useradd",
		Short: "Create a new user account",
		Long: `Creates a user with the given login. If --password is not supplied, the
command reads the password from the terminal (twice, for confirmation).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dataDir == "" || login == "" {
				return errors.New("--data-dir and --login are required")
			}
			cfg, err := config.Load(datadir.ConfigPath(dataDir))
			if err != nil {
				return err
			}
			if password == "" {
				password, err = promptPassword(cmd.OutOrStderr())
				if err != nil {
					return err
				}
			}

			ctx := context.Background()
			pool, err := db.NewPool(ctx, cfg.Database.DSN)
			if err != nil {
				return err
			}
			defer pool.Close()

			users := auth.NewUserService(pool)
			user, err := users.Create(ctx, login, password)
			if err != nil {
				return err
			}
			cmd.Printf("created user %s (id=%s)\n", user.Login, user.ID)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "path to the storman data directory")
	cmd.Flags().StringVar(&login, "login", "", "login name (case-insensitive)")
	cmd.Flags().StringVar(&password, "password", "", "password (omit for interactive prompt)")
	_ = cmd.MarkFlagRequired("data-dir")
	_ = cmd.MarkFlagRequired("login")
	return cmd
}

// promptPassword reads a password twice from the terminal without echoing.
func promptPassword(w interface{ Write(p []byte) (int, error) }) (string, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return "", errors.New("stdin is not a terminal — supply --password explicitly")
	}
	_, _ = fmt.Fprint(w, "password: ")
	pw1, err := term.ReadPassword(fd)
	_, _ = fmt.Fprintln(w)
	if err != nil {
		return "", err
	}
	_, _ = fmt.Fprint(w, "confirm:  ")
	pw2, err := term.ReadPassword(fd)
	_, _ = fmt.Fprintln(w)
	if err != nil {
		return "", err
	}
	if string(pw1) != string(pw2) {
		return "", errors.New("passwords do not match")
	}
	return string(pw1), nil
}
