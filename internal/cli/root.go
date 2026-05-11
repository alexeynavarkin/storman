// Package cli wires the storman command-line interface.
package cli

import (
	"runtime/debug"

	"github.com/spf13/cobra"
)

func NewRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "storman",
		Short:         "Personal file storage server",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.AddCommand(
		newInitCmd(),
		newMigrateCmd(),
		newBootstrapCmd(),
		newUserAddCmd(),
		newVersionCmd(),
		newServeCmd(),
		newBackupCmd(),
		newRecoverCmd(),
	)
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version",
		Run: func(cmd *cobra.Command, args []string) {
			info, ok := debug.ReadBuildInfo()
			if !ok {
				cmd.Println("(no build info)")
				return
			}
			cmd.Println(info.Main.Version)
			cmd.Printf("go: %s\n", info.GoVersion)
			for _, s := range info.Settings {
				if s.Key == "vcs.revision" || s.Key == "vcs.time" {
					cmd.Printf("%s: %s\n", s.Key, s.Value)
				}
			}
		},
	}
}
