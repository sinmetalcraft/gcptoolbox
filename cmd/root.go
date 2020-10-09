package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// RootCmd is root command
var RootCmd = &cobra.Command{
	Use:   "gcptoolbox",
	Short: "command line gcptoolbox",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("Command name argument expected.")
	},
}

func init() {
	cobra.OnInitialize()
	ServiceUsageCmd.AddCommand(
		serviceUsageDiffCmd(),
	)

	RootCmd.AddCommand(
		ServiceUsageCmd,
	)
}
