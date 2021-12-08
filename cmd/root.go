package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	projectID string
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
	loadEnvironmentValue()

	cobra.OnInitialize()

	RootCmd.PersistentFlags().StringVar(&projectID, "project", "project", "project id")

	ServiceUsageCmd.AddCommand(
		serviceUsageDiffCmd(),
	)

	BigQueryCmd.AddCommand(
		bigQueryDeleteTablesCmd(),
	)

	RootCmd.AddCommand(
		ServiceUsageCmd,
		BigQueryCmd,
	)
}

func loadEnvironmentValue() {
	projectID = os.Getenv("GCLOUD_SDK_PROJECT")
}
