package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/sinmetalcraft/gcptoolbox/cmd/bigquery"
	"github.com/sinmetalcraft/gcptoolbox/cmd/bq2gcs"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"github.com/sinmetalcraft/gcptoolbox/cmd/monitoring"
	"github.com/spf13/cobra"
)

var (
	projectID string
)

// RootCmd is root command
var RootCmd = &cobra.Command{
	Use:   "gcptoolbox",
	Short: "gcptoolbox",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("Command name argument expected.")
	},
}

func init() {
	loadEnvironmentValue()

	cobra.OnInitialize()

	RootCmd.PersistentFlags().StringVar(&projectID, "project", "project", "project id")
	RootCmd.PersistentPreRunE = preRunE

	// TODO これを新しい形に変更する
	ServiceUsageCmd.AddCommand(
		serviceUsageDiffCmd(),
	)

	RootCmd.AddCommand(
		ServiceUsageCmd,
		bigquery.Command(),
		bq2gcs.Command(),
		monitoring.Command(),
	)
}

func loadEnvironmentValue() {
	projectID = os.Getenv("GCLOUD_SDK_PROJECT")
}

func preRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	ctx = contexter.WithProjectID(ctx, projectID)
	cmd.SetContext(ctx)
	return nil
}
