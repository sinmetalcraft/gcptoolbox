package bigquery

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	datasetID string
	prefix    string
	dryRun    bool
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bq",
		Short: "command line bigquery",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("Command name argument expected.")
		},
	}
	cmd.AddCommand(cmdDeleteTables())
	cmd.AddCommand(cmdUpdateExpiration())
	cmd.AddCommand(cmdCopyDefaultExpirationTables())
	return cmd
}
