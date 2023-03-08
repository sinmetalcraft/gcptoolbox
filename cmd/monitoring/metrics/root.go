package metrics

import (
	"github.com/sinmetalcraft/gcptoolbox/cmd/monitoring/metrics/export"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "metrics",
	}
	cmd.AddCommand(export.Command())
	return cmd
}
