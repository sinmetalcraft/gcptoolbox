package monitoring

import (
	"github.com/sinmetalcraft/gcptoolbox/cmd/monitoring/metrics"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "monitoring",
		Short: "monitoring",
	}
	cmd.AddCommand(metrics.Command())
	return cmd
}
