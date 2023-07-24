package storage

import (
	"github.com/sinmetalcraft/gcptoolbox/cmd/storage/deletes"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "storage",
	}
	cmd.AddCommand(deletes.Command())
	return cmd
}
