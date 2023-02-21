package monitoring

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var targetProjectID string

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "monitoring",
		Short: "monitoring",
		RunE:  runE,
	}
	return cmd
}

func runE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	targetProjectID = args[0]
	fileName := fmt.Sprintf("monitoring-storage-totalbytes.%s.%s.json", targetProjectID, time.Now().Format(time.RFC3339))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("warning: failed file.Close() err=%s", err)
		}
	}()

	if err := Export(ctx, file, targetProjectID); err != nil {
		return err
	}

	fmt.Printf("created %s\n", fileName)
	return nil
}
