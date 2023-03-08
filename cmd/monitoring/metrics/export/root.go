package export

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "export",
		RunE:  RunE,
	}
	return cmd
}

func RunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	targetProjectID := args[0]
	exportMetrics := args[1]
	if validMetricsType(exportMetrics) {
		return fmt.Errorf("invalid metrics type")
	}

	fileName := exportFileName(exportMetrics, targetProjectID)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("warning: failed file.Close() err=%s", err)
		}
	}()

	switch exportMetrics {
	case "storage-totalbytes":
		if err := ExportStorageTotalByte(ctx, file, targetProjectID); err != nil {
			return err
		}
	case "storage-receive-bytes":
		if err := ExportStorageReceiveByte(ctx, file, targetProjectID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid metrics type")
	}

	fmt.Printf("created %s\n", fileName)
	return nil
}

func validMetricsType(metricsType string) bool {
	switch metricsType {
	case "storage-totalbytes":
		return true
	default:
		return false
	}
}

func exportFileName(exportMetrics string, targetProjectID string) string {
	return fmt.Sprintf("monitoring-%s.%s.%s.json", exportMetrics, targetProjectID, time.Now().Format(time.RFC3339))
}
