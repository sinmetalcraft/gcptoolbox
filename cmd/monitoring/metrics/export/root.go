package export

import (
	"fmt"
	"os"
	"strings"
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

const (
	exportTypeStorageTotalBytes   = "storage-total-bytes"
	exportTypeStorageReceiveBytes = "storage-receive-bytes"
)

func RunE(cmd *cobra.Command, args []string) (err error) {
	ctx := cmd.Context()

	exportMetrics := args[0]
	targetProjectID := args[1]
	if err := validateExportMetricsType(exportMetrics); err != nil {
		return fmt.Errorf("input type is %s: %w", exportMetrics, err)
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

		if err != nil {
			// 処理が成功しなかった場合は、Exportしようとして作ったファイルを消す
			if err := os.Remove(fileName); err != nil {
				fmt.Printf("warning: failed file.Remove() err=%s", err)
			}
		}
	}()

	switch exportMetrics {
	case exportTypeStorageTotalBytes:
		if err := ExportStorageTotalByte(ctx, file, targetProjectID); err != nil {
			return err
		}
	case exportTypeStorageReceiveBytes:
		if err := ExportStorageReceiveByte(ctx, file, targetProjectID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid metrics type")
	}

	fmt.Printf("created %s\n", fileName)
	return nil
}

func validateExportMetricsType(metricsType string) error {
	l := []string{exportTypeStorageTotalBytes, exportTypeStorageReceiveBytes}
	for _, v := range l {
		if v == metricsType {
			return nil
		}
	}
	return fmt.Errorf("invalid metrics type.support type is %s", strings.Join(l, ","))
}

func exportFileName(exportMetrics string, targetProjectID string) string {
	return fmt.Sprintf("monitoring-%s.%s.%s.json", exportMetrics, targetProjectID, time.Now().Format(time.RFC3339))
}
