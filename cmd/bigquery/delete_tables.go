package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus/tokensource"
	bqbox "github.com/sinmetalcraft/gcpbox/bigquery"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
)

func cmdDeleteTables() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-tables",
		Short: "delete-tables",
		RunE:  runDeleteTables,
	}
	cmd.Flags().StringVar(&datasetID, "dataset", "dataset", "dataset")
	cmd.Flags().BoolVar(&dryRun, "dryrun", false, "dryrun")
	return cmd
}

func runDeleteTables(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	projectID, ok := contexter.ProjectID(ctx)
	if !ok {
		return fmt.Errorf("project required")
	}

	ts, err := tokensource.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	bq, err := bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
	if err != nil {
		return err
	}
	bqboxService, err := bqbox.NewService(ctx, bq)
	if err != nil {
		return err
	}
	defer func() {
		if err := bqboxService.Close(ctx); err != nil {
			fmt.Printf("FIY: failed bqboxService.Close %s", err)
		}
	}()

	var ops []bqbox.APIOptions
	fmt.Println("bigquery delete tables")
	fmt.Printf("ProjectID=%s\n", projectID)
	fmt.Printf("DatasetID=%s\n", datasetID)
	fmt.Printf("TablePrefix=%s\n", args[0])
	fmt.Printf("DryRun=%t\n", dryRun)
	if dryRun {
		ops = append(ops, bqbox.WithDryRun())
	}
	fmt.Println()
	msgs, err := bqboxService.DeleteTablesByTablePrefix(ctx, projectID, datasetID, args[0], ops...)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		fmt.Println(msg)
	}
	fmt.Println()
	fmt.Println("Done")
	return nil
}
