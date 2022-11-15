package bigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus/tokensource"
	"github.com/sinmetalcraft/gcptoolbox/bigquery/tables"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
)

var (
	overwriteTableExpiration bool
)

func cmdCopyDefaultExpirationTables() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "copy-default-expiration-tables",
		Short: "copy-default-expiration-tables",
		RunE:  runCopyDefaultExpirationTables,
	}

	// TODO datasetをargs[0]で、table-prefixがflagで統一した方が自然な感じはする
	cmd.Flags().StringVar(&datasetID, "dataset", "dataset", "dataset")
	cmd.Flags().BoolVar(&overwriteTableExpiration, "overwrite-table-expiration", false, "It will be overwritten even if there is already an expiration in the table")
	cmd.Flags().BoolVar(&dryRun, "dryrun", false, "Display the target table but do not actually process it")
	return cmd
}

func runCopyDefaultExpirationTables(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
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
	defer func() {
		if err := bq.Close(); err != nil {
			fmt.Printf("FIY: failed bq.Close %s", err)
		}
	}()
	s, err := tables.NewService(ctx, bq)
	if err != nil {
		return err
	}

	fmt.Printf("ProjectID=%s\n", projectID)
	fmt.Printf("DatasetID=%s\n", datasetID)
	fmt.Printf("OverwriteTableExpiration=%t\n", overwriteTableExpiration)
	fmt.Printf("DryRun=%t\n", dryRun)
	fmt.Println("Start copying default table expiration to tables")
	fmt.Println()

	var ops []tables.APIOptions
	if overwriteTableExpiration {
		ops = append(ops, tables.WithOverwriteExpiration())
	}
	if dryRun {
		ops = append(ops, tables.WithDryRun())
	}

	if err := s.UpdateTablesExpirationFromDatasetDefaultSetting(ctx, projectID, datasetID, ops...); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Done")
	return nil
}
