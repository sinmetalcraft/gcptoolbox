package bq2gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus/tokensource"
	"github.com/sinmetalcraft/gcptoolbox/bq2gcs"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
)

var datasetID string
var dryRun bool
var location string
var gcsURI string
var destinationFormat string
var compression string

var tablePrefix string
var expirationDay int

var limitTableSize int64
var limitTableCount int64

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bq2gcs",
		Short: "bq2gcs",
		RunE:  runE,
	}
	const datasetName = "dataset"
	cmd.Flags().StringVar(&datasetID, datasetName, "dataset", "dataset")
	if err := cmd.MarkFlagRequired(datasetName); err != nil {
		fmt.Println(err)
	}
	cmd.Flags().BoolVar(&dryRun, "dryrun", false, "dryrun")

	cmd.Flags().StringVar(&location, "location", "", "bigquery region")

	const gcsURIName = "gcs_uri"
	cmd.Flags().StringVar(&gcsURI, gcsURIName, "", "Path starting with gs://.The {{TABLE_ID}} part is replaced with table_id")
	if err := cmd.MarkFlagRequired(gcsURIName); err != nil {
		fmt.Println(err)
	}

	cmd.Flags().StringVar(&destinationFormat, "destination_format", "", "")
	cmd.Flags().StringVar(&compression, "compression", "", "")

	const tablePrefixName = "table_prefix"
	cmd.Flags().StringVar(&tablePrefix, tablePrefixName, "", "Prefix of the table to be exported. If not specified, all will be targeted")
	cmd.Flags().IntVar(&expirationDay, "expiration_day", 0, "How many days old table to export.If not specified, all tables are targeted")

	cmd.Flags().Int64Var(&limitTableSize, "limit_table_size", 50*1024*1024*1024*1024, "Total limit of exported table size")
	cmd.Flags().Int64Var(&limitTableCount, "limit_table_count", 100000, "Limit on the number of tables to export")
	return cmd
}

func runE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	projectID, ok := contexter.ProjectID(ctx)
	if !ok {
		return fmt.Errorf("project required")
	}

	fmt.Printf("ProjectID=%s\n", projectID)
	ts, err := tokensource.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	to := &bq2gcs.GCSReferenceForExportShardingTables{
		URI:               gcsURI,
		DestinationFormat: bigquery.DataFormat(destinationFormat),
		Compression:       bigquery.Compression(compression),
	}
	target := &bq2gcs.DateShardingTableTarget{
		Prefix:        tablePrefix,
		ExpirationDay: expirationDay,
	}
	limit := &bq2gcs.ExportShardingTablesLimit{
		TableSize:  limitTableSize,
		TableCount: limitTableCount,
	}

	bq, err := bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
	if err != nil {
		return err
	}

	service, err := bq2gcs.NewService(ctx, bq)
	if err != nil {
		return err
	}

	_, err = service.ExportShardingTables(ctx, to, projectID, datasetID, target, func(ctx context.Context, jobID string) {
		fmt.Printf("working %s\n", jobID)
	}, limit)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Done")
	return nil
}
