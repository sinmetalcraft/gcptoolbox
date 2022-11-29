package bq2gcs_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/sinmetalcraft/gcptoolbox/bq2gcs"
)

const GCPToolBoxProjectID = "sinmetal-gcptoolbox"

func TestService_ExportShardingTable(t *testing.T) {
	ctx := context.Background()

	bq, err := bigquery.NewClient(ctx, GCPToolBoxProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Log(err)
		}
	}()

	s, err := bq2gcs.NewService(ctx, bq)
	if err != nil {
		t.Fatal(err)
	}

	// 今日のTableをExportするか？
	// URIに実行時刻か何かを入れないと、二重実行された時にわけわからんくなる
	_, err = s.ExportShardingTable(ctx, &bq2gcs.GCSReferenceForExportShardingTables{
		URI:               "gs://sinmetal-gcptoolbox-test/cloudaudit_googleapis_com_activity_/20221005/*.parquet",
		DestinationFormat: bigquery.Parquet,
		Compression:       bigquery.None,
	}, GCPToolBoxProjectID, "alllog", "cloudaudit_googleapis_com_activity_20221005")
	if err != nil {
		t.Fatal(err)
	}
}
