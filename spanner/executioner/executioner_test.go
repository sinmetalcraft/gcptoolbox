package executioner

import (
	"context"
	"fmt"
	"testing"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/storage"
)

func TestExecutioner_Run(t *testing.T) {
	ctx := t.Context()

	executioner := newExecutionerForTest(ctx, t)

	// FIXME: backupOperationをどうするか
	if err := executioner.Run(ctx, "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal", "", true, WithDryRun(true)); err != nil {
		t.Fatal(err)
	}
}

func TestExecutioner_ListDatabase(t *testing.T) {
	ctx := t.Context()

	executioner := newExecutionerForTest(ctx, t)

	dbs, err := executioner.ListDatabase(ctx, "gcpug-public-spanner", "merpay-sponsored-instance")
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range dbs {
		t.Log(db)
	}
}

func TestExecutioner_CreateBackup(t *testing.T) {
	ctx := t.Context()

	executioner := newExecutionerForTest(ctx, t)

	ope, execution, err := executioner.CreateBackup(ctx, "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal")
	if err != nil {
		t.Fatal(err)
	}
	if !execution {
		t.Log("not execution")
		return
	}
	fmt.Println(ope.Name())
}

func TestExecutioner_ExportIamPolicy(t *testing.T) {
	ctx := t.Context()

	executioner := newExecutionerForTest(ctx, t)

	exported, err := executioner.ExportIamPolicy(ctx, "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("exported: %v", exported)
}

func newExecutionerForTest(ctx context.Context, t *testing.T) *Executioner {
	metricCli, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dbAdminCli, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	gcsCli, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutioner(ctx, metricCli, dbAdminCli, gcsCli, "spanner-iam-policy-export-gcpug-public-spanner")
}
