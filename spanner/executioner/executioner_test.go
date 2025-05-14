package executioner

import (
	"fmt"
	"testing"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
)

func TestExecutioner_Run(t *testing.T) {
	ctx := t.Context()

	metricCli, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dbAdminCli, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	executioner := NewExecutioner(ctx, metricCli, dbAdminCli)

	if err := executioner.Run(ctx, "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal", WithDryRun(true)); err != nil {
		t.Fatal(err)
	}
}

func TestExecutioner_ListDatabase(t *testing.T) {
	ctx := t.Context()

	metricCli, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dbAdminCli, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	executioner := NewExecutioner(ctx, metricCli, dbAdminCli)

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

	metricCli, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dbAdminCli, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	executioner := NewExecutioner(ctx, metricCli, dbAdminCli)

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
