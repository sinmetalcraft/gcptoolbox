package executioner

import (
	"testing"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
)

func TestExecutioner_Run(t *testing.T) {
	ctx := t.Context()

	metricCli, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	executioner := NewExecutioner(ctx, metricCli)

	// TODO instance IDもFilter条件にいる気がする
	if err := executioner.Run(ctx, "gcpug-public-spanner", "sinmetal"); err != nil {
		t.Fatal(err)
	}
}
