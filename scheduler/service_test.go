package scheduler_test

import (
	"context"
	"fmt"
	"testing"

	sch "cloud.google.com/go/scheduler/apiv1"
	"github.com/goccy/go-yaml"

	"github.com/sinmetalcraft/gcptoolbox/scheduler"
)

func TestService_List(t *testing.T) {
	ctx := context.Background()

	s := newService(t)
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	jobs, err := s.List(ctx, "sinmetal-ci", "asia-northeast1")
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := yaml.Marshal(jobs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(bytes))
}

func newService(t *testing.T) *scheduler.Service {
	ctx := context.Background()

	client, err := sch.NewCloudSchedulerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler.NewService(ctx, client)
}
