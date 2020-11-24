package scheduler_test

import (
	"context"
	"path/filepath"
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
	_, err = yaml.Marshal(jobs)
	if err != nil {
		t.Fatal(err)
	}
}

func TestService_CheckUpsertJobs(t *testing.T) {
	ctx := context.Background()

	s := newService(t)
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	cases := []struct {
		name          string
		fileName      string
		wantInsertLen int
		wantUpdateLen int
	}{
		{"change nothing", baseScheduleYaml, 0, 0},
		{"insert", insertScheduleYaml, 1, 0},
		{"update", updateScheduleYaml, 0, 1},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fn := filepath.Join("testdata", tt.fileName)
			jobs, err := s.ReadYamlFile(ctx, fn)
			if err != nil {
				t.Fatal(err)
			}

			insertJobs, updateJobs, err := s.CheckUpsertJobs(ctx, jobs)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.wantInsertLen, len(insertJobs); e != g {
				t.Errorf("want insert len %d but got %d", e, g)
			}
			if e, g := tt.wantUpdateLen, len(updateJobs); e != g {
				t.Errorf("want update len %d but got %d", e, g)
			}
		})
	}
}

func newService(t *testing.T) *scheduler.Service {
	ctx := context.Background()

	client, err := sch.NewCloudSchedulerClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return scheduler.NewService(ctx, client)
}
