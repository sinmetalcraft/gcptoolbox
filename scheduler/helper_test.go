package scheduler_test

import (
	"context"
	"flag"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/goccy/go-yaml"

	"github.com/sinmetalcraft/gcptoolbox/scheduler"
)

var Update = flag.Bool("update", false, "update golden file")

const (
	baseScheduleYaml   = "base_schedule.yaml"
	insertScheduleYaml = "insert_schedule.yaml"
	updateScheduleYaml = "update_schedule.yaml"
)

func TestUpdateGolden(t *testing.T) {
	if !*Update {
		return
	}

	ctx := context.Background()

	s := newService(t)

	jobs, err := s.List(ctx, "sinmetal-ci", "asia-northeast1")
	if err != nil {
		t.Fatal(err)
	}
	{ // 実際にAPI叩いた結果をまるっとyamlで出力
		bytes, err := yaml.Marshal(jobs)
		if err != nil {
			t.Fatal(err)
		}
		fn := filepath.Join("testdata", baseScheduleYaml)
		if err := ioutil.WriteFile(fn, bytes, 0644); err != nil {
			t.Fatal(err)
		}
	}
	{ // UPDATEが存在するyamlを出力
		jobs[0].Description = "required update job"
		bytes, err := yaml.Marshal(jobs)
		if err != nil {
			t.Fatal(err)
		}
		fn := filepath.Join("testdata", updateScheduleYaml)
		if err := ioutil.WriteFile(fn, bytes, 0644); err != nil {
			t.Fatal(err)
		}
	}
	{ // INSERTが必要なyamlを出力
		var insertJobs []*scheduler.Job
		job := jobs[0]
		job.JobName.JobID = "helloNewJob"
		insertJobs = append(insertJobs, job)
		bytes, err := yaml.Marshal(insertJobs)
		if err != nil {
			t.Fatal(err)
		}
		fn := filepath.Join("testdata", insertScheduleYaml)
		if err := ioutil.WriteFile(fn, bytes, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
