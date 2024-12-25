package dfrun_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/sinmetalcraft/gcptoolbox/dfrun"
	dataflowbox "github.com/sinmetalcraft/gcptoolbox/dfrun/dataflow"
)

func TestHandler_LaunchJobRequest(t *testing.T) {
	cloudRunURI := os.Getenv("CLOUDRUN_URI")
	if cloudRunURI == "" {
		t.SkipNow()
	}

	v := dfrun.LaunchJobRequest{
		SpannerToAvroOnGCSJobRequest: &dataflowbox.SpannerToAvroOnGCSJobRequest{
			ProjectID:                    "sinmetal-ci", // TODO 未指定だとRunと同じProjectにしても良い気がするな
			Location:                     "us-central1",
			OutputDir:                    "gs://sinmetal-ci-spanner-export/",
			OutputDirAddCurrentDateJST:   true,
			AvroTempDirectory:            "gs://sinmetal-ci-work-us-central1/avro/",
			SnapshotTime:                 time.Time{},
			SnapshotTimeJSTDayChangeTime: false,
			SpannerProjectID:             "gcpug-public-spanner",
			SpannerInstanceID:            "merpay-sponsored-instance",
			SpannerDatabaseID:            "sinmetal",
			DataBoostEnabled:             true,
		},
		RuntimeEnvironment: &dataflowbox.ClassicLaunchTemplateRuntimeEnvironment{
			ServiceAccountEmail: "dataflow-worker@sinmetal-ci.iam.gserviceaccount.com",
			TempLocation:        "gs://sinmetal-ci-work-us-central1/temp/",
		},
	}
	j, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(j))

	r, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/dfrun/launchJob", cloudRunURI), bytes.NewBuffer(j))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Log(err)
		}
	}()
	fmt.Println(resp.Status)
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(respBody))
}
