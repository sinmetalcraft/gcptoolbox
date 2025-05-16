package executioner_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/apstndb/adcplus/tokensource"
	"github.com/sinmetalcraft/gcptoolbox/executioner"
)

func TestHandler_HandleHead(t *testing.T) {
	ctx := t.Context()

	cloudRunURI := os.Getenv("CLOUDRUN_URI")
	if cloudRunURI == "" {
		t.SkipNow()
	}

	req := executioner.HeadRequest{
		ProjectID:  "gcpug-public-spanner",
		InstanceID: "merpay-sponsored-instance",
		DryRun:     false,
	}
	j, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	ts, err := tokensource.SmartIDTokenSource(ctx, cloudRunURI)
	if err != nil {
		t.Fatal(err)
	}
	tk, err := ts.Token()
	if err != nil {
		t.Fatal(err)
	}

	r, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s", cloudRunURI, executioner.SpannerDatabaseHeadPath), bytes.NewBuffer(j))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tk.AccessToken))
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
