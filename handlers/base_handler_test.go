package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/sinmetalcraft/gcptoolbox/handlers"
)

func TestBasicErrorMessage_MarshalJSON(t *testing.T) {
	resp := &handlers.HTTPResponse{
		StatusCode: http.StatusBadRequest,
		Body: &handlers.BasicErrorMessage{
			Err: fmt.Errorf("hello error"),
		},
	}
	j, err := json.Marshal(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(j), `"error":"hello error"`) {
		t.Errorf("error message does not contain \"error\":\"hello error\"")
	}
}
