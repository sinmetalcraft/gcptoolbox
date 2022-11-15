package contexter_test

import (
	"context"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"testing"
)

func TestProjectID(t *testing.T) {
	ctx := context.Background()

	const project = "hoge"
	ctx = contexter.WithProjectID(ctx, project)
	v, ok := contexter.ProjectID(ctx)
	if !ok {
		t.Errorf("not found project id\n")
		return
	}
	if g, e := v, project; e != g {
		t.Errorf("want %s but got %s\n", e, g)
	}
}
