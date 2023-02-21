package monitoring

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestExport(t *testing.T) {
	ctx := context.Background()

	buf := new(bytes.Buffer)
	if err := Export(ctx, buf, "sinmetalcraft-monitoring-all1"); err != nil {
		t.Fatal(err)
	}
	fmt.Println(buf.String())
}
