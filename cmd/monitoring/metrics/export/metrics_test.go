package export

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestExportStorageTotalByte(t *testing.T) {
	ctx := context.Background()

	buf := new(bytes.Buffer)
	if err := ExportStorageTotalByte(ctx, buf, "sinmetalcraft-monitoring-all1"); err != nil {
		t.Fatal(err)
	}
	fmt.Println(buf.String())
}

func TestExportStorageReceiveByte(t *testing.T) {
	ctx := context.Background()

	buf := new(bytes.Buffer)
	if err := ExportStorageReceiveByte(ctx, buf, "sinmetalcraft-monitoring-all1"); err != nil {
		t.Fatal(err)
	}
	fmt.Println(buf.String())
}
