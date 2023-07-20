package storage_test

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/storage"
	gcstoolbox "github.com/sinmetalcraft/gcptoolbox/cmd/storage"
)

func TestService_DeleteObject(t *testing.T) {
	ctx := context.Background()

	const testBucket = ""
	const testObject = ""

	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := gcs.Close(); err != nil {
			t.Log(err.Error())
		}
	}()

	w := gcs.Bucket(testBucket).Object(testObject).NewWriter(ctx)
	_, err = w.Write([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := gcstoolbox.NewService(ctx, gcs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, fmt.Sprintf("gs://%s/%s", testBucket, testObject)); err != nil {
		t.Fatal(err)
	}
}
