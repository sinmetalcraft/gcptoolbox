package deletes_test

import (
	"context"
	"errors"
	"fmt"

	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	deletetoolbox "github.com/sinmetalcraft/gcptoolbox/cmd/storage/deletes"
)

const testBucket = "gcptoolbox-ci"

func TestService_DeleteObject(t *testing.T) {
	ctx := context.Background()

	testObject := uuid.New().String()

	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := gcs.Close(); err != nil {
			t.Log(err.Error())
		}
	}()

	// test data 作成
	w := gcs.Bucket(testBucket).Object(testObject).NewWriter(ctx)
	_, err = w.Write([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// test start
	s, err := deletetoolbox.NewService(ctx, gcs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, fmt.Sprintf("gs://%s/%s", testBucket, testObject)); err != nil {
		t.Fatal(err)
	}
}

func TestService_DeleteObjectsFromObjectListFilePath(t *testing.T) {
	ctx := context.Background()

	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := gcs.Close(); err != nil {
			t.Log(err.Error())
		}
	}()

	// test data 作成
	testPathPrefix := fmt.Sprintf("%s-%s", time.Now().Format("20060102-150405"), uuid.New().String())
	t.Logf("testdata gs://%s/%s", testBucket, testPathPrefix)
	var targetObjectList []string
	for i := 0; i < 3; i++ {
		testObject := fmt.Sprintf("%s/%s", testPathPrefix, uuid.New().String())
		w := gcs.Bucket(testBucket).Object(testObject).NewWriter(ctx)
		_, err = w.Write([]byte("hello world"))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		targetObjectList = append(targetObjectList, testObject)
	}

	// target delete list 作成
	targetDeleteObjectListPath := fmt.Sprintf("%s/delete-list.csv", testPathPrefix)
	targetDeleteObjectListFullPath := fmt.Sprintf("gs://%s/%s", testBucket, targetDeleteObjectListPath)
	w := gcs.Bucket(testBucket).Object(targetDeleteObjectListPath).NewWriter(ctx)
	for _, o := range targetObjectList {
		_, err = w.Write([]byte(fmt.Sprintf("gs://%s/%s\n", testBucket, o)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// test start
	s, err := deletetoolbox.NewService(ctx, gcs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObjectsFromObjectListFilePath(ctx, targetDeleteObjectListFullPath, 8); err != nil {
		t.Fatal(err)
	}

	// objectが消えていることを確認
	for _, o := range targetObjectList {
		_, err := gcs.Bucket(testBucket).Object(o).Attrs(ctx)
		if errors.Is(err, storage.ErrObjectNotExist) {
			//noop
		} else {
			t.Fatal(err)
		}
	}
}
