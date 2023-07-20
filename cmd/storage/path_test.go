package storage_test

import (
	"testing"

	"github.com/sinmetalcraft/gcptoolbox/cmd/storage"
)

func TestResolutionBucketAndObjectPath(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		wantBucket string
		wantObject string
	}{
		{"gs://hoge/", "gs://hoge/", "hoge", "/"},
		{"gs://hoge//", "gs://hoge//", "hoge", "//"},
		{"gs://hoge/fuga", "gs://hoge/fuga", "hoge", "fuga"},
		{"gs://hoge/fuga/moge", "gs://hoge/fuga/moge", "hoge", "fuga/moge"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotObject, err := storage.ResolutionBucketAndObjectPath(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if gotBucket != tt.wantBucket || gotObject != tt.wantObject {
				t.Errorf("want %s %s but got %s %s", tt.wantBucket, tt.wantObject, gotBucket, gotObject)
			}
		})
	}
}

func TestResolutionBucketAndObjectPathError(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{"empty", ""},
		{"gs://", "gs://"},
		{"gs://hoge", "gs://hoge"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := storage.ResolutionBucketAndObjectPath(tt.path)
			if err == nil {
				t.Failed()
			}
			t.Log(err.Error())
		})
	}
}
