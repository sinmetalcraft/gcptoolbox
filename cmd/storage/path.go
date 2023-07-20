package storage

import (
	"fmt"
	"strings"
)

// ResolutionBucketAndObjectPath is gs://... をbucketとobject pathに分解して返す
func ResolutionBucketAndObjectPath(path string) (string, string, error) {
	if !strings.HasPrefix(path, "gs://") {
		return "", "", fmt.Errorf("invalid Cloud Storage Path. plz format gs://xxx/xxx")
	}

	if path == "gs://" {
		return "", "", fmt.Errorf("invalid Cloud Storage Path. plz format gs://xxx/xxx")
	}

	firstSlashIndex := strings.Index(path[5:], "/")
	if firstSlashIndex < 0 {
		return "", "", fmt.Errorf("invalid Cloud Storage Path. plz format gs://xxx/xxx")
	}
	bucket := path[5 : 5+firstSlashIndex]
	object := path[5+1+len(bucket):]
	if object == "" && strings.HasSuffix(path, "/") {
		object = "/"
	}

	return bucket, object, nil
}
