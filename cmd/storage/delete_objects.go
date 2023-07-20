package storage

import (
	"context"

	"cloud.google.com/go/storage"
)

type Service struct {
	gcs *storage.Client
}

func NewService(ctx context.Context, gcs *storage.Client) (*Service, error) {
	return &Service{
		gcs,
	}, nil
}

// DeleteObjectsFromObjectListFilePath is 指定したCloud Storageのpathに書いてあるobject listのobjectを消す
func (s *Service) DeleteObjectsFromObjectListFilePath(ctx context.Context, objectListFilePath string) {
	s.gcs.Bucket("").Object("").Delete(ctx)
}

func (s *Service) DeleteObject(ctx context.Context, path string) error {
	bucket, object, err := ResolutionBucketAndObjectPath(path)
	if err != nil {
		return err
	}
	if err := s.gcs.Bucket(bucket).Object(object).Delete(ctx); err != nil {
		return err
	}
	return nil
}
