package deletes

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
	gcsstrings "github.com/sinmetalcraft/gcptoolbox/cmd/storage/strings"
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
func (s *Service) DeleteObjectsFromObjectListFilePath(ctx context.Context, objectListFilePath string, skipHeaderRowCount int, multiCount int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bucket, object, err := gcsstrings.ResolutionBucketAndObjectPath(objectListFilePath)
	if err != nil {
		return fmt.Errorf("invalid objectListFilePath %s :%w", objectListFilePath, err)
	}
	r, err := s.gcs.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed read objectListFilePath. gs://%s/%s :%w", bucket, object, err)
	}

	var errs []error
	errCh := make(chan error)
	go func(ctx context.Context, errs []error) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				errs = append(errs, err)
			}
		}
	}(ctx, errs)

	wg := &sync.WaitGroup{}
	ch := make(chan string)
	for i := 0; i < multiCount; i++ { // 並列にやるなら、goroutine増やす
		wg.Add(1)
		go func(ctx context.Context) {
			for {
				select {
				case path, more := <-ch:
					if !more {
						wg.Done()
						return
					}
					if err := s.DeleteObject(ctx, path); err != nil {
						if errors.Is(err, storage.ErrObjectNotExist) {
							fmt.Printf("NotExist %s\n", path)
							continue
						}
						fmt.Printf("Failed %s\n  err=%s\n", path, err)
						errCh <- err
						continue
					}
					fmt.Printf("Delete %s\n", path)
				case <-ctx.Done():
					wg.Done()
					return
				}
			}
		}(ctx)
	}

	var rowCount int
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		rowCount++
		if rowCount <= skipHeaderRowCount {
			continue
		}
		ch <- scanner.Text()
	}
	close(ch)
	wg.Wait()
	cancel()

	err = scanner.Err()
	if err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) DeleteObject(ctx context.Context, path string) error {
	bucket, object, err := gcsstrings.ResolutionBucketAndObjectPath(path)
	if err != nil {
		return err
	}
	if err := s.gcs.Bucket(bucket).Object(object).Delete(ctx); err != nil {
		return err
	}
	return nil
}
