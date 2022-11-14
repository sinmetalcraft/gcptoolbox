package tables

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

var errNotApplicableTableType error

type Service struct {
	bq *bigquery.Client
}

func NewService(ctx context.Context, bq *bigquery.Client) (*Service, error) {
	return &Service{
		bq: bq,
	}, nil
}

// UpdateTablesExpirationFromDatasetDefaultSetting is DatasetのDefault Table ExpirationをTableにコピーする
func (s *Service) UpdateTablesExpirationFromDatasetDefaultSetting(ctx context.Context, projectID string, dataset string) error {
	ds := s.bq.DatasetInProject(projectID, dataset)
	meta, err := ds.Metadata(ctx)
	if err != nil {
		return err
	}
	dte := meta.DefaultTableExpiration

	iter := ds.Tables(ctx)
	for {
		t, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		err = s.UpdateTableExpirationFromDatasetDefaultSetting(ctx, t, dte)
		if err != nil {
			if errors.Is(err, errNotApplicableTableType) {
				fmt.Printf("%s is not applicable table type\n", t.TableID)
				continue
			}
			var gapiErr *googleapi.Error
			if errors.As(err, &gapiErr) {
				if gapiErr.Code == http.StatusNotFound {
					fmt.Printf("%s is not found\n", t.TableID)
					continue
				} else {
					return err
				}
			}
		}
		fmt.Printf("%s update expiration\n", t.TableID)
	}
	return nil
}

func (s *Service) UpdateTableExpirationFromDatasetDefaultSetting(ctx context.Context, table *bigquery.Table, expiration time.Duration) error {
	meta, err := table.Metadata(ctx)
	if err != nil {
		return err
	}

	// 実Table以外は対象外
	if meta.Type != bigquery.RegularTable {
		return errNotApplicableTableType
	}

	// TimePartitioningの場合
	if meta.TimePartitioning != nil {
		_, err := table.Update(ctx, bigquery.TableMetadataToUpdate{
			TimePartitioning: &bigquery.TimePartitioning{
				Expiration: expiration,
			},
		}, meta.ETag)
		if err != nil {
			return err
		}
		return nil
	}

	// Sharding Table等の場合
	_, err = table.Update(ctx, bigquery.TableMetadataToUpdate{
		ExpirationTime: meta.CreationTime.Add(expiration), // TODO CreationTime以外の選択肢
	}, meta.ETag)
	if err != nil {
		return err
	}
	return nil
}
