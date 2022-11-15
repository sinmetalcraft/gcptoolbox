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

var (
	ErrNotApplicableTableType   = fmt.Errorf("not applicable table type")
	ErrAlreadyExpirationSetting = fmt.Errorf("already expiration setting")
)

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
	if dte == 0 {
		return fmt.Errorf("dataset did not have default table expiration")
	}
	// TODO defaultPartitionExpirationMsがdatasetにある場合は、それを設定するのが正しい https://github.com/googleapis/google-cloud-go/issues/7021

	iter := ds.Tables(ctx)
	for {
		t, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		if err := s.UpdateTableExpirationFromDatasetDefaultSetting(ctx, t, dte); err != nil {
			if errors.Is(err, ErrNotApplicableTableType) {
				fmt.Printf("%s is not applicable table type\n", t.TableID)
				continue
			}
			if errors.Is(err, ErrAlreadyExpirationSetting) {
				fmt.Printf("%s is already expiration setting\n", t.TableID)
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
			return err
		}
		fmt.Printf("%s update expiration \n", t.TableID)
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
		return ErrNotApplicableTableType
	}

	// TimePartitioningの場合
	// TODO すでに設定されている場合、上書きするかスルーするか
	if meta.TimePartitioning != nil {
		_, err := table.Update(ctx, bigquery.TableMetadataToUpdate{
			TimePartitioning: &bigquery.TimePartitioning{
				Expiration: expiration, // TODO defaultPartitionExpirationMsがdatasetにある場合は、それを設定するのが正しい https://github.com/googleapis/google-cloud-go/issues/7021
			},
		}, meta.ETag)
		if err != nil {
			return err
		}
		return nil
	}

	// Sharding Table等の場合
	if !meta.ExpirationTime.IsZero() {
		// すでに設定されている場合、更新しない
		return ErrAlreadyExpirationSetting // TODO 上書きオプション追加
	}

	_, err = table.Update(ctx, bigquery.TableMetadataToUpdate{
		ExpirationTime: meta.CreationTime.Add(expiration), // TODO CreationTime以外の選択肢
	}, meta.ETag)
	if err != nil {
		return err
	}
	return nil
}
