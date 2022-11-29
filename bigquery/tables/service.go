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
func (s *Service) UpdateTablesExpirationFromDatasetDefaultSetting(ctx context.Context, projectID string, dataset string, ops ...APIOptions) error {
	opt := apiOptions{}
	for _, o := range ops {
		o(&opt)
	}

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

		if err := s.UpdateTableExpirationFromDatasetDefaultSetting(ctx, t, dte, ops...); err != nil {
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
			return fmt.Errorf("failed update expiration. %s: %w", t.TableID, err)
		}
	}
	return nil
}

func (s *Service) UpdateTableExpirationFromDatasetDefaultSetting(ctx context.Context, table *bigquery.Table, expiration time.Duration, ops ...APIOptions) error {
	opt := apiOptions{}
	for _, o := range ops {
		o(&opt)
	}

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
		msg := fmt.Sprintf("%s update TimePartitioning.Expiration %s \n", table.TableID, expiration)
		if opt.dryRun {
			fmt.Printf("DryRun: %s", msg)
			return nil
		}
		_, err := table.Update(ctx, bigquery.TableMetadataToUpdate{
			TimePartitioning: &bigquery.TimePartitioning{
				Expiration: expiration, // TODO defaultPartitionExpirationMsがdatasetにある場合は、それを設定するのが正しい https://github.com/googleapis/google-cloud-go/issues/7021
			},
		}, meta.ETag)
		if err != nil {
			return err
		}
		fmt.Print(msg)
		return nil
	}

	// 通常のTableの場合
	if !meta.ExpirationTime.IsZero() && !opt.overwriteExpiration {
		// 上書き指示がなく、すでに設定されていれば、更新しない
		return ErrAlreadyExpirationSetting
	}

	var expirationTime time.Time
	switch opt.baseDate {
	case LastModifiedTime:
		expirationTime = meta.LastModifiedTime.Add(expiration)
	case TableSuffix:
		v, err := getTableSuffixDate(table.TableID)
		if err != nil {
			return err
		}
		expirationTime = v.Add(expiration)
	default:
		expirationTime = meta.CreationTime.Add(expiration)
	}

	msg := fmt.Sprintf("%s update Table.ExpirationTime %s \n", table.TableID, expirationTime)
	if opt.dryRun {
		fmt.Printf("DryRun: %s", msg)
		return nil
	}
	_, err = table.Update(ctx, bigquery.TableMetadataToUpdate{
		ExpirationTime: expirationTime,
	}, meta.ETag)
	if err != nil {
		return err
	}
	fmt.Print(msg)
	return nil
}

func getYYYYMMDD(tableID string) (string, error) {
	yyyyMMDD := tableID[len(tableID)-8:]
	_, err := time.Parse("20060102", yyyyMMDD)
	if err != nil {
		return "", err
	}
	return yyyyMMDD, nil
}

func getTableSuffixDate(tableID string) (time.Time, error) {
	yyyyMMDD := tableID[len(tableID)-8:]
	v, err := time.Parse("20060102", yyyyMMDD)
	if err != nil {
		return time.Time{}, err
	}
	return v, nil
}
