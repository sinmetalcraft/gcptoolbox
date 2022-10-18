package bq2gcs

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const (
	DefaultExportMaxTableSize  = 50 * 1024 * 1024 * 1024 * 1024
	DefaultExportMaxTableCount = 100000
)

type Service struct {
	BQ *bigquery.Client
}

func NewService(ctx context.Context, bq *bigquery.Client) (*Service, error) {
	return &Service{
		BQ: bq,
	}, nil
}

type GCSReferenceForExportShardingTables struct {
	// URI is Google Cloud Storage object
	URI string

	// DestinationFormat is the format to use when writing exported files.
	// Allowed values are: CSV, Avro, JSON.  The default is CSV.
	// CSV is not supported for tables with nested or repeated fields.
	DestinationFormat bigquery.DataFormat

	// Compression specifies the type of compression to apply when writing data
	// to Google Cloud Storage, or using this GCSReference as an ExternalData
	// source with CSV or JSON SourceFormat. Default is None.
	//
	// Avro files allow additional compression types: DEFLATE and SNAPPY.
	Compression bigquery.Compression
}

type DateShardingTableTarget struct {
	// Prefix is TableID Prefix
	Prefix string

	// ExpirationDay is 期限切れ日数
	ExpirationDay int
}

func (t *DateShardingTableTarget) Match(tableID string) (bool, error) {
	if t.Prefix == "" && t.ExpirationDay == 0 {
		return true, nil
	}

	if len(t.Prefix) > 0 {
		if !strings.HasPrefix(tableID, t.Prefix) {
			return false, nil
		}
	}

	b, err := CheckExpireForDateShardingTable(tableID, t.ExpirationDay)
	if err != nil {
		return false, err
	}
	return b, nil
}

// CheckExpireForDateShardingTable is 指定したtableがexpireしているかを返す
func CheckExpireForDateShardingTable(tableID string, expireDay int) (bool, error) {
	if expireDay < 0 {
		return false, fmt.Errorf("expireDay must be positive")
	}
	c := utf8.RuneCountInString(tableID)
	td := tableID[c-8:]
	t, err := time.Parse("20060102", td)
	if err != nil {
		return false, fmt.Errorf("failed time.Parse %s", td)
	}
	expireDate := t.Add(time.Duration(expireDay) * 24 * time.Hour)

	if expireDate.Unix() >= time.Now().Unix() {
		return false, nil
	}

	return true, nil
}

type ExportShardingTablesLimit struct {
	TableSize  int64
	TableCount int64
}

func (s *Service) ExportShardingTables(ctx context.Context, to *GCSReferenceForExportShardingTables, projectID string, datasetID string, target *DateShardingTableTarget, jobFunc func(ctx context.Context, jobID string), limit *ExportShardingTablesLimit, ops ...APIOptions) ([]string, error) {
	opt := apiOptions{}
	for _, o := range ops {
		o(&opt)
	}

	max := &ExportShardingTablesLimit{
		TableSize:  limit.TableSize,
		TableCount: limit.TableCount,
	}
	if max.TableSize == 0 {
		max.TableSize = DefaultExportMaxTableSize
	}
	if max.TableCount == 0 {
		max.TableCount = DefaultExportMaxTableCount
	}
	var workTableSize int64
	var workTableCount int64

	var targetTableIDs []string
	iter := s.BQ.DatasetInProject(projectID, datasetID).Tables(ctx)
	for {
		table, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return targetTableIDs, fmt.Errorf("failed list tables : %w", err)
		}

		ok, err := target.Match(table.TableID)
		if err != nil {
			return targetTableIDs, fmt.Errorf("failed target match : %w", err)
		}
		if !ok {
			if opt.streamLogFn != nil {
				opt.streamLogFn(fmt.Sprintf("%s is not match", table.TableID))
			}
			continue
		}
		msg := fmt.Sprintf("target %s", table.TableID)
		if opt.dryRun {
			msg = fmt.Sprintf("DryRun: %s", msg)
		} else {
			meta, err := s.BQ.DatasetInProject(projectID, datasetID).Table(table.TableID).Metadata(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed get metadata table=%s : %w", table.TableID, err)
			}
			workTableSize += meta.NumBytes
			if workTableCount > max.TableSize {
				return targetTableIDs, fmt.Errorf("export table size limit. limit=%d", max.TableSize)
			}
			if workTableCount > max.TableCount {
				return targetTableIDs, fmt.Errorf("export table count limit. limit=%d", max.TableCount)
			}

			job, err := s.ExportShardingTable(ctx, to, projectID, datasetID, table.TableID)
			if err != nil {
				// TODO ErrorStatusで続きをすすめるかどうかを決めたいところではある
				return nil, fmt.Errorf("failed run job table=%s : %w", table.TableID, err)
			}
			if jobFunc != nil {
				jobFunc(ctx, job.ID())
			}
			if opt.wait {
				sts, err := job.Wait(ctx)
				if err != nil {
					return targetTableIDs, fmt.Errorf("failed job.Wait() table=%s : %w", table.TableID, err)
				}
				if sts.Err() != nil {
					return targetTableIDs, fmt.Errorf("failed job.Status.Err table=%s : %w", table.TableID, err)
				}
				if opt.streamLogFn != nil {
					opt.streamLogFn(fmt.Sprintf("%s job:%s", table.TableID, job.ID()))
				}
			}
		}
		if opt.streamLogFn != nil {
			opt.streamLogFn(msg)
		}
		targetTableIDs = append(targetTableIDs, table.TableID)
		workTableCount++
	}
	return targetTableIDs, nil
}

func (s *Service) ExportShardingTable(ctx context.Context, to *GCSReferenceForExportShardingTables, projectID string, datasetID string, tableID string) (*bigquery.Job, error) {
	u := strings.ReplaceAll(to.URI, "{{TABLE_ID}}", tableID)
	job, err := s.BQ.DatasetInProject(projectID, datasetID).Table(tableID).ExtractorTo(&bigquery.GCSReference{
		URIs:              []string{u}, // TODO 複数いることってあるか・・・？
		DestinationFormat: to.DestinationFormat,
		Compression:       to.Compression,
	}).Run(ctx)
	if err != nil {
		return nil, err
	}
	return job, nil
}
