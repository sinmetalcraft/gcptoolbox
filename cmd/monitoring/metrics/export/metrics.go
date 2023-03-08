package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/apstndb/adcplus/tokensource"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type StorageTotalByte struct {
	ProjectID    string    `json:"projectID"`
	Bucket       string    `json:"bucket"`
	StorageClass string    `json:"storageClass"`
	Location     string    `json:"location"`
	TotalBytes   float64   `json:"totalBytes"`
	StartTime    time.Time `json:"startTime"`
}

func ExportStorageTotalByte(ctx context.Context, w io.Writer, project string) error {
	ts, err := tokensource.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	metricsClient, err := monitoring.NewMetricClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return err
	}

	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)
	var pageToken string
	for {
		req := &monitoringpb.ListTimeSeriesRequest{
			Name:   fmt.Sprintf("projects/%s", project),
			Filter: "metric.type = \"storage.googleapis.com/storage/total_bytes\"",
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(startTime),
				EndTime:   timestamppb.New(endTime),
			},
		}
		if pageToken != "" {
			req.PageToken = pageToken
		}

		iter := metricsClient.ListTimeSeries(ctx, req)
		pageToken = iter.PageInfo().Token
		for {
			raw, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			v := &StorageTotalByte{
				ProjectID:    raw.GetResource().GetLabels()["project_id"],
				Bucket:       raw.GetResource().GetLabels()["bucket_name"],
				StorageClass: raw.GetMetric().GetLabels()["storage_class"],
				Location:     raw.GetResource().GetLabels()["location"],
				TotalBytes:   raw.GetPoints()[0].GetValue().GetDoubleValue(),
				StartTime:    startTime,
			}

			j, err := json.Marshal(v)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(w, "%s\n", j)
			if err != nil {
				return err
			}
		}
		if pageToken == "" {
			break
		}
	}

	return nil
}

type StorageReceiveByte struct {
	ProjectID     string    `json:"projectID"`
	Bucket        string    `json:"bucket"`
	Location      string    `json:"location"`
	ReceivedBytes int64     `json:"receivedBytes"`
	StartTime     time.Time `json:"startTime"`
}

func ExportStorageReceiveByte(ctx context.Context, w io.Writer, project string) error {
	ts, err := tokensource.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	metricsClient, err := monitoring.NewMetricClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return err
	}

	d := durationpb.New(30 * 24 * time.Hour)

	endTime := time.Now()
	startTime := endTime.Add(-1 * 24 * 30 * time.Hour)
	var pageToken string
	for {
		req := &monitoringpb.ListTimeSeriesRequest{
			Name:   fmt.Sprintf("projects/%s", project),
			Filter: "metric.type = \"storage.googleapis.com/network/received_bytes_count\"",
			Aggregation: &monitoringpb.Aggregation{
				AlignmentPeriod:    d,
				PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_SUM,
				CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_SUM,
				GroupByFields:      []string{"resource.project_id", "resource.bucket_name", "resource.location"},
			},
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(startTime),
				EndTime:   timestamppb.New(endTime),
			},
		}
		if pageToken != "" {
			req.PageToken = pageToken
		}

		iter := metricsClient.ListTimeSeries(ctx, req)
		pageToken = iter.PageInfo().Token
		for {
			raw, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			v := &StorageReceiveByte{
				ProjectID:     raw.GetResource().GetLabels()["project_id"],
				Bucket:        raw.GetResource().GetLabels()["bucket_name"],
				Location:      raw.GetResource().GetLabels()["location"],
				ReceivedBytes: raw.GetPoints()[0].GetValue().GetInt64Value(),
				StartTime:     startTime,
			}

			j, err := json.Marshal(v)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(w, "%s\n", j)
			if err != nil {
				return err
			}
		}
		if pageToken == "" {
			break
		}
	}

	return nil
}
