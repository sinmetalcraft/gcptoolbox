package bq2gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
)

type ExportHandler struct {
}

type TargetTable struct {
	Project       string `json:"project"`
	Dataset       string `json:"dataset"`
	TablePrefix   string `json:"tablePrefix"`
	ExpirationDay int    `json:"expirationDay"`
}

type GCSReferenceForExportShardingTablesReq struct {
	// URI is Google Cloud Storage object
	// {{TABLE_ID}} を入れるとこれはExportするTableIDに置き換えられます
	// 対象のTableが1GBを超えることを想定してWildcardを含めてください
	// eg. gs://hoge/{{TABLE_ID}}/*.parquest
	URI string `json:"uri"`

	// DestinationFormat is the format to use when writing exported files.
	// Allowed values are: CSV, Avro, JSON.  The default is CSV.
	// CSV is not supported for tables with nested or repeated fields.
	DestinationFormat string

	// Compression specifies the type of compression to apply when writing data
	// to Google Cloud Storage, or using this GCSReference as an ExternalData
	// source with CSV or JSON SourceFormat. Default is None.
	//
	// Avro files allow additional compression types: DEFLATE and SNAPPY.
	Compression string
}

type ExportsReq struct {
	Project string                                  `json:"project"`
	Target  *TargetTable                            `json:"target"`
	ToGCS   *GCSReferenceForExportShardingTablesReq `json:"toGCS"`
	Limit   *ExportShardingTablesLimit              `json:"limit"`
}

func (h *ExportHandler) Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *ExportsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}

	project, err := metadatabox.ProjectID()
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed get project id from metadata server")},
		}
	}
	if req.Project != "" {
		project = req.Project
	}
	bq, err := bigquery.NewClient(ctx, project) // requestごとにProjectを指定できるようにClientは都度作る
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed create bigquery client. %w", err)},
		}
	}
	s, err := NewService(ctx, bq)
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed create BQ2GCS Service. %w", err)},
		}
	}

	to := &GCSReferenceForExportShardingTables{
		URI:               req.ToGCS.URI,
		DestinationFormat: bigquery.DataFormat(req.ToGCS.DestinationFormat),
		Compression:       bigquery.Compression(req.ToGCS.Compression),
	}

	_, err = s.ExportShardingTables(ctx, to, req.Target.Project, req.Target.Dataset, &DateShardingTableTarget{
		Prefix:        req.Target.TablePrefix,
		ExpirationDay: req.Target.ExpirationDay,
	}, func(ctx context.Context, jobID string) {
		// TODO Cloud TasksにJobIDを入れる
	}, &ExportShardingTablesLimit{
		TableSize:  req.Limit.TableSize,
		TableCount: req.Limit.TableCount,
	})
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed Export Tables. %w", err)},
		}
	}

	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}
