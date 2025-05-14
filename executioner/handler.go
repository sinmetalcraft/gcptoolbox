package executioner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/googleapis/gax-go/v2/apierror"
	cloudtasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
	scutioner "github.com/sinmetalcraft/gcptoolbox/spanner/executioner"
)

const (
	SpannerDatabaseHeadPath              = "/executioner/spanner/database/head"
	SpannerDatabaseDeletePreparationPath = "/executioner/spanner/database/deletePreparation"
	SpannerDatabaseExecutionPath         = "/executioner/spanner/database/execution"
)

type Handler struct {
	spannerExecutioner *scutioner.Executioner
	cloudRunURI        string

	// executionDBQueue is 処理対象のDBを詰め込むQueue
	executionDBQueue *cloudtasksbox.Queue

	// deleteDBQueue is 削除対象となったDBを詰め込むQueue
	deleteDBQueue *cloudtasksbox.Queue
	taskService   *cloudtasksbox.Service
}

func NewHandler(ctx context.Context, spannerExecutioner *scutioner.Executioner, taskService *cloudtasksbox.Service, cloudRunURI string, cloudRunRegion string) (*Handler, error) {
	projectID, err := metadatabox.ProjectID()
	if err != nil {
		return nil, err
	}

	executionDBQueue := cloudtasksbox.Queue{
		ProjectID: projectID,
		Region:    cloudRunRegion,
		Name:      "gcptoolbox-executioner-execution-db",
	}
	deleteDBQueue := cloudtasksbox.Queue{
		ProjectID: projectID,
		Region:    cloudRunRegion,
		Name:      "gcptoolbox-executioner-delete-db",
	}
	return &Handler{
		spannerExecutioner: spannerExecutioner,
		cloudRunURI:        cloudRunURI,
		executionDBQueue:   &executionDBQueue,
		deleteDBQueue:      &deleteDBQueue,
		taskService:        taskService,
	}, nil
}

func (h *Handler) Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	switch {
	case strings.HasPrefix(r.URL.Path, SpannerDatabaseHeadPath):
		return h.HandleHead(ctx, w, r)
	case strings.HasPrefix(r.URL.Path, SpannerDatabaseDeletePreparationPath):
		return h.HandleDeletePreparation(ctx, w, r)
	case strings.HasPrefix(r.URL.Path, SpannerDatabaseExecutionPath):
		return h.HandleExecution(ctx, w, r)
	default:
		return &handlers.HTTPResponse{
			StatusCode: http.StatusNotFound,
		}
	}
}

type HeadRequest struct {
	ProjectID  string
	InstanceID string
	DryRun     bool
}

// HandleHead is 一番最初に呼ぶHandler
// 指定したSpanner InstanceのDBを全部取ってきて、Cloud Tasksに入れる
func (h *Handler) HandleHead(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *HeadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Printf("invalid request body. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}

	dbs, err := h.spannerExecutioner.ListDatabase(ctx, req.ProjectID, req.InstanceID)
	if err != nil {
		fmt.Printf("failed ListDatabase %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}

	for _, db := range dbs {
		task := &cloudtasksbox.JsonPostTask{
			Audience:    h.cloudRunURI,
			RelativeURI: fmt.Sprintf("%s%s", h.cloudRunURI, SpannerDatabaseDeletePreparationPath),
			Deadline:    0,
			Body: &DeletePreparationRequest{
				ProjectID:  req.ProjectID,
				InstanceID: req.InstanceID,
				DatabaseID: db,
				DryRun:     req.DryRun,
			},
		}
		_, err = h.taskService.CreateJsonPostTask(ctx, h.executionDBQueue, task)
		if err != nil {
			fmt.Printf("error creating json post task. %s\n", err)
			return &handlers.HTTPResponse{
				StatusCode: http.StatusInternalServerError,
			}
		}
	}
	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}

type DeletePreparationRequest struct {
	ProjectID  string
	InstanceID string
	DatabaseID string
	DryRun     bool
}

func (h *Handler) HandleDeletePreparation(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *DeletePreparationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Printf("invalid request body. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}

	ope, isExecution, err := h.spannerExecutioner.CreateBackup(ctx, req.ProjectID, req.InstanceID, req.DatabaseID)
	if err != nil {
		// TODO DB BackupIDが重複している場合はエラーにしなくていい
		apiErr, ok := apierror.FromError(err)
		if ok {
			fmt.Printf("failed Create Backup %#v\n", apiErr)
			return &handlers.HTTPResponse{
				StatusCode: http.StatusInternalServerError,
			}
		} else {
			fmt.Printf("error creating backup operation. projects/%s/instances/%s/databases/%s %s\n", req.ProjectID, req.InstanceID, req.DatabaseID, err)
			return &handlers.HTTPResponse{
				StatusCode: http.StatusInternalServerError,
			}
		}
	}
	if !isExecution {
		fmt.Printf("projects/%s/instances/%s/databases/%s is not execution target\n", req.ProjectID, req.InstanceID, req.DatabaseID)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusOK,
		}
	}
	fmt.Printf("create backup ope %s projects/%s/instances/%s/databases/%s\n", ope.Name(), req.ProjectID, req.InstanceID, req.DatabaseID)

	task := &cloudtasksbox.JsonPostTask{
		Audience:     h.cloudRunURI,
		RelativeURI:  fmt.Sprintf("%s%s", h.cloudRunURI, SpannerDatabaseExecutionPath),
		ScheduleTime: time.Now().Add(10 * time.Minute),
		Deadline:     0,
		Body: &ExecutionRequest{
			ProjectID:                 req.ProjectID,
			InstanceID:                req.InstanceID,
			DatabaseID:                req.DatabaseID,
			DatabaseBackupOperationID: ope.Name(),
			DryRun:                    req.DryRun,
		},
	}
	_, err = h.taskService.CreateJsonPostTask(ctx, h.deleteDBQueue, task)
	if err != nil {
		fmt.Printf("error creating json post task. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}
	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}

type ExecutionRequest struct {
	ProjectID                 string
	InstanceID                string
	DatabaseID                string
	DatabaseBackupOperationID string
	DryRun                    bool
}

func (h *Handler) HandleExecution(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *ExecutionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Printf("invalid request body. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}
	if err := h.spannerExecutioner.Run(ctx, req.ProjectID, req.InstanceID, req.DatabaseID, scutioner.WithDryRun(req.DryRun)); err != nil {
		fmt.Printf("error executing run. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}

	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}
