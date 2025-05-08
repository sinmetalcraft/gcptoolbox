package executioner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	cloudtasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
	scutioner "github.com/sinmetalcraft/gcptoolbox/spanner/executioner"
)

const (
	SpannerDatabaseDeletePreparationPath = "/executioner/spanner/database/deletePreparation"
	SpannerDatabaseDeletePath            = "/executioner/spanner/database/delete"
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
	case strings.HasPrefix(r.URL.Path, SpannerDatabaseDeletePreparationPath):
		return nil
	case strings.HasPrefix(r.URL.Path, SpannerDatabaseDeletePath):
		return nil
	default:
		return &handlers.HTTPResponse{
			StatusCode: http.StatusNotFound,
		}
	}
}

type HeadRequest struct {
	ProjectID  string
	InstanceID string
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

	ope, err := h.spannerExecutioner.CreateBackup(ctx, req.ProjectID, req.InstanceID, req.DatabaseID)
	if err != nil {
		// TODO DB BackupIDが重複している場合はエラーにしなくていい
		fmt.Printf("error creating backup operation. projects/%s/instances/%s/databases/%s %s\n", req.ProjectID, req.InstanceID, req.DatabaseID, err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}

	task := &cloudtasksbox.JsonPostTask{
		Audience:     h.cloudRunURI,
		RelativeURI:  fmt.Sprintf("%s%s", h.cloudRunURI, SpannerDatabaseDeletePath),
		ScheduleTime: time.Now().Add(10 * time.Minute),
		Deadline:     0,
		Body: &DeleteDatabaseRequest{
			ProjectID:                 req.ProjectID,
			InstanceID:                req.InstanceID,
			DatabaseID:                req.DatabaseID,
			DatabaseBackupOperationID: ope.Name(),
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

type DeleteDatabaseRequest struct {
	ProjectID                 string
	InstanceID                string
	DatabaseID                string
	DatabaseBackupOperationID string
}

func (h *Handler) HandleDelete(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *DeleteDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Printf("invalid request body. %s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}
	done, err := h.spannerExecutioner.DeleteDatabase(ctx, req.ProjectID, req.InstanceID, req.DatabaseID, req.DatabaseBackupOperationID)
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}
	if !done {
		// TODO もう一回
	}
	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}
