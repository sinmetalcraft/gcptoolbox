package dfrun

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"github.com/google/uuid"
	cloudtasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
	dataflowbox "github.com/sinmetalcraft/gcptoolbox/dfrun/dataflow"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
)

type Handler struct {
	projectID   string
	runner      *dataflowbox.ClassicTemplateRunner
	taskService *cloudtasksbox.Service
	relativeURI string
}

func NewHandler(ctx context.Context, runner *dataflowbox.ClassicTemplateRunner, taskService *cloudtasksbox.Service, cloudRunURI string) (*Handler, error) {
	projectID, err := metadatabox.ProjectID()
	if err != nil {
		return nil, err
	}

	return &Handler{
		projectID:   projectID,
		runner:      runner,
		taskService: taskService,
		relativeURI: fmt.Sprintf("%s/dfrun/checkJobStatus", cloudRunURI),
	}, nil
}

type LaunchJobRequest struct {
	SpannerToAvroOnGCSJobRequest *dataflowbox.SpannerToAvroOnGCSJobRequest            `json:"spannerToAvroOnGCSJobRequest"`
	RuntimeEnvironment           *dataflowbox.ClassicLaunchTemplateRuntimeEnvironment `json:"runtimeEnvironment"`
}

func (h *Handler) Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	if strings.HasPrefix(r.URL.Path, "dfrun/launchJob") {
		return h.HandleLaunchJob(ctx, w, r)
	}
	if strings.HasPrefix(r.URL.Path, "dfrun/checkJobStatus") {
		return h.HandleCheckJobStatus(ctx, w, r)
	}
	return &handlers.HTTPResponse{
		StatusCode: http.StatusNotFound,
	}
}

func (h *Handler) HandleLaunchJob(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *LaunchJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}

	resp, err := h.runner.LaunchSpannerToAvroOnGCSJob(ctx, req.SpannerToAvroOnGCSJobRequest, req.RuntimeEnvironment)
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}
	job := resp.GetJob()

	checkJobStatusRequest := &CheckJobStatusRequest{
		JobProjectID: job.GetProjectId(),
		JobLocation:  job.GetLocation(),
		JobID:        job.GetId(),
	}

	// Cloud Tasksに投入
	taskName := fmt.Sprintf("%s-%s", time.Now().Format(time.DateOnly), uuid.New().String())
	_, err = h.taskService.CreateJsonPostTask(ctx, &cloudtasksbox.Queue{},
		&cloudtasksbox.JsonPostTask{
			Name:         taskName,
			Audience:     "",
			RelativeURI:  "",
			ScheduleTime: time.Now().Add(1*time.Hour + 15*time.Minute),
			Body:         checkJobStatusRequest,
		})
	if err != nil {
		fmt.Printf("failed cloudtasks.CreateTask :%s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
		}
	}

	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}

type CheckJobStatusRequest struct {
	JobProjectID string `json:"jobProjectId"`
	JobLocation  string `json:"jobLocation"`
	JobID        string `json:"jobId"`
}

func (h *Handler) HandleCheckJobStatus(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *CheckJobStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}
	job, err := h.runner.GetJob(ctx, req.JobProjectID, req.JobLocation, req.JobID)
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed GetJob: %w", err)},
		}
	}
	tasksHeader, err := cloudtasksbox.GetHeader(r)
	if err != nil {
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       handlers.BasicErrorMessage{Err: fmt.Errorf("failed cloudtasksbox.GetHeader: %w", err)},
		}
	}
	switch job.GetCurrentState() {
	case dataflowpb.JobState_JOB_STATE_DONE:
		// Slackに完了通知
		fmt.Println("job is done")
	case dataflowpb.JobState_JOB_STATE_FAILED:
		// Slackに失敗通知
		fmt.Println("job is failed")
	case dataflowpb.JobState_JOB_STATE_CANCELLED:
		// Slackにキャンセル通知
		fmt.Println("job is cancelled")
	case dataflowpb.JobState_JOB_STATE_STOPPED:
		// Slackに停止通知
		fmt.Println("job is stopped")
	default:
		// 処理中だったら
		return &handlers.HTTPResponse{
			StatusCode: http.StatusConflict,
		}
	}

	if tasksHeader.RetryCount > 10 {
		// TODO 通知をして終わるみたいなところある
	}

	// Slackに完了通知
	return &handlers.HTTPResponse{
		StatusCode: http.StatusOK,
	}
}
