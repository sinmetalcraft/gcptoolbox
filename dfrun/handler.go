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
	"github.com/sinmetalcraft/gcptoolbox/internal/slack"
)

type Handler struct {
	projectID     string
	runner        *dataflowbox.ClassicTemplateRunner
	taskService   *cloudtasksbox.Service
	relativeURI   string
	checkJobQueue *cloudtasksbox.Queue

	// slackに通知を行う場合に設定するChannelID
	slackChannelID string
	// slackService is 登録するとslackにnotifyを送ってくれるようになる
	slackService *slack.Service
}

type Options struct {
	SlackChannelID string
	SlackService   *slack.Service
}

type Option func(*Options)

func WithNotifyToSlack(slackChannelID string, slackService *slack.Service) Option {
	return func(ops *Options) {
		ops.SlackChannelID = slackChannelID
		ops.SlackService = slackService
	}
}

func NewHandler(ctx context.Context, runner *dataflowbox.ClassicTemplateRunner, taskService *cloudtasksbox.Service, cloudRunURI string, ops ...Option) (*Handler, error) {
	options := &Options{}
	for _, op := range ops {
		op(options)
	}

	projectID, err := metadatabox.ProjectID()
	if err != nil {
		return nil, err
	}
	checkJobQueue := &cloudtasksbox.Queue{
		ProjectID: projectID,
		Region:    "asia-northeast1",
		Name:      "gcptoolbox-dfrun-check-job",
	}

	return &Handler{
		projectID:     projectID,
		runner:        runner,
		taskService:   taskService,
		relativeURI:   fmt.Sprintf("%s/dfrun/checkJobStatus", cloudRunURI),
		checkJobQueue: checkJobQueue,
	}, nil
}

type LaunchJobRequest struct {
	SpannerToAvroOnGCSJobRequest *dataflowbox.SpannerToAvroOnGCSJobRequest            `json:"spannerToAvroOnGCSJobRequest"`
	RuntimeEnvironment           *dataflowbox.ClassicLaunchTemplateRuntimeEnvironment `json:"runtimeEnvironment"`
}

func (h *Handler) Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	if strings.HasPrefix(r.URL.Path, "/dfrun/launchJob") {
		return h.HandleLaunchJob(ctx, w, r)
	}
	if strings.HasPrefix(r.URL.Path, "/dfrun/checkJobStatus") {
		return h.HandleCheckJobStatus(ctx, w, r)
	}
	return &handlers.HTTPResponse{
		StatusCode: http.StatusNotFound,
	}
}

func (h *Handler) HandleLaunchJob(ctx context.Context, w http.ResponseWriter, r *http.Request) *handlers.HTTPResponse {
	var req *LaunchJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// TODO slack通知
		return &handlers.HTTPResponse{
			StatusCode: http.StatusBadRequest,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}

	resp, err := h.runner.LaunchSpannerToAvroOnGCSJob(ctx, req.SpannerToAvroOnGCSJobRequest, req.RuntimeEnvironment)
	if err != nil {
		// TODO slack通知
		fmt.Printf("error launching spanner to avro on GCS job: %v\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       &handlers.BasicErrorMessage{Err: err},
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
	_, err = h.taskService.CreateJsonPostTask(ctx, h.checkJobQueue,
		&cloudtasksbox.JsonPostTask{
			Name:         taskName,
			Audience:     h.relativeURI,
			RelativeURI:  fmt.Sprintf("%s/%s", h.relativeURI, taskName), // Request Logで情報を増やすためにtaskNameをURIに付けている
			ScheduleTime: time.Now().Add(1*time.Hour + 15*time.Minute),  // Spanner Exportがおおよそ完了しそうな時間に設定
			Body:         checkJobStatusRequest,
		})
	if err != nil {
		fmt.Printf("failed cloudtasks.CreateTask :%s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       &handlers.BasicErrorMessage{Err: err},
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
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("invalid json body")},
		}
	}
	job, err := h.runner.GetJob(ctx, req.JobProjectID, req.JobLocation, req.JobID)
	if err != nil {
		fmt.Printf("failed cloudtasks.GetJob :%s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("failed GetJob: %w", err)},
		}
	}
	tasksHeader, err := cloudtasksbox.GetHeader(r)
	if err != nil {
		fmt.Printf("failed cloudtasksbox.GetHeader :%s\n", err)
		return &handlers.HTTPResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       &handlers.BasicErrorMessage{Err: fmt.Errorf("failed cloudtasksbox.GetHeader: %w", err)},
		}
	}
	switch job.GetCurrentState() {
	case dataflowpb.JobState_JOB_STATE_DONE,
		dataflowpb.JobState_JOB_STATE_FAILED,
		dataflowpb.JobState_JOB_STATE_CANCELLED,
		dataflowpb.JobState_JOB_STATE_STOPPED:

		startAt := job.GetStartTime().AsTime()
		currentStateAT := job.GetCurrentStateTime().AsTime()
		elapsedTime := startAt.Sub(currentStateAT)
		fmt.Printf("spanner export job is %s\n", job.GetCurrentState())
		if err := h.notifyToSlack(ctx, job.GetProjectId(), job.GetLocation(), job.GetId(), job.GetName(),
			job.GetCurrentState(), startAt, elapsedTime, ""); err != nil {
			return &handlers.HTTPResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       &handlers.BasicErrorMessage{Err: err},
			}
		}
		return &handlers.HTTPResponse{
			StatusCode: http.StatusOK,
		}
	default:
		// 大抵、処理中のはず
		fmt.Println("spanner export job is running")

		// 時間が長すぎる場合は、通知をして処理終了する
		if tasksHeader.RetryCount > 10 {
			fmt.Println("spanner export job state check retry count over")
			startAt := job.GetStartTime().AsTime()
			currentStateAT := job.GetCurrentStateTime().AsTime()
			elapsedTime := startAt.Sub(currentStateAT)
			if err := h.notifyToSlack(ctx, job.GetProjectId(), job.GetLocation(), job.GetId(), job.GetName(),
				job.GetCurrentState(), startAt, elapsedTime,
				"spanner export job state check retry count over. Check the status of Dataflow Job."); err != nil {
				return &handlers.HTTPResponse{
					StatusCode: http.StatusInternalServerError,
					Body:       &handlers.BasicErrorMessage{Err: err},
				}
			}
			return &handlers.HTTPResponse{
				StatusCode: http.StatusOK,
			}
		}
		return &handlers.HTTPResponse{
			StatusCode: http.StatusConflict,
		}
	}
}

func (h *Handler) handleError(ctx context.Context, statusCode int, err error) *handlers.HTTPResponse {
	return &handlers.HTTPResponse{
		StatusCode: statusCode,
		Body:       &handlers.BasicErrorMessage{Err: err},
	}
}

func (h *Handler) notifyToSlack(ctx context.Context, jobProjectID, jobLocation, jobID, jobName string, jobState dataflowpb.JobState, startAt time.Time, elapsedTime time.Duration, message string) error {
	if h.slackService == nil {
		// TODO logだけ出力して終わる
		return nil
	}
	err := h.slackService.PostMessageForDFRunJobNotify(ctx, &slack.DFRunJobNotifyMessage{
		ChannelID:            h.slackChannelID,
		DataflowJobProjectID: jobProjectID,
		DataflowLocation:     jobLocation,
		DataflowJobID:        jobID,
		DataflowJobName:      jobName,
		JobState:             jobState,
		JobStartAt:           startAt,
		JobElapsedTime:       elapsedTime,
		QueueName:            h.checkJobQueue.Name,
		Message:              "",
	})
	if err != nil {
		return fmt.Errorf("failed slack.PostMessageForDFRunJobNotify: %w", err)
	}
	return nil
}
