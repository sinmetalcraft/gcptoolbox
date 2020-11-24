package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	sch "cloud.google.com/go/scheduler/apiv1"
	"github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/iterator"
	proto "google.golang.org/genproto/googleapis/cloud/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	client *sch.CloudSchedulerClient
}

func NewService(ctx context.Context, client *sch.CloudSchedulerClient) *Service {
	return &Service{client: client}
}

type JobName struct {
	ProjectID string
	Location  string
	JobID     string
}

func (jn *JobName) Name() string {
	return fmt.Sprintf("projects/%s/locations/%s/jobs/%s", jn.ProjectID, jn.Location, jn.JobID)
}

func NameToJobName(jobName string) (*JobName, error) {
	l := strings.Split(jobName, "/")
	if len(l) < 6 {
		return nil, fmt.Errorf("invalid format. requreid projects/{PROJECT_ID}/locations/{LOCATION}/jobs/{JOB_ID} but got %s", jobName)
	}
	return &JobName{
		ProjectID: l[1],
		Location:  l[3],
		JobID:     l[5],
	}, nil
}

type Job struct {
	JobName             *JobName
	Description         string
	Schedule            string
	TimeZone            string
	AttemptDeadline     *Duration            `yaml:",omitempty"`
	RetryConfig         *RetryConfig         `yaml:",omitempty"`
	PubSubTarget        *PubSubTarget        `yaml:",omitempty"`
	AppEngineHttpTarget *AppEngineHttpTarget `yaml:",omitempty"`
	HttpTarget          *HttpTarget          `yaml:",omitempty"`
}

type RetryConfig struct {
	RetryCount         int32
	MaxRetryDuration   *Duration `yaml:",omitempty"`
	MinBackoffDuration *Duration `yaml:",omitempty"`
	MaxBackoffDuration *Duration `yaml:",omitempty"`
	MaxDoublings       int32
}

func ProtoToRetryConfig(config *proto.RetryConfig) *RetryConfig {
	if config == nil {
		return nil
	}
	rc := &RetryConfig{}
	rc.RetryCount = config.GetRetryCount()
	rc.MaxRetryDuration = &Duration{config.GetMaxRetryDuration().AsDuration()}
	rc.MinBackoffDuration = &Duration{config.GetMinBackoffDuration().AsDuration()}
	rc.MaxBackoffDuration = &Duration{config.GetMaxBackoffDuration().AsDuration()}
	rc.MaxDoublings = config.GetMaxDoublings()
	return rc
}

type PubSubTarget struct {
	TopicName  string
	Data       interface{} `yaml:",omitempty"`
	Attributes map[string]string
}

type AppEngineHttpTarget struct {
	HttpMethod       string
	AppEngineRouting *AppEngineRouting `yaml:",omitempty"`
	RelativeUri      string
	Headers          map[string]string
	Body             string `yaml:",omitempty"`
}

type AppEngineRouting struct {
	Service  string
	Version  string
	Instance string
	Host     string
}

func ProtoToAppEngineRouting(routing *proto.AppEngineRouting) *AppEngineRouting {
	if routing == nil {
		return nil
	}
	return &AppEngineRouting{
		Service:  routing.GetService(),
		Version:  routing.GetVersion(),
		Instance: routing.GetInstance(),
		Host:     routing.GetHost(),
	}
}

type HttpTarget struct {
	Uri        string
	HttpMethod string
	Headers    map[string]string
	Body       interface{} `yaml:",omitempty"`
	OAuthToken *OAuthToken `yaml:",omitempty"`
	OIDCToken  *OIDCToken  `yaml:",omitempty"`
}

type OAuthToken struct {
	ServiceAccountEmail string
	Scope               string
}

type OIDCToken struct {
	ServiceAccountEmail string
	Audience            string
}

func (s *Service) Close() error {
	return s.client.Close()
}

func (s *Service) Create(ctx context.Context) {
	j := &proto.Job{
		Name:            "",
		Description:     "",
		Target:          nil,
		Schedule:        "",
		TimeZone:        "",
		UserUpdateTime:  nil,
		State:           0,
		Status:          nil,
		ScheduleTime:    nil,
		LastAttemptTime: nil,
		RetryConfig:     nil,
		AttemptDeadline: nil,
	}
	j.Target = &proto.Job_HttpTarget{
		HttpTarget: &proto.HttpTarget{
			Uri:                 "",
			HttpMethod:          0,
			Headers:             nil,
			Body:                nil,
			AuthorizationHeader: nil,
		},
	}
	_ = &proto.CreateJobRequest{
		Parent: "",
		Job: &proto.Job{
			Name:            "",
			Description:     "",
			Target:          nil,
			Schedule:        "",
			TimeZone:        "",
			UserUpdateTime:  nil,
			State:           0,
			Status:          nil,
			ScheduleTime:    nil,
			LastAttemptTime: nil,
			RetryConfig:     nil,
			AttemptDeadline: nil,
		},
	}
}

func (s *Service) List(ctx context.Context, project string, location string) ([]*Job, error) {
	var boxJobs []*Job
	var pageToken string
	for {
		req := &proto.ListJobsRequest{
			Parent: fmt.Sprintf("projects/%s/locations/%s", project, location),
		}
		if pageToken != "" {
			req.PageToken = pageToken
		}
		iter := s.client.ListJobs(ctx, req)
		for {
			job, err := iter.Next()
			if err == iterator.Done {
				pageToken = ""
				break
			} else if err != nil {
				return nil, err
			}

			boxJob, err := s.toBoxJob(job)
			if err != nil {
				return nil, err
			}

			boxJobs = append(boxJobs, boxJob)
			pageToken = iter.PageInfo().Token
		}
		if pageToken == "" {
			break
		}
	}
	return boxJobs, nil
}

func (s *Service) toBoxJob(job *proto.Job) (*Job, error) {
	jobName, err := NameToJobName(job.GetName())
	if err != nil {
		return nil, err
	}
	boxJob := &Job{
		JobName:     jobName,
		Description: job.GetDescription(),
		Schedule:    job.GetSchedule(),
		TimeZone:    job.GetTimeZone(),
		RetryConfig: ProtoToRetryConfig(job.GetRetryConfig()),
	}
	if job.GetAttemptDeadline() != nil {
		boxJob.AttemptDeadline = &Duration{job.GetAttemptDeadline().AsDuration()}
	}

	ptj := job.GetPubsubTarget()
	if ptj != nil {
		var d interface{}
		if err := json.Unmarshal(ptj.GetData(), &d); err != nil {
			return nil, err
		}
		boxJob.PubSubTarget = &PubSubTarget{
			TopicName:  ptj.GetTopicName(),
			Data:       d,
			Attributes: ptj.GetAttributes(),
		}
	}
	aht, err := s.parseAppEngineHttpTarget(job.GetAppEngineHttpTarget())
	if err != nil {
		return nil, err
	}
	boxJob.AppEngineHttpTarget = aht

	ht, err := s.parseHttpTarget(job.GetHttpTarget())
	if err != nil {
		return nil, err
	}
	boxJob.HttpTarget = ht

	return boxJob, nil
}

func (s *Service) parseAppEngineHttpTarget(arg *proto.AppEngineHttpTarget) (*AppEngineHttpTarget, error) {
	if arg == nil {
		return nil, nil
	}
	ret := &AppEngineHttpTarget{
		HttpMethod:       arg.GetHttpMethod().String(),
		AppEngineRouting: ProtoToAppEngineRouting(arg.GetAppEngineRouting()),
		RelativeUri:      arg.GetRelativeUri(),
		Headers:          arg.GetHeaders(),
		Body:             string(arg.GetBody()),
	}
	return ret, nil
}

func (s *Service) parseHttpTarget(arg *proto.HttpTarget) (*HttpTarget, error) {
	if arg == nil {
		return nil, nil
	}

	ret := &HttpTarget{
		Uri:        arg.GetUri(),
		HttpMethod: arg.GetHttpMethod().String(),
		Headers:    arg.GetHeaders(),
		OAuthToken: nil,
		OIDCToken:  nil,
	}
	if arg.GetHttpMethod() == proto.HttpMethod_POST && arg.GetBody() != nil {
		ret.Body = string(arg.GetBody())
	}
	if arg.GetOauthToken() != nil {
		ret.OAuthToken = &OAuthToken{
			ServiceAccountEmail: arg.GetOauthToken().GetServiceAccountEmail(),
			Scope:               arg.GetOauthToken().GetScope(),
		}
	}
	if arg.GetOidcToken() != nil {
		ret.OIDCToken = &OIDCToken{
			ServiceAccountEmail: arg.GetOidcToken().GetServiceAccountEmail(),
			Audience:            arg.GetOidcToken().GetAudience(),
		}
	}

	return ret, nil
}

func (s *Service) Get(ctx context.Context, jobName *JobName) (*Job, error) {
	j, err := s.client.GetJob(ctx, &proto.GetJobRequest{Name: jobName.Name()})
	if err != nil {
		return nil, err
	}
	boxJob, err := s.toBoxJob(j)
	if err != nil {
		return nil, err
	}
	return boxJob, nil
}

// Diff is 渡された Jobs と GCP Project の Jobs を比較して、新たに追加する必要があるもの、更新する必要があるものを返す
func (s *Service) CheckUpsertJobs(ctx context.Context, jobs []*Job) (insertJobs []*Job, updateJobs []*Job, err error) {
	for _, job := range jobs {
		got, err := s.Get(ctx, job.JobName)
		sts, ok := status.FromError(err)
		if ok && sts.Code() == codes.NotFound {
			insertJobs = append(insertJobs, job)
			continue
		} else if err != nil {
			return nil, nil, err
		}
		if !s.equal(ctx, job, got) {
			updateJobs = append(updateJobs, job)
		}
	}
	return
}

func (s *Service) equal(ctx context.Context, job1 *Job, job2 *Job) bool {
	return cmp.Equal(job1, job2)
}

func (s *Service) ReadYamlFile(ctx context.Context, path string) ([]*Job, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("failed file close. path=%s : err=%#v", path, err)
		}
	}()
	var jobs []*Job
	if err := yaml.NewDecoder(file).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
