package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	sch "cloud.google.com/go/scheduler/apiv1"
	"google.golang.org/api/iterator"
	proto "google.golang.org/genproto/googleapis/cloud/scheduler/v1"
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

type GetReq struct {
	ProjectID string
	Location  string
	JobID     string
}

func (req *GetReq) Name() string {
	return fmt.Sprintf("projects/%s/locations/%s/jobs/%s", req.ProjectID, req.Location, req.JobID)
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

			boxJobs = append(boxJobs, boxJob)
			pageToken = iter.PageInfo().Token
		}
		if pageToken == "" {
			break
		}
	}
	return boxJobs, nil
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

//func (s *schedulerService) get(ctx context.Context, req *SchedulerGetReq) {
//	job, err := s.client.GetJob(ctx, &sproto.GetJobRequest{
//		Name: req.Name(),
//	})
//	if err != nil {
//
//	}
//}
