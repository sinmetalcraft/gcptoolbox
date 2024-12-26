package slack

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"github.com/sinmetalcraft/gcptoolbox/internal/dataflowpbbox"
	"github.com/slack-go/slack"
)

type Service struct {
	cli *slack.Client
}

func NewService(ctx context.Context, token string) (*Service, error) {
	cli := slack.New(token)
	return &Service{cli: cli}, nil
}

func (s *Service) PostMessage(ctx context.Context, channelID string, attachment *slack.Attachment) error {
	//attachment := slack.Attachment{
	//	Color:   "good",
	//	Pretext: "some pretext",
	//	Text:    "some text",
	//	// Uncomment the following part to send a field too
	//	/*
	//		Fields: []slack.AttachmentField{
	//			slack.AttachmentField{
	//				Title: "a",
	//				Value: "no",
	//			},
	//		},
	//	*/
	//}
	_, _, err := s.cli.PostMessageContext(ctx, channelID,
		slack.MsgOptionAttachments(*attachment),
		slack.MsgOptionAsUser(true))
	if err != nil {
		return err
	}
	return nil
}

type DFRunJobNotifyMessage struct {
	ChannelID            string
	DataflowJobProjectID string
	DataflowLocation     string
	DataflowJobID        string
	DataflowJobName      string
	JobState             dataflowpb.JobState
	JobStartAt           time.Time
	JobElapsedTime       time.Duration
	QueueName            string
	Message              string
}

func (s *Service) PostMessageForDFRunJobNotify(ctx context.Context, message *DFRunJobNotifyMessage) error {
	var color string
	switch message.JobState {
	case dataflowpb.JobState_JOB_STATE_DONE:
		color = "good"
	case dataflowpb.JobState_JOB_STATE_FAILED:
		color = "danger"
	case dataflowpb.JobState_JOB_STATE_CANCELLED:
		color = "warning"
	case dataflowpb.JobState_JOB_STATE_STOPPED:
		color = "warning"
	default:
		color = "warning"
	}
	attachment := &slack.Attachment{
		Color:     color,
		Title:     message.DataflowJobName,
		TitleLink: fmt.Sprintf("https://console.cloud.google.com/dataflow/jobs/%s/%s;graphView=0?project=%s&inv=1&invt=AblDtQ", message.DataflowLocation, message.DataflowJobID, message.DataflowJobProjectID),
		Pretext:   fmt.Sprintf("%s: ", message.QueueName),
		Text:      fmt.Sprintf("State:%s\nStartAt:%s,ElastedTime:%s\n%s", dataflowpbbox.JobStateText(message.JobState), message.JobStartAt, message.JobElapsedTime, message.Message),
	}
	if err := s.PostMessage(ctx, message.ChannelID, attachment); err != nil {
		return err
	}
	return nil
}
