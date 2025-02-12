package slack_test

import (
	"context"

	"os"

	"testing"

	slackbox "github.com/sinmetalcraft/gcptoolbox/internal/slack"
	"github.com/slack-go/slack"
)

func TestService_PostMessage(t *testing.T) {
	ctx := context.Background()

	token := os.Getenv("GCPTOOLBOX_SLACK_OAUTH_TOKEN")
	if token == "" {
		t.Skip("GCPTOOLBOX_SLACK_OAUTH_TOKEN is not set")
	}

	s, err := slackbox.NewService(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	err = s.PostMessage(ctx, "C09GK6H3Q", &slack.Attachment{
		Color:         "#36a64f",
		Fallback:      "",
		CallbackID:    "",
		ID:            0,
		AuthorID:      "",
		AuthorName:    "author_name",
		AuthorSubname: "",
		AuthorLink:    "http://flickr.com/bobby/",
		AuthorIcon:    "https://storage.googleapis.com/sinmetal/sinmetal_320.jpg",
		Title:         "Hello Title",
		TitleLink:     "",
		Pretext:       "サムネイルはどうした？出てこないか？",
		Text:          "Hello Text",
		ImageURL:      "https://storage.googleapis.com/sinmetal/sinmetal-merpay.png",
		ThumbURL:      "https://storage.googleapis.com/sinmetal/sinmetal-merpay.png",
		ServiceName:   "",
		ServiceIcon:   "",
		FromURL:       "",
		OriginalURL:   "",
		Fields: []slack.AttachmentField{
			slack.AttachmentField{
				Title: "Filed Title",
				Value: "Field Value",
				Short: false,
			},
		},
		Actions:    nil,
		MarkdownIn: []string{"text"},
		Blocks:     slack.Blocks{},
		Footer:     "footer",
		FooterIcon: "https://emoji.slack-edge.com/TLC9K4D7G/user_payout_dev/2bbb229e9a9a28e6.png",
		Ts:         "",
	})
	if err != nil {
		t.Fatal(err)
	}
}
