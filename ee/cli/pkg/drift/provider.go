package drift

import (
	"fmt"
	core_drift "github.com/diggerhq/digger/cli/pkg/core/drift"
	ce_drift "github.com/diggerhq/digger/cli/pkg/drift"
	"github.com/diggerhq/digger/libs/ci"
	"os"
)

type DriftNotificationProviderAdvanced struct{}

func (d DriftNotificationProviderAdvanced) Get(prService ci.PullRequestService) (core_drift.Notification, error) {
	slackNotificationUrl := os.Getenv("INPUT_DRIFT_DETECTION_SLACK_NOTIFICATION_URL")
	slackBotToken := os.Getenv("INPUT_DRIFT_DETECTION_SLACK_BOT_TOKEN")
	slackChannel := os.Getenv("INPUT_DRIFT_DETECTION_SLACK_CHANNEL")
	slackNotificationAdvancedUrl := os.Getenv("INPUT_DRIFT_DETECTION_ADVANCED_SLACK_NOTIFICATION_URL")
	DriftAsGithubIssues := os.Getenv("INPUT_DRIFT_GITHUB_ISSUES")
	var notification core_drift.Notification
	if slackNotificationUrl != "" || (slackBotToken != "" && slackChannel != "") {
		notification = &ce_drift.SlackNotification{Url: slackNotificationUrl, BotToken: slackBotToken, Channel: slackChannel}
	} else if slackNotificationAdvancedUrl != "" {
		notification = NewSlackAdvancedAggregatedNotificationWithAiSummary(slackNotificationAdvancedUrl)
	} else if DriftAsGithubIssues != "" {
		notification = &ce_drift.GithubIssueNotification{GithubService: &prService}
	} else {
		return nil, fmt.Errorf("could not identify drift mode, please specify using env variable INPUT_DRIFT_DETECTION_SLACK_NOTIFICATION_URL, INPUT_DRIFT_DETECTION_ADVANCED_SLACK_NOTIFICATION_URL or INPUT_DRIFT_GITHUB_ISSUES")
	}
	return notification, nil
}
