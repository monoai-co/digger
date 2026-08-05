package drift

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	core_drift "github.com/diggerhq/digger/cli/pkg/core/drift"
)

const (
	// Slack collapses messages beyond ~4000 characters, so larger content is
	// split into parts of at most this size (pre-existing behaviour).
	slackMessageLimit = 4000
	// Max plan characters posted directly to a channel via an incoming
	// webhook. Webhooks cannot thread replies, so this bounds channel spam.
	webhookPlanLimit = 12000
	// Max plan characters posted as threaded replies via a bot token. Slack
	// rejects single messages above 40000 chars; threads keep the channel to
	// one message, so the cap here bounds the number of replies instead.
	threadPlanLimit = 40000

	defaultSlackApiBase = "https://slack.com/api"
)

type SlackNotification struct {
	Url string
	// Optional: when BotToken and Channel are set the notification is sent
	// via chat.postMessage and the plan goes into a thread under the header
	// message instead of flooding the channel.
	BotToken string
	Channel  string
	// Overridable for tests; defaults to https://slack.com/api
	ApiBase string
}

func SplitCodeBlocks(message string) []string {
	var res []string

	if strings.Count(message, "```") < 2 {
		res = append(res, message)
		return res
	}

	regex := regexp.MustCompile("\n")
	split := regex.Split(message, -1)
	part := ""
	for _, line := range split {
		if len(part+line) > slackMessageLimit {
			res = append(res, part+"\n"+line+"\n```")
			part = "```\n" + line
		} else {
			part = part + "\n" + line
		}
	}
	if len(part) > 0 {
		res = append(res, part)
	}
	return res
}

// TruncatePlan cuts plan down to at most maxChars characters (at a line
// boundary where possible) and appends a note saying how much was omitted.
func TruncatePlan(plan string, maxChars int) (string, bool) {
	if len(plan) <= maxChars {
		return plan, false
	}
	cut := plan[:maxChars]
	if idx := strings.LastIndex(cut, "\n"); idx > 0 {
		cut = cut[:idx]
	}
	omitted := len(plan) - len(cut)
	return cut + fmt.Sprintf("\n... plan truncated (%d of %d characters shown, %d omitted) — full plan in the workflow logs", len(cut), len(plan), omitted), true
}

// splitIntoFencedChunks splits text into chunks of at most chunkSize
// characters at line boundaries, each wrapped in a ``` code fence.
func splitIntoFencedChunks(text string, chunkSize int) []string {
	var chunks []string
	var part strings.Builder
	for _, line := range strings.Split(text, "\n") {
		// oversized single line: hard-split it
		for len(line) > chunkSize {
			if part.Len() > 0 {
				chunks = append(chunks, "```\n"+part.String()+"\n```")
				part.Reset()
			}
			chunks = append(chunks, "```\n"+line[:chunkSize]+"\n```")
			line = line[chunkSize:]
		}
		if part.Len()+len(line)+1 > chunkSize && part.Len() > 0 {
			chunks = append(chunks, "```\n"+part.String()+"\n```")
			part.Reset()
		}
		if part.Len() > 0 {
			part.WriteString("\n")
		}
		part.WriteString(line)
	}
	if part.Len() > 0 {
		chunks = append(chunks, "```\n"+part.String()+"\n```")
	}
	return chunks
}

func driftHeader(projectName string, repoFullName string, lastChange *core_drift.LastChange) string {
	lastChangeLine := ""
	if lastChange != nil {
		lastChangeLine = fmt.Sprintf(":bust_in_silhouette: *Last change by:* %s (`%s`, %s)\n\n", lastChange.Author, lastChange.Commit, lastChange.When)
	}
	return fmt.Sprintf(
		":warning: *Infrastructure Drift Detected* :warning:\n\n"+
			":file_folder: *Project:* `%s`\n"+
			":books: *Repository:* `%s`\n\n"+
			"%s",
		projectName, repoFullName, lastChangeLine,
	)
}

func (slack *SlackNotification) useThreads() bool {
	return slack.BotToken != "" && slack.Channel != ""
}

func (slack *SlackNotification) SendNotificationForProject(projectName string, repoFullName string, plan string, lastChange *core_drift.LastChange) error {
	header := driftHeader(projectName, repoFullName, lastChange)

	if slack.useThreads() {
		return slack.sendThreaded(header, plan)
	}

	plan, truncated := TruncatePlan(plan, webhookPlanLimit)
	if truncated {
		slog.Warn("drift plan truncated for slack webhook notification", "project", projectName)
	}
	message := header + fmt.Sprintf(":memo: *Terraform Plan:*\n```\n%v\n```\n\n", plan)
	parts := SplitCodeBlocks(message)
	for _, part := range parts {
		err := SendSlackMessage(slack.Url, part)
		if err != nil {
			slog.Error("failed to send slack drift request", "error", err)
			return err
		}
	}

	return nil
}

// sendThreaded posts the header to the channel and the plan as replies in
// its thread, so the channel only ever sees a single message per project.
func (slack *SlackNotification) sendThreaded(header string, plan string) error {
	ts, err := slack.postMessage(header+":memo: *Terraform Plan in thread* :thread:", "")
	if err != nil {
		return err
	}

	plan, truncated := TruncatePlan(plan, threadPlanLimit)
	if truncated {
		slog.Warn("drift plan truncated for slack thread notification")
	}
	for _, chunk := range splitIntoFencedChunks(plan, slackMessageLimit) {
		if _, err := slack.postMessage(chunk, ts); err != nil {
			return err
		}
	}
	return nil
}

// postMessage calls chat.postMessage; when threadTs is non-empty the message
// is posted as a reply in that thread. Returns the created message's ts.
func (slack *SlackNotification) postMessage(text string, threadTs string) (string, error) {
	apiBase := slack.ApiBase
	if apiBase == "" {
		apiBase = defaultSlackApiBase
	}

	payload := map[string]string{
		"channel": slack.Channel,
		"text":    text,
	}
	if threadTs != "" {
		payload["thread_ts"] = threadTs
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal slack message: %v", err)
	}

	request, err := http.NewRequest("POST", apiBase+"/chat.postMessage", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create slack request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+slack.BotToken)

	resp, err := (&http.Client{}).Do(request)
	if err != nil {
		return "", fmt.Errorf("failed to send slack request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read slack response: %v", err)
	}
	var apiResp struct {
		Ok    bool   `json:"ok"`
		Ts    string `json:"ts"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", fmt.Errorf("failed to parse slack response: %v", err)
	}
	if !apiResp.Ok {
		return "", fmt.Errorf("slack api error: %v", apiResp.Error)
	}
	return apiResp.Ts, nil
}

func (slack *SlackNotification) SendErrorNotificationForProject(projectName string, repoFullName string, err error) error {
	message := fmt.Sprintf(
		":rotating_light: *Error While Drift Processing* :rotating_light:\n\n"+
			":file_folder: *Project:* `%s`\n"+
			":books: *Repository:* `%s`\n\n"+
			":warning: *Error Details:*\n```\n%v\n```\n\n"+
			"_Please check the workflow logs for more information._",
		projectName, repoFullName, err,
	)

	if slack.useThreads() {
		_, sendErr := slack.postMessage(message, "")
		return sendErr
	}
	return SendSlackMessage(slack.Url, message)
}

func (slack *SlackNotification) Flush() error {
	return nil
}
