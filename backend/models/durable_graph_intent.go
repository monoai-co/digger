package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/diggerhq/digger/libs/scheduler"
)

type DurableGraphCheckRunData struct {
	Id  string
	Url string
}

type DurableGraphJobIntent struct {
	ProjectName    string          `json:"project_name"`
	OperationID    string          `json:"operation_id"`
	SerializedSpec json.RawMessage `json:"serialized_spec"`
	WorkflowFile   string          `json:"workflow_file"`
	CheckRunID     *string         `json:"check_run_id"`
	CheckRunURL    *string         `json:"check_run_url"`
	Parents        []string        `json:"parents"`
}

type DurableGraphIntent struct {
	ProtocolVersion          int                       `json:"protocol_version"`
	JobType                  scheduler.DiggerCommand   `json:"job_type"`
	JobReporterType          string                    `json:"job_reporter_type"`
	OrganisationID           uint                      `json:"organisation_id"`
	GithubInstallationID     int64                     `json:"github_installation_id"`
	Branch                   string                    `json:"branch"`
	PullRequestNumber        int                       `json:"pull_request_number"`
	RepoOwner                string                    `json:"repo_owner"`
	RepoName                 string                    `json:"repo_name"`
	RepoFullName             string                    `json:"repo_full_name"`
	CommitSHA                string                    `json:"commit_sha"`
	CommentID                *int64                    `json:"comment_id"`
	DiggerConfig             string                    `json:"digger_config"`
	AISummaryCommentID       string                    `json:"ai_summary_comment_id"`
	ReportTerraformOutput    bool                      `json:"report_terraform_output"`
	CoverAllImpactedProjects bool                      `json:"cover_all_impacted_projects"`
	VCSConnectionID          *uint                     `json:"vcs_connection_id"`
	BatchCheckRunData        *DurableGraphCheckRunData `json:"batch_check_run_data"`
	Jobs                     []DurableGraphJobIntent   `json:"jobs"`
}

func (intent DurableGraphIntent) SHA256() (string, error) {
	canonicalIntent := intent
	canonicalIntent.Jobs = append([]DurableGraphJobIntent(nil), intent.Jobs...)
	for index := range canonicalIntent.Jobs {
		canonicalIntent.Jobs[index].Parents = append([]string(nil), canonicalIntent.Jobs[index].Parents...)
		sort.Strings(canonicalIntent.Jobs[index].Parents)
	}
	sort.Slice(canonicalIntent.Jobs, func(first int, second int) bool {
		if canonicalIntent.Jobs[first].ProjectName == canonicalIntent.Jobs[second].ProjectName {
			return canonicalIntent.Jobs[first].OperationID < canonicalIntent.Jobs[second].OperationID
		}
		return canonicalIntent.Jobs[first].ProjectName < canonicalIntent.Jobs[second].ProjectName
	})
	serializedIntent, err := json.Marshal(canonicalIntent)
	if err != nil {
		return "", fmt.Errorf("marshal durable graph intent: %w", err)
	}
	digest := sha256.Sum256(serializedIntent)
	return hex.EncodeToString(digest[:]), nil
}
