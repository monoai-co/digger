package utils

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/comment_utils/reporting"
	"github.com/diggerhq/digger/libs/scheduler"
)

// PrepareGithubSubmissionWithReports freezes initial reports with the graph.
// Call only for a new submission, using its persisted target selection time.
// Replays load the saved submission instead of rendering newer configuration.
func PrepareGithubSubmissionWithReports(intent models.GithubSubmissionIntent, appID int64, preparedAt time.Time) (models.GithubSubmissionIntent, error) {
	if len(intent.Reports) != 0 || preparedAt.IsZero() || appID <= 0 {
		return models.GithubSubmissionIntent{}, models.ErrGithubSubmissionIntent
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		return models.GithubSubmissionIntent{}, err
	}
	prepared, err := models.DecodeGithubSubmissionIntent(raw)
	if err != nil {
		return models.GithubSubmissionIntent{}, err
	}
	jobs := make([]scheduler.JobJson, 0, len(prepared.Graph.Jobs))
	for _, job := range prepared.Graph.Jobs {
		var spec scheduler.JobJson
		if err := json.Unmarshal(job.SerializedSpec, &spec); err != nil {
			return models.GithubSubmissionIntent{}, err
		}
		jobs = append(jobs, spec)
	}
	base := models.GithubReportCreatePayload{OrganisationID: prepared.Graph.OrganisationID, GithubAppID: appID,
		GithubInstallationID: prepared.Graph.GithubInstallationID, RepoOwner: prepared.Graph.RepoOwner, RepoName: prepared.Graph.RepoName,
		PullRequestNumber: prepared.Graph.PullRequestNumber}
	appendReport := func(report models.GithubSubmissionReport, payload models.GithubReportCreatePayload) error {
		raw, err := models.PrepareGithubReportCreatePayload(payload)
		if err != nil {
			return err
		}
		report.Payload, report.Order = raw, len(prepared.Reports)
		prepared.Reports = append(prepared.Reports, report)
		return nil
	}
	if prepared.Graph.JobReporterType != "noop" {
		comment := base
		comment.ResourceKind, comment.Body = models.GithubReportResourceComment, GetInitialJobSummaryFromJobSpecs(jobs)
		if err := appendReport(models.GithubSubmissionReport{Key: "initial:summary", Role: models.GithubSubmissionReportSummary}, comment); err != nil {
			return models.GithubSubmissionIntent{}, err
		}
	}
	for index, initial := range RenderGithubInitialChecks(jobs) {
		check := base
		check.ResourceKind, check.HeadSHA, check.Check = models.GithubReportResourceCheckRun, prepared.Graph.CommitSHA, &initial.Check
		var role models.GithubSubmissionReportRole
		switch initial.Role {
		case GithubInitialCheckProject:
			role = models.GithubSubmissionReportProject
		case GithubInitialCheckBatch:
			role = models.GithubSubmissionReportBatch
		case GithubInitialCheckCompanion:
			role = models.GithubSubmissionReportCompanion
		default:
			return models.GithubSubmissionIntent{}, models.ErrGithubSubmissionIntent
		}
		if err := appendReport(models.GithubSubmissionReport{Key: fmt.Sprintf("initial:check:%d", index), Role: role, ProjectName: initial.ProjectName, Optional: initial.Optional}, check); err != nil {
			return models.GithubSubmissionIntent{}, err
		}
	}
	for index, source := range prepared.Sources {
		comment := base
		title := fmt.Sprintf("Report for location: %s %s", source.Location, preparedAt.UTC().Format("2006-01-02 15:04:05 (MST)"))
		comment.ResourceKind, comment.Body = models.GithubReportResourceComment, reporting.AsCollapsibleComment(title, false)("")
		if err := appendReport(models.GithubSubmissionReport{Key: fmt.Sprintf("initial:source:%d", index), Role: models.GithubSubmissionReportSource, SourceLocation: source.Location}, comment); err != nil {
			return models.GithubSubmissionIntent{}, err
		}
	}
	raw, err = json.Marshal(prepared)
	if err != nil {
		return models.GithubSubmissionIntent{}, err
	}
	return models.DecodeGithubSubmissionIntent(raw)
}
