package controllers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	config2 "github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/apply_requirements"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
)

type githubSubmissionEvent struct {
	repository *github.Repository
	labels     []*github.Label
	selectJobs func(*githubSubmissionConfig, models.GithubDeliveryTargetIntent, githubImpactLimits) (*githubPullRequestPreparation, []configuration.Project, bool, error)
}

func (d DiggerController) processGithubPullRequestSubmission(ctx context.Context, delivery *models.GithubWebhookDelivery, event *github.PullRequestEvent) error {
	if event == nil || event.GetPullRequest() == nil || event.GetRepo() == nil {
		return errors.New("pull request delivery is missing required event fields")
	}
	if os.Getenv("DIGGER_IGNORE_PULL_REQUEST_EVENTS") == "1" || !slices.Contains([]string{"closed", "opened", "reopened", "synchronize", "converted_to_draft"}, event.GetAction()) {
		return nil
	}
	return d.processGithubEventSubmission(ctx, delivery, githubSubmissionEvent{repository: event.GetRepo(), labels: event.GetPullRequest().Labels,
		selectJobs: func(snapshot *githubSubmissionConfig, target models.GithubDeliveryTargetIntent, limits githubImpactLimits) (*githubPullRequestPreparation, []configuration.Project, bool, error) {
			selected, err := prepareGithubPullRequest(snapshot, target, event, limits)
			if err != nil {
				return nil, nil, false, err
			}
			return selected, selected.Projects, false, nil
		}})
}

func (d DiggerController) processGithubCommentSubmission(ctx context.Context, delivery *models.GithubWebhookDelivery, event *github.IssueCommentEvent) error {
	if event == nil || event.GetIssue() == nil || event.GetComment() == nil || event.GetRepo() == nil {
		return errors.New("comment delivery is missing required event fields")
	}
	if event.GetAction() != "created" || !event.GetIssue().IsPullRequest() || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(event.GetComment().GetBody())), "digger") {
		return nil
	}
	return d.processGithubEventSubmission(ctx, delivery, githubSubmissionEvent{repository: event.GetRepo(), labels: event.GetIssue().Labels,
		selectJobs: func(snapshot *githubSubmissionConfig, target models.GithubDeliveryTargetIntent, limits githubImpactLimits) (*githubPullRequestPreparation, []configuration.Project, bool, error) {
			selected, err := prepareGithubComment(snapshot, target, event, limits)
			if err != nil {
				return nil, nil, false, err
			}
			return &selected.githubPullRequestPreparation, selected.AllProjects, selected.RequiresApplyRequirements, nil
		}})
}

func (d DiggerController) processGithubEventSubmission(ctx context.Context, delivery *models.GithubWebhookDelivery, event githubSubmissionEvent) error {
	identity := models.JobCreationIdentity{DeliveryOperationID: delivery.OperationID, DeliveryLeaseID: delivery.LeaseID,
		DatabaseIdentity: d.ControlPlaneDatabaseIdentity, WriterEpoch: d.ControlPlaneWriterEpoch, ProtocolVersion: operation.ProtocolVersion}
	stored, err := models.DB.GetGithubSubmission(ctx, identity)
	if err == nil {
		return d.resumeGithubSubmission(ctx, identity, stored)
	}
	if !errors.Is(err, models.ErrGithubSubmissionNotFound) {
		return err
	}
	provider, ok := d.GithubClientProvider.(utils.ContextGithubClientProvider)
	if !ok {
		return errors.New("durable event preparation requires a context-aware GitHub provider")
	}
	selectedTarget, err := prepareGithubDeliveryTarget(ctx, identity, delivery, models.DB, provider)
	if err != nil {
		return err
	}
	target, err := models.DecodeGithubDeliveryTarget(selectedTarget.Target)
	if err != nil {
		return err
	}
	base := models.GithubReportCreatePayload{OrganisationID: selectedTarget.OrganisationID, GithubAppID: delivery.GithubAppID,
		GithubInstallationID: delivery.InstallationIDValue(), RepoOwner: target.RepoOwner, RepoName: target.RepoName, PullRequestNumber: target.PullRequestNumber}
	freeze := func(intent models.GithubSubmissionIntent) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		stored, _, err := models.DB.RecordGithubSubmission(ctx, identity, intent)
		if err != nil {
			return err
		}
		return d.resumeGithubSubmission(ctx, identity, stored)
	}
	failure := func(reason, message string, detection *models.GithubSubmissionDetection) error {
		payload := base
		payload.ResourceKind, payload.Body = models.GithubReportResourceComment, ":x: "+message
		intent, err := models.PrepareGithubReportOnlySubmission(reason, []models.GithubReportCreatePayload{payload})
		if err != nil {
			return err
		}
		intent.Detection = detection
		return freeze(intent)
	}
	snapshot, err := loadGithubSubmissionConfig(ctx, provider, delivery.GithubAppID, delivery.InstallationIDValue(), target, event.repository.GetCloneURL())
	if err != nil {
		var configError githubSubmissionConfigError
		if errors.As(err, &configError) {
			return failure("configuration_error", fmt.Sprintf("Error loading digger config: %v", configError), nil)
		}
		return err
	}
	selected, allProjects, checkApply, err := event.selectJobs(snapshot, target, githubImpactLimits{ByChangedFiles: config2.LimitByNumOfFilesChanged(), MaxProjects: config2.MaxImpactedProjectsPerChange()})
	if err != nil {
		return failure("selection_error", err.Error(), nil)
	}
	labels := make([]string, 0, len(event.labels))
	for _, label := range event.labels {
		labels = append(labels, label.GetName())
	}
	detection := &models.GithubSubmissionDetection{DefaultBranch: event.repository.GetDefaultBranch(), Labels: labels,
		ChangedFiles: snapshot.ChangedFiles, Projects: allProjects, SourceMapping: selected.Sources}
	if checkApply {
		pr, _, err := snapshot.Service.Client.PullRequests.Get(ctx, target.RepoOwner, target.RepoName, target.PullRequestNumber)
		if err != nil {
			return err
		}
		if pr.GetHead().GetSHA() != target.HeadSHA || pr.GetBase().GetSHA() != target.BaseSHA {
			return failure("apply_target_changed", "The pull request comparison changed. Submit a new apply command for the current head.", detection)
		}
		if err := apply_requirements.CheckApplyRequirements(snapshot.Service, selected.Projects, selected.Jobs, target.PullRequestNumber, target.HeadSHA, target.BaseSHA); err != nil {
			return failure("apply_requirements_failed", err.Error(), detection)
		}
	}
	var intent models.GithubSubmissionIntent
	if selected.Outcome != "" {
		intent, err = prepareGithubSelectionOutcome(base, target, selected)
	} else {
		if err := populatePolicyFieldsForJobs(snapshot.Service, snapshot.Service, selected.Jobs, target.RepoOwner, target.PullRequestNumber); err != nil {
			return err
		}
		jobs, projects := map[string]scheduler.Job{}, map[string]configuration.Project{}
		for _, job := range selected.Jobs {
			jobs[job.ProjectName] = job
		}
		for _, project := range selected.Projects {
			projects[project.Name] = project
		}
		reporter := "lazy"
		if !snapshot.Config.Reporting.CommentsEnabled {
			reporter = "noop"
		}
		graph, graphErr := utils.PrepareDurableGraphIntent(utils.DurableJobGraphRequest{Identity: identity, JobType: selected.Command, JobReporterType: reporter,
			OrganisationID: selectedTarget.OrganisationID, Jobs: jobs, Projects: projects, ProjectsGraph: snapshot.Projects, GithubInstallationID: delivery.InstallationIDValue(),
			Branch: target.HeadRef, PullRequestNumber: target.PullRequestNumber, RepoOwner: target.RepoOwner, RepoName: target.RepoName,
			RepoFullName: target.RepoOwner + "/" + target.RepoName, CommitSHA: target.HeadSHA, DiggerConfig: snapshot.Content,
			ReportTerraformOutput: snapshot.Config.ReportTerraformOutputs, CoverAllImpactedProjects: selected.CoverAll})
		if graphErr != nil {
			return graphErr
		}
		intent.Graph = graph
		if snapshot.Config.CommentRenderMode == configuration.CommentRenderModeGroupByModule {
			locations := map[string][]string{}
			for project, source := range selected.Sources {
				if _, ok := jobs[project]; !ok {
					continue
				}
				for _, location := range source.ImpactingLocations {
					locations[location] = append(locations[location], project)
				}
			}
			for location, projects := range locations {
				intent.Sources = append(intent.Sources, models.GithubSubmissionSource{Location: location, Projects: projects})
			}
		}
		intent, err = utils.PrepareGithubSubmissionWithReports(intent, delivery.GithubAppID, selectedTarget.CreatedAt)
	}
	if err != nil {
		return err
	}
	intent.Detection = detection
	if selected.ReleaseAllPRLocks {
		intent.Locks = &models.GithubSubmissionLocks{ReleaseAll: true}
	} else if selected.LockCommand != "" && len(selected.Projects) > 0 {
		projects := make([]string, 0, len(selected.Projects))
		for _, project := range selected.Projects {
			projects = append(projects, project.Name)
		}
		owners, err := models.DB.ReadGithubSubmissionLockOwners(ctx, identity, projects)
		if err != nil {
			return err
		}
		closed, err := observeClosedGithubLockOwners(ctx, snapshot.Service.Client, target, owners)
		if errors.Is(err, models.ErrGithubSubmissionLockConflict) {
			return failure("project_locked", "An impacted project is locked by another open pull request.", detection)
		}
		if err != nil {
			return err
		}
		intent.Locks = &models.GithubSubmissionLocks{Acquire: projects, ClosedOwners: closed}
	}
	return freeze(intent)
}

func prepareGithubSelectionOutcome(base models.GithubReportCreatePayload, target models.GithubDeliveryTargetIntent, selected *githubPullRequestPreparation) (models.GithubSubmissionIntent, error) {
	payloads := []models.GithubReportCreatePayload{}
	comment := func(body string) {
		payload := base
		payload.ResourceKind, payload.Body = models.GithubReportResourceComment, body
		payloads = append(payloads, payload)
	}
	switch selected.Outcome {
	case "no_jobs":
		for _, initial := range utils.RenderGithubInitialChecks(nil) {
			payload := base
			payload.ResourceKind, payload.HeadSHA, payload.Check = models.GithubReportResourceCheckRun, target.HeadSHA, &initial.Check
			payloads = append(payloads, payload)
		}
	case "lock_command":
		comment(fmt.Sprintf(":white_check_mark: Command %s completed successfully", selected.Command))
	case "layers_require_selection":
		comment(utils.GetLayersTableForJobs(selected.Jobs))
		specs := make([]scheduler.JobJson, 0, len(selected.Jobs))
		for _, job := range selected.Jobs {
			specs = append(specs, scheduler.JobToJson(job, selected.Command, "", target.HeadRef, target.HeadSHA, "", "", configuration.Project{}))
		}
		for _, initial := range utils.RenderGithubInitialChecks(specs) {
			payload := base
			payload.ResourceKind, payload.HeadSHA, payload.Check = models.GithubReportResourceCheckRun, target.HeadSHA, &initial.Check
			payloads = append(payloads, payload)
		}
	default:
		if os.Getenv("DIGGER_REPORT_BEFORE_LOADING_CONFIG") == "1" {
			comment(":construction_worker: No execution required: " + selected.Outcome)
		}
	}
	if len(payloads) == 0 {
		return models.PrepareGithubSilentSubmission(selected.Outcome)
	}
	return models.PrepareGithubReportOnlySubmission(selected.Outcome, payloads)
}
