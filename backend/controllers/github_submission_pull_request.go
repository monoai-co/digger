package controllers

import (
	"errors"
	"fmt"
	"slices"

	"github.com/diggerhq/digger/backend/models"
	githubci "github.com/diggerhq/digger/libs/ci/github"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
)

// githubPullRequestPreparation separates deterministic selection from fenced
// lock changes, report publication and execution. A nonempty Outcome means no
// workflow may be dispatched, even when Jobs retains inputs for report rendering.
type githubPullRequestPreparation struct {
	Jobs              []scheduler.Job
	Projects          []digger_config.Project
	Sources           map[string]digger_config.ProjectToSourceMapping
	Command           scheduler.DiggerCommand
	LockCommand       scheduler.DiggerCommand
	ReleaseAllPRLocks bool
	CoverAll          bool
	Outcome           string
}

type githubImpactLimits struct {
	ByChangedFiles bool
	MaxProjects    int
}

func prepareGithubPullRequest(snapshot *githubSubmissionConfig, target models.GithubDeliveryTargetIntent, event *github.PullRequestEvent, limits githubImpactLimits) (*githubPullRequestPreparation, error) {
	if snapshot == nil || snapshot.Config == nil || event == nil || event.GetPullRequest() == nil || event.GetRepo() == nil || event.GetSender() == nil {
		return nil, errors.New("pull request preparation requires configuration and an accepted event")
	}
	pr := event.GetPullRequest()
	if target.Source != models.GithubDeliveryTargetSignedPullRequest || pr.GetNumber() != target.PullRequestNumber ||
		pr.GetHead().GetSHA() != target.HeadSHA || pr.GetHead().GetRef() != target.HeadRef ||
		pr.GetBase().GetSHA() != target.BaseSHA || pr.GetBase().GetRef() != target.BaseRef ||
		event.GetRepo().GetFullName() != target.RepoOwner+"/"+target.RepoName {
		return nil, errors.New("pull request preparation does not match the selected target")
	}
	result := &githubPullRequestPreparation{}
	action := event.GetAction()
	if !slices.Contains([]string{"closed", "opened", "reopened", "synchronize", "converted_to_draft"}, action) {
		result.Outcome = "action_ignored"
		return result, nil
	}
	projects, sources, err := snapshot.pullRequestImpact(target, event.GetRepo().GetDefaultBranch())
	if err != nil {
		return nil, err
	}
	result.Projects, result.Sources = projects, sources
	result.Jobs, result.CoverAll, err = githubci.ConvertGithubPullRequestEventToJobs(event, projects, nil, *snapshot.Config, false)
	if err != nil {
		return nil, err
	}
	if len(result.Jobs) == 0 {
		result.Outcome = "no_jobs"
		return result, nil
	}
	if limits.ByChangedFiles && len(projects) > len(snapshot.ChangedFiles) {
		return nil, fmt.Errorf("impacted projects (%d) exceed changed files (%d)", len(projects), len(snapshot.ChangedFiles))
	}
	force := false
	for _, label := range pr.Labels {
		force = force || label.GetName() == "digger:force"
	}
	if len(projects) > limits.MaxProjects && !force {
		return nil, fmt.Errorf("impacted projects (%d) exceed configured maximum (%d); add digger:force to override", len(projects), limits.MaxProjects)
	}
	command, err := scheduler.GetCommandFromJob(result.Jobs[0])
	if err != nil || command == nil {
		return nil, errors.New("pull request jobs have no supported command")
	}
	result.Command = *command
	if *command == scheduler.DiggerCommandNoop {
		result.Outcome = "noop"
		return result, nil
	}
	if !snapshot.Config.AllowDraftPRs && pr.GetDraft() && (action == "opened" || action == "synchronize") {
		result.Outcome = "draft_ignored"
		return result, nil
	}
	if snapshot.Config.PrLocks {
		result.LockCommand = *command
	}
	result.ReleaseAllPRLocks = *command == scheduler.DiggerCommandUnlock
	if *command == scheduler.DiggerCommandUnlock || *command == scheduler.DiggerCommandLock {
		result.Outcome = "lock_command"
		return result, nil
	}
	if !snapshot.Config.AllowDraftPRs && pr.GetDraft() {
		result.Outcome = "draft_ignored"
		return result, nil
	}
	layers, _ := scheduler.CountUniqueLayers(result.Jobs)
	if snapshot.Config.RespectLayers && layers > 1 {
		result.Outcome = "layers_require_selection"
	}
	return result, nil
}
