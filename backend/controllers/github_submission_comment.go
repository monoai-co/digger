package controllers

import (
	"errors"
	"slices"
	"strings"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/ci/generic"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
)

type githubCommentPreparation struct {
	githubPullRequestPreparation
	AllProjects               []digger_config.Project
	RequiresApplyRequirements bool
}

// prepareGithubComment retains signed command and label inputs, using only the
// saved PR comparison for branch selection and impact. Apply requirements remain
// a mandatory provider-read gate before this preparation becomes a submission.
func prepareGithubComment(snapshot *githubSubmissionConfig, target models.GithubDeliveryTargetIntent, event *github.IssueCommentEvent, limits githubImpactLimits) (*githubCommentPreparation, error) {
	if snapshot == nil || snapshot.Config == nil || event == nil || event.GetIssue() == nil || event.GetComment() == nil || event.GetRepo() == nil || event.GetSender() == nil {
		return nil, errors.New("comment preparation requires configuration and an accepted event")
	}
	if target.Source != models.GithubDeliveryTargetIssueCommentLookup || event.GetIssue().GetNumber() != target.PullRequestNumber ||
		event.GetRepo().GetFullName() != target.RepoOwner+"/"+target.RepoName || target.HeadSHA == "" || target.HeadRef == "" || target.BaseSHA == "" || target.BaseRef == "" {
		return nil, errors.New("comment preparation does not match the selected target")
	}
	result := &githubCommentPreparation{}
	commandText := event.GetComment().GetBody()
	cleaned := strings.ToLower(strings.TrimSpace(commandText))
	if event.GetAction() != "created" || !event.GetIssue().IsPullRequest() || !strings.HasPrefix(cleaned, "digger") {
		result.Outcome = "comment_ignored"
		return result, nil
	}
	labels := event.GetIssue().Labels
	allowBot := false
	for _, label := range labels {
		allowBot = allowBot || label.GetName() == "digger:allowbot"
	}
	if event.GetSender().GetType() == "Bot" && !allowBot {
		actorID := event.GetComment().GetUser().GetID()
		if actorID == 0 {
			actorID = event.GetSender().GetID()
		}
		if !slices.Contains(snapshot.Config.TrustedAppIDs, actorID) {
			result.Outcome = "untrusted_bot"
			return result, nil
		}
	}
	if snapshot.Config.DisableDiggerApplyComment && strings.HasPrefix(cleaned, "digger apply") {
		result.Outcome = "apply_comment_disabled"
		return result, nil
	}
	command, err := scheduler.GetCommandFromComment(commandText)
	if err != nil || command == nil {
		return nil, errors.New("comment has no supported command")
	}
	result.Command = *command
	impact, err := snapshot.commentImpact(target)
	if err != nil {
		return nil, err
	}
	result.AllProjects, result.Sources = impact.AllImpactedProjects, impact.ImpactedProjectsSourceMapping
	selected, err := generic.FilterOutProjectsFromComment(result.AllProjects, commandText)
	if err != nil {
		return nil, err
	}
	result.Projects = generic.FilterTargetBranchForImpactedProjects(selected, event.GetRepo().GetDefaultBranch(), target.BaseRef)
	result.Jobs, result.CoverAll, err = generic.ConvertIssueCommentEventToJobs(event.GetRepo().GetFullName(), event.GetSender().GetLogin(), target.PullRequestNumber, commandText, result.Projects, result.AllProjects, snapshot.Config.Workflows, target.HeadRef, event.GetRepo().GetDefaultBranch(), false)
	if err != nil {
		return nil, err
	}
	if err := limits.check(len(result.Projects), len(snapshot.ChangedFiles), labels); err != nil {
		return nil, err
	}
	result.ReleaseAllPRLocks = *command == scheduler.DiggerCommandUnlock
	if !snapshot.Config.AllowDraftPRs && event.GetIssue().GetDraft() {
		result.Outcome = "draft_ignored"
		if result.ReleaseAllPRLocks {
			result.Outcome = "lock_command"
		}
		return result, nil
	}
	if snapshot.Config.PrLocks {
		result.LockCommand = *command
	}
	if *command == scheduler.DiggerCommandLock || *command == scheduler.DiggerCommandUnlock {
		result.Outcome = "lock_command"
		return result, nil
	}
	result.RequiresApplyRequirements = *command == scheduler.DiggerCommandApply
	if len(result.Jobs) == 0 {
		result.Outcome = "no_jobs"
	}
	return result, nil
}
