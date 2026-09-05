package controllers

import (
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestGithubCommentPreparationRetainsCommandGates(t *testing.T) {
	for _, test := range []struct {
		name, command, outcome                               string
		bot, trusted, allowBot, disableApply, draft, noFiles bool
		applyGate, unlock, wantError                         bool
		selected                                             int
	}{
		{name: "plan", command: "digger plan", selected: 2},
		{name: "apply", command: "digger apply", selected: 2, applyGate: true},
		{name: "selected project", command: "digger plan -p a", selected: 1},
		{name: "missing project", command: "digger plan -p missing", wantError: true},
		{name: "untrusted bot", command: "digger apply", bot: true, outcome: "untrusted_bot"},
		{name: "trusted bot", command: "digger apply", bot: true, trusted: true, selected: 2, applyGate: true},
		{name: "allowed bot", command: "digger plan", bot: true, allowBot: true, selected: 2},
		{name: "disabled apply", command: "digger apply", disableApply: true, outcome: "apply_comment_disabled"},
		{name: "draft apply", command: "digger apply", draft: true, selected: 2, outcome: "draft_ignored"},
		{name: "draft unlock", command: "digger unlock", draft: true, selected: 2, outcome: "lock_command", unlock: true},
		{name: "unlock", command: "digger unlock", selected: 2, outcome: "lock_command", unlock: true},
		{name: "empty comparison", command: "digger plan", noFiles: true, outcome: "no_jobs"},
		{name: "ordinary comment", command: "looks good", outcome: "comment_ignored"},
	} {
		t.Run(test.name, func(t *testing.T) {
			projects := []digger_config.Project{{Name: "a", Dir: "a", Branch: "main", Workflow: "default"}, {Name: "b", Dir: "b", Branch: "main", Workflow: "default"}}
			dependencies, err := digger_config.CreateProjectDependencyGraph(projects)
			require.NoError(t, err)
			snapshot := &githubSubmissionConfig{Config: &digger_config.DiggerConfig{Projects: projects, PrLocks: true, DisableDiggerApplyComment: test.disableApply, Workflows: map[string]digger_config.Workflow{"default": {}}}, Projects: dependencies, ChangedFiles: []string{"a/main.tf", "b/main.tf"}}
			if test.noFiles {
				snapshot.ChangedFiles = nil
			}
			if test.trusted {
				snapshot.Config.TrustedAppIDs = []int64{123}
			}
			target := models.GithubDeliveryTargetIntent{Source: models.GithubDeliveryTargetIssueCommentLookup, RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 12, HeadSHA: "head", HeadRef: "selected-feature", BaseSHA: "base", BaseRef: "main"}
			event := &github.IssueCommentEvent{Action: github.String("created"), Sender: &github.User{ID: github.Int64(123), Login: github.String("author"), Type: github.String("User")}, Repo: &github.Repository{FullName: github.String("owner/repo"), DefaultBranch: github.String("main")}, Issue: &github.Issue{Number: github.Int(12), Draft: github.Bool(test.draft), PullRequestLinks: &github.PullRequestLinks{}}, Comment: &github.IssueComment{Body: github.String(test.command)}}
			if test.bot {
				event.Sender.Type = github.String("Bot")
			}
			if test.allowBot {
				event.Issue.Labels = []*github.Label{{Name: github.String("digger:allowbot")}}
			}
			prepared, err := prepareGithubComment(snapshot, target, event, githubImpactLimits{MaxProjects: 2})
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.outcome, prepared.Outcome)
			require.Equal(t, test.applyGate, prepared.RequiresApplyRequirements)
			require.Equal(t, test.unlock, prepared.ReleaseAllPRLocks)
			require.Len(t, prepared.Projects, test.selected)
			for _, job := range prepared.Jobs {
				require.Equal(t, "selected-feature", job.RunEnvVars["PR_BRANCH"])
			}
			if test.draft {
				require.Equal(t, scheduler.DiggerCommand(""), prepared.LockCommand)
			}
			if test.selected == 1 {
				require.False(t, prepared.CoverAll)
			}
			event.Issue.Number = github.Int(13)
			_, err = prepareGithubComment(snapshot, target, event, githubImpactLimits{MaxProjects: 2})
			require.Error(t, err)
		})
	}
}
