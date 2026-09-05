package controllers

import (
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestGithubPullRequestPreparationRetainsExecutionGates(t *testing.T) {
	for _, test := range []struct {
		name, action                                      string
		draft, allowDraft, merged, layers, noFiles, force bool
		maximum                                           int
		outcome                                           string
		lock                                              scheduler.DiggerCommand
		unlock, wantError                                 bool
	}{
		{name: "plan", action: "opened", maximum: 2, lock: scheduler.DiggerCommandPlan},
		{name: "draft", action: "opened", draft: true, maximum: 2, outcome: "draft_ignored"},
		{name: "allowed draft", action: "opened", draft: true, allowDraft: true, maximum: 2, lock: scheduler.DiggerCommandPlan},
		{name: "converted draft", action: "converted_to_draft", draft: true, maximum: 2, outcome: "lock_command", lock: scheduler.DiggerCommandUnlock, unlock: true},
		{name: "closed", action: "closed", maximum: 2, outcome: "lock_command", lock: scheduler.DiggerCommandUnlock, unlock: true},
		{name: "merged", action: "closed", merged: true, maximum: 2, lock: scheduler.DiggerCommandApply},
		{name: "layers", action: "opened", maximum: 2, layers: true, outcome: "layers_require_selection", lock: scheduler.DiggerCommandPlan},
		{name: "no projects", action: "opened", noFiles: true, maximum: 2, outcome: "no_jobs"},
		{name: "unsupported", action: "edited", maximum: 2, outcome: "action_ignored"},
		{name: "limit", action: "opened", maximum: 1, wantError: true},
		{name: "force", action: "opened", maximum: 1, force: true, lock: scheduler.DiggerCommandPlan},
	} {
		t.Run(test.name, func(t *testing.T) {
			projects := []digger_config.Project{{Name: "a", Dir: "a", Branch: "main", Workflow: "default"}, {Name: "b", Dir: "b", Branch: "main", Workflow: "default", Layer: 1}}
			dependencies, err := digger_config.CreateProjectDependencyGraph(projects)
			require.NoError(t, err)
			snapshot := &githubSubmissionConfig{Config: &digger_config.DiggerConfig{Projects: projects, AllowDraftPRs: test.allowDraft, PrLocks: true, RespectLayers: test.layers,
				Workflows: map[string]digger_config.Workflow{"default": {Configuration: &digger_config.WorkflowConfiguration{OnPullRequestPushed: []string{"digger plan"}, OnPullRequestClosed: []string{"digger unlock"}, OnCommitToDefault: []string{"digger apply"}}}}}, Projects: dependencies, ChangedFiles: []string{"a/main.tf", "b/main.tf"}}
			if test.noFiles {
				snapshot.ChangedFiles = nil
			}
			target := models.GithubDeliveryTargetIntent{Source: models.GithubDeliveryTargetSignedPullRequest, RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 12, HeadSHA: "head", HeadRef: "feature", BaseSHA: "base", BaseRef: "main"}
			event := &github.PullRequestEvent{Action: github.String(test.action), Sender: &github.User{Login: github.String("author")}, Repo: &github.Repository{FullName: github.String("owner/repo"), DefaultBranch: github.String("main")}, PullRequest: &github.PullRequest{Number: github.Int(12), Draft: github.Bool(test.draft), Merged: github.Bool(test.merged), Head: &github.PullRequestBranch{SHA: github.String("head"), Ref: github.String("feature")}, Base: &github.PullRequestBranch{SHA: github.String("base"), Ref: github.String("main")}}}
			if test.force {
				event.PullRequest.Labels = []*github.Label{{Name: github.String("digger:force")}}
			}
			prepared, err := prepareGithubPullRequest(snapshot, target, event, githubImpactLimits{MaxProjects: test.maximum})
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.outcome, prepared.Outcome)
			require.Equal(t, test.lock, prepared.LockCommand)
			require.Equal(t, test.unlock, prepared.ReleaseAllPRLocks)
			if prepared.Outcome == "" {
				require.Len(t, prepared.Jobs, 2)
				require.True(t, prepared.CoverAll)
			}
			event.PullRequest.Head.SHA = github.String("later-head")
			_, err = prepareGithubPullRequest(snapshot, target, event, githubImpactLimits{MaxProjects: 2})
			require.Error(t, err, "a target mismatch must fail before any decision")
		})
	}
}
