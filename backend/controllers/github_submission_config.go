package controllers

import (
	"context"
	"errors"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/ci/generic"
	githubci "github.com/diggerhq/digger/libs/ci/github"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/git_utils"
	"github.com/dominikbraun/graph"
)

type githubSubmissionConfig struct {
	Content      string
	Config       *digger_config.DiggerConfig
	Projects     graph.Graph[string, digger_config.Project]
	ChangedFiles []string
	Service      *githubci.GithubService
}

func (prepared *githubSubmissionConfig) pullRequestImpact(target models.GithubDeliveryTargetIntent, defaultBranch string) ([]digger_config.Project, map[string]digger_config.ProjectToSourceMapping, error) {
	projects, sources, _, err := githubci.ProcessGitHubPullRequestFiles(target.PullRequestNumber, defaultBranch, target.BaseRef, prepared.ChangedFiles, prepared.Config, prepared.Projects)
	return projects, sources, err
}

func (prepared *githubSubmissionConfig) commentImpact(target models.GithubDeliveryTargetIntent) (*generic.ProcessIssueCommentEventResult, error) {
	return generic.ProcessIssueCommentFiles(target.PullRequestNumber, prepared.ChangedFiles, prepared.Config, prepared.Projects)
}

// loadGithubSubmissionConfig consumes a previously persisted target. Configuration
// and impact inputs come from one fixed Git comparison, not subsequent PR reads.
func loadGithubSubmissionConfig(ctx context.Context, provider utils.ContextGithubClientProvider, appID, installationID int64, target models.GithubDeliveryTargetIntent, cloneURL string) (*githubSubmissionConfig, error) {
	if target.BaseSHA == "" || target.BaseRef == "" || target.HeadSHA == "" || provider == nil || appID <= 0 || installationID <= 0 {
		return nil, errors.New("submission configuration requires a complete selected comparison and installation")
	}
	client, token, err := provider.GetContext(ctx, appID, installationID)
	if err != nil {
		return nil, err
	}
	if client == nil || token == nil || *token == "" {
		return nil, errors.New("submission configuration requires an installation client and token")
	}
	prepared := &githubSubmissionConfig{Service: &githubci.GithubService{Client: githubClientWithoutRedirects(client), Owner: target.RepoOwner, RepoName: target.RepoName}}
	err = git_utils.CloneComparisonAndDoAction(ctx, cloneURL, target.BaseSHA, target.HeadSHA, *token, "", func(directory string, changedFiles []string) error {
		content, err := digger_config.ReadDiggerYmlFileContents(directory)
		if err != nil {
			return err
		}
		config, _, projects, _, err := digger_config.LoadDiggerConfig(directory, true, changedFiles, nil)
		if err != nil {
			return err
		}
		prepared.Content, prepared.Config, prepared.Projects = content, config, projects
		prepared.ChangedFiles = append([]string{}, changedFiles...)
		return ctx.Err()
	})
	if err != nil {
		return nil, err
	}
	return prepared, nil
}
