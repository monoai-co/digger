package github

import (
	"testing"

	"github.com/diggerhq/digger/libs/ci"
	"github.com/diggerhq/digger/libs/ci/generic"
	"github.com/diggerhq/digger/libs/digger_config"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestSelectedFilesPreserveImpactAndLegacyEntryPoints(t *testing.T) {
	projects := []digger_config.Project{
		{Name: "root", Dir: "root", Branch: digger_config.DefaultBranchName},
		{Name: "child", Dir: "child", Branch: digger_config.DefaultBranchName, DependencyProjects: []string{"root"}},
		{Name: "release", Dir: "release", Branch: "release"},
	}
	dependencies, err := digger_config.CreateProjectDependencyGraph(projects)
	require.NoError(t, err)
	config := &digger_config.DiggerConfig{Projects: projects, DependencyConfiguration: digger_config.DependencyConfiguration{Mode: digger_config.DependencyConfigurationHard}}
	files := []string{"root/main.tf", "release/main.tf"}
	provider := ci.MockPullRequestManager{ChangedFiles: files}
	event := &github.PullRequestEvent{Action: github.String("synchronize"), Repo: &github.Repository{DefaultBranch: github.String("main")}, PullRequest: &github.PullRequest{Number: github.Int(12), Base: &github.PullRequestBranch{Ref: github.String("main")}}}
	want, sources, number, err := ProcessGitHubPullRequestFiles(12, "main", "main", files, config, dependencies)
	require.NoError(t, err)
	require.Equal(t, 12, number)
	require.ElementsMatch(t, []digger_config.Project{projects[0], projects[1]}, want)
	legacy, legacySources, legacyNumber, err := ProcessGitHubPullRequestEvent(event, config, dependencies, provider)
	require.NoError(t, err)
	require.ElementsMatch(t, want, legacy)
	require.Equal(t, sources, legacySources)
	require.Equal(t, number, legacyNumber)

	comment, err := generic.ProcessIssueCommentFiles(12, files, config, dependencies)
	require.NoError(t, err)
	require.ElementsMatch(t, projects, comment.AllImpactedProjects)
	legacyComment, err := generic.ProcessIssueCommentEvent(12, config, dependencies, provider)
	require.NoError(t, err)
	require.ElementsMatch(t, comment.AllImpactedProjects, legacyComment.AllImpactedProjects)
	require.Equal(t, comment.ImpactedProjectsSourceMapping, legacyComment.ImpactedProjectsSourceMapping)

	// A later live file list changes only the legacy entry point. The selected
	// comparison remains the input to both durable impact calculations.
	provider.ChangedFiles = []string{"child/main.tf"}
	later, _, _, err := ProcessGitHubPullRequestEvent(event, config, dependencies, provider)
	require.NoError(t, err)
	require.Equal(t, []digger_config.Project{projects[1]}, later)
	selected, _, _, err := ProcessGitHubPullRequestFiles(12, "main", "main", files, config, dependencies)
	require.NoError(t, err)
	require.ElementsMatch(t, want, selected)
}
