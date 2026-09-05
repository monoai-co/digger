package controllers

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

type submissionConfigProvider struct {
	utils.DiggerGithubClientMockProvider
	client            *github.Client
	app, installation int64
	calls             int
}

func (p *submissionConfigProvider) GetContext(ctx context.Context, app, installation int64) (*github.Client, *string, error) {
	p.calls++
	p.app, p.installation = app, installation
	token := "fixture-token"
	return p.client, &token, ctx.Err()
}

func TestGithubSubmissionConfigReadsSelectedHeadWithoutLivePRRequests(t *testing.T) {
	if err := exec.Command("git", "var", "GIT_AUTHOR_IDENT").Run(); err != nil {
		t.Skip("git fixture commits require a configured author")
	}
	t.Setenv("DIGGER_FILENAME", "digger.yml")
	source := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = source
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
		return strings.TrimSpace(string(output))
	}
	commitConfig := func(name string) string {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(source, "digger.yml"), []byte("projects:\n- name: "+name+"\n  dir: .\n"), 0600))
		run("add", "digger.yml")
		run("commit", "-m", "Configuration fixture "+name)
		return run("rev-parse", "HEAD")
	}
	run("init", "-b", "main")
	base := commitConfig("base")
	head := commitConfig("selected")
	commitConfig("newer")
	requests := 0
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "live PR reads are not allowed", http.StatusBadRequest)
	})
	provider := &submissionConfigProvider{client: client}
	target := models.GithubDeliveryTargetIntent{RepoOwner: "owner", RepoName: "repo", HeadSHA: head, HeadRef: "deleted-branch", BaseSHA: base, BaseRef: "main"}
	prepared, err := loadGithubSubmissionConfig(context.Background(), provider, 456, 123, target, (&url.URL{Scheme: "file", Path: source}).String())
	require.NoError(t, err)
	require.Len(t, prepared.Config.Projects, 1)
	require.Equal(t, "selected", prepared.Config.Projects[0].Name)
	require.Contains(t, prepared.Content, "name: selected")
	require.Equal(t, []string{"digger.yml"}, prepared.ChangedFiles)
	impacted, _, err := prepared.pullRequestImpact(target, "main")
	require.NoError(t, err)
	require.Equal(t, prepared.Config.Projects, impacted)
	commentImpact, err := prepared.commentImpact(target)
	require.NoError(t, err)
	require.Equal(t, impacted, commentImpact.AllImpactedProjects)
	require.Zero(t, requests)
	require.EqualValues(t, 456, provider.app)
	require.EqualValues(t, 123, provider.installation)
	require.Equal(t, 1, provider.calls)
	target.BaseSHA = ""
	_, err = loadGithubSubmissionConfig(context.Background(), provider, 456, 123, target, source)
	require.Error(t, err)
	require.Equal(t, 1, provider.calls, "missing saved base must not trigger a fresh provider lookup")
}
