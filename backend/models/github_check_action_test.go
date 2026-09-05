package models

import (
	"testing"

	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func legacyCheckActionFixture() (*github.CheckRunEvent, *DiggerBatch, []DiggerJob) {
	batch := &DiggerBatch{ID: uuid.New(), DiggerBatchID: "short-batch", VCS: DiggerVCSGithub, PrNumber: 42, CommitSha: "original-commit",
		BranchName: "feature/from-fork", GithubInstallationId: 123, RepoOwner: "owner", RepoName: "repo", RepoFullName: "owner/repo", CheckRunId: github.String("900")}
	jobs := []DiggerJob{{BatchID: github.String(batch.ID.String()), CheckRunId: github.String("901")}}
	event := &github.CheckRunEvent{Action: github.String("requested_action"), Installation: &github.Installation{ID: github.Int64(123)},
		Repo:            &github.Repository{ID: github.Int64(91), Name: github.String("repo"), FullName: github.String("owner/repo"), Owner: &github.User{Login: github.String("owner")}},
		CheckRun:        &github.CheckRun{ID: github.Int64(900), HeadSHA: github.String("original-commit"), App: &github.App{ID: github.Int64(456)}},
		RequestedAction: &github.RequestedAction{Identifier: "abatch:short-batch"}}
	return event, batch, jobs
}

func TestLegacyGithubCheckActionBindsBatchOrJobCheckWithoutPRLookup(t *testing.T) {
	event, batch, jobs := legacyCheckActionFixture()
	require.NoError(t, validateLegacyGithubCheckAction(event, 456, batch, jobs))
	event.CheckRun.ID = github.Int64(901)
	require.NoError(t, validateLegacyGithubCheckAction(event, 456, batch, jobs))
	require.Empty(t, event.CheckRun.PullRequests, "fork checks need not advertise a PR association")
}

func TestLegacyGithubCheckActionRejectsUnboundIdentity(t *testing.T) {
	for _, field := range []string{"app", "installation", "repository", "owner", "head", "check", "action", "identifier", "job batch", "durable batch", "durable job", "nil check"} {
		t.Run(field, func(t *testing.T) {
			event, batch, jobs := legacyCheckActionFixture()
			switch field {
			case "app":
				event.CheckRun.App.ID = github.Int64(999)
			case "installation":
				event.Installation.ID = github.Int64(999)
			case "repository":
				event.Repo.FullName = github.String("owner/other")
			case "owner":
				event.Repo.Owner.Login = github.String("other")
			case "head":
				event.CheckRun.HeadSHA = github.String("newer-commit")
			case "check":
				event.CheckRun.ID = github.Int64(999)
			case "action":
				event.Action = github.String("completed")
			case "identifier":
				event.RequestedAction.Identifier = "abatch:another-batch"
			case "job batch":
				jobs[0].BatchID = github.String(uuid.NewString())
			case "durable batch":
				batch.OperationID = github.String("durable-operation")
			case "durable job":
				jobs[0].OperationID = github.String("durable-operation")
			case "nil check":
				event.CheckRun = nil
			}
			require.ErrorIs(t, validateLegacyGithubCheckAction(event, 456, batch, jobs), ErrGithubCheckActionBinding)
		})
	}
}
