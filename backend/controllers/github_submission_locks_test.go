package controllers

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestClosedGithubLockObservationChecksExactPRAndRepository(t *testing.T) {
	for _, test := range []struct {
		name, state string
		number      int
		repository  int64
		wantError   bool
	}{
		{"closed", "closed", 7, 91, false}, {"open", "open", 7, 91, true}, {"wrong PR", "closed", 8, 91, true}, {"wrong repository", "closed", 7, 92, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/repos/owner/repo/pulls/7", r.URL.Path)
				repo := targetResolutionRepo()
				repo.ID = github.Int64(test.repository)
				require.NoError(t, json.NewEncoder(w).Encode(&github.PullRequest{Number: github.Int(test.number), State: github.String(test.state), Base: &github.PullRequestBranch{Repo: repo}}))
			})
			target := models.GithubDeliveryTargetIntent{RepositoryID: 91, RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 42}
			owners := []models.GithubSubmissionLockOwner{{ID: 1, Project: "one", PullRequestNumber: 7}, {ID: 2, Project: "two", PullRequestNumber: 7}}
			observed, err := observeClosedGithubLockOwners(context.Background(), client, target, owners)
			if test.wantError {
				require.ErrorIs(t, err, models.ErrGithubSubmissionLockConflict)
				require.Nil(t, observed)
			} else {
				require.NoError(t, err)
				require.Equal(t, owners, observed)
			}
			require.Equal(t, 1, requests)
		})
	}
}
