package ci_backends

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestTriggerWorkflowUsesRepositoryDefaultBranch(t *testing.T) {
	t.Parallel()

	var dispatchedRef string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre":
			response.Header().Set("Content-Type", "application/json")
			_, err := response.Write([]byte(`{"default_branch":"main"}`))
			require.NoError(t, err)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/digger_workflow.yml/dispatches":
			var payload struct {
				Ref string `json:"ref"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
			dispatchedRef = payload.Ref
			response.WriteHeader(http.StatusNoContent)
		default:
			http.Error(response, "unexpected request", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	client := github.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	workflowSpec := spec.Spec{
		Job: scheduler.JobJson{Branch: "untrusted-feature-branch"},
		VCS: spec.VcsSpec{
			RepoOwner:    "monoai-co",
			RepoName:     "sre",
			WorkflowFile: "digger_workflow.yml",
		},
	}

	err = (GithubActionCi{Client: client}).TriggerWorkflow(workflowSpec, "test run", "")
	require.NoError(t, err)
	require.Equal(t, "main", dispatchedRef)
}

func TestTriggerWorkflowRejectsMissingDefaultBranch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, err := response.Write([]byte(`{}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	client := github.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	workflowSpec := spec.Spec{VCS: spec.VcsSpec{RepoOwner: "monoai-co", RepoName: "sre", WorkflowFile: "digger_workflow.yml"}}
	err = (GithubActionCi{Client: client}).TriggerWorkflow(workflowSpec, "test run", "")
	require.ErrorContains(t, err, "default branch is empty")
}
