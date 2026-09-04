package ci_backends

import (
	"context"
	"encoding/json"
	"errors"
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
				Ref              string `json:"ref"`
				ReturnRunDetails bool   `json:"return_run_details"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
			dispatchedRef = payload.Ref
			require.False(t, payload.ReturnRunDetails)
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

func TestDurableTriggerWorkflowReturnsExactRunDetails(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/repos/monoai-co/sre/actions/workflows/42/dispatches", request.URL.Path)
		require.Equal(t, "2026-03-10", request.Header.Get("X-GitHub-Api-Version"))
		var payload struct {
			Ref              string `json:"ref"`
			ReturnRunDetails bool   `json:"return_run_details"`
		}
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Equal(t, "main", payload.Ref)
		require.True(t, payload.ReturnRunDetails)
		response.Header().Set("Content-Type", "application/json")
		_, err := response.Write([]byte(`{"workflow_run_id":901}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	client := github.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	workflowSpec := spec.Spec{VCS: spec.VcsSpec{RepoOwner: "monoai-co", RepoName: "sre", WorkflowFile: "digger_workflow.yml"}}
	details, err := (GithubActionCi{Client: client}).TriggerWorkflowContextAtRefWithRunDetails(context.Background(), workflowSpec, "run", "", "main", 42)
	require.NoError(t, err)
	require.Equal(t, int64(901), details.RunID)
}

func TestDurableTriggerWorkflowRejectsLegacyNoContentAsAmbiguous(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client := github.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	workflowSpec := spec.Spec{VCS: spec.VcsSpec{RepoOwner: "monoai-co", RepoName: "sre", WorkflowFile: "digger_workflow.yml"}}
	_, err = (GithubActionCi{Client: client}).TriggerWorkflowContextAtRefWithRunDetails(context.Background(), workflowSpec, "run", "", "main", 42)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWorkflowDispatchAcceptanceAmbiguous))
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
