package backendapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildExecutionClaimRequestUsesExactGithubRuntimeIdentity(t *testing.T) {
	t.Setenv("GITHUB_RUN_ID", "123")
	t.Setenv("GITHUB_RUN_ATTEMPT", "2")
	t.Setenv("GITHUB_WORKFLOW_REF", "monoai-co/repo/.github/workflows/digger.yml@refs/heads/main")
	t.Setenv("GITHUB_WORKFLOW_SHA", strings.Repeat("a", 40))
	t.Setenv("GITHUB_ACTION_REPOSITORY", "diggerhq/digger")
	t.Setenv("GITHUB_ACTION_REF", "v1.2.3")

	request, err := BuildExecutionClaimRequest("monoai-co/repo", "root", "operation-id", 1, 7)
	require.NoError(t, err)
	require.Equal(t, int64(123), request.RunID)
	require.Equal(t, int64(2), request.RunAttempt)
	require.Equal(t, "diggerhq/digger@v1.2.3", request.ActionRef)
	require.Equal(t, strings.Repeat("a", 40), request.WorkflowSHA)
	require.Len(t, request.CLISHA256, 64)
}

func TestDiggerApiClaimsExecutionAndForwardsGrantToStatusCallbacks(t *testing.T) {
	const executionGrant = "execution-grant"
	var claimSeen bool
	var claimAttempts int
	var statusSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/jobs/job-1/execution-claims":
			claimSeen = true
			claimAttempts++
			require.Equal(t, "Bearer cli:test", request.Header.Get("Authorization"))
			var claim ExecutionClaimRequest
			require.NoError(t, json.NewDecoder(request.Body).Decode(&claim))
			require.Equal(t, "monoai-co/repo", claim.RepositoryFullName)
			require.Equal(t, "root", claim.ProjectName)
			if claimAttempts == 1 {
				response.WriteHeader(http.StatusTooEarly)
				return
			}
			if claimAttempts == 2 {
				response.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			response.Header().Set("Content-Type", "application/json")
			_, err := response.Write([]byte(`{"granted":true,"execution_grant":"execution-grant","signing_key_id":"key-v1","grant_expires_at":"2099-01-01T00:00:00Z","future_additive_field":true}`))
			require.NoError(t, err)
		case "/repos/repo/projects/root/jobs/job-1/set-status":
			statusSeen = true
			require.Equal(t, executionGrant, request.Header.Get("X-Digger-Execution-Grant"))
			response.Header().Set("Content-Type", "application/json")
			_, err := response.Write([]byte(`{"id":"batch"}`))
			require.NoError(t, err)
		default:
			http.NotFound(response, request)
		}
	}))
	t.Cleanup(server.Close)

	api := &DiggerApi{DiggerHost: server.URL, AuthToken: "cli:test", HttpClient: server.Client()}
	claim, err := api.ClaimProjectJobExecution("monoai-co/repo", "root", "job-1", ExecutionClaimRequest{RepositoryFullName: "monoai-co/repo", ProjectName: "root"})
	require.NoError(t, err)
	require.Equal(t, executionGrant, claim.ExecutionGrant)
	api.SetExecutionGrant(claim.ExecutionGrant)
	_, err = api.ReportProjectJobStatus("repo", "root", "job-1", "started", time.Now().UTC(), nil, "", "", "", "", nil)
	require.NoError(t, err)
	require.True(t, claimSeen)
	require.Equal(t, 3, claimAttempts)
	require.True(t, statusSeen)
}
