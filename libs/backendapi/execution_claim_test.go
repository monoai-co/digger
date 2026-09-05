package backendapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestDiggerApiClaimsExecutionAndPreservesLegacyStatusFallbackWithoutExactContext(t *testing.T) {
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
	api.durableExecutionContext = nil
	_, err = api.ReportProjectJobStatus("repo", "root", "job-1", "started", time.Now().UTC(), nil, "", "", "", "", nil)
	require.NoError(t, err)
	require.True(t, claimSeen)
	require.Equal(t, 3, claimAttempts)
	require.True(t, statusSeen)
}

func TestDiggerApiDurableStatusCallbackRetriesStableIdentityAndBody(t *testing.T) {
	const executionGrant = "execution-grant"
	var callbackBodies [][]byte
	var callbackPaths []string
	var callbackHeaders []http.Header
	serverStatusAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/v1/jobs/job-1/execution-claims":
			response.Header().Set("Content-Type", "application/json")
			_, err := response.Write([]byte(`{"granted":true,"execution_grant":"execution-grant","signing_key_id":"key-v1","grant_expires_at":"2099-01-01T00:00:00Z","future_additive_field":true}`))
			require.NoError(t, err)
		case strings.HasPrefix(request.URL.Path, "/v1/jobs/job-1/status-callbacks/"):
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			callbackBodies = append(callbackBodies, body)
			callbackPaths = append(callbackPaths, request.URL.Path)
			callbackHeaders = append(callbackHeaders, request.Header.Clone())
			serverStatusAttempts++
			switch serverStatusAttempts {
			case 1:
				response.WriteHeader(http.StatusTooEarly)
			case 2:
				response.WriteHeader(http.StatusServiceUnavailable)
			case 3:
				response.WriteHeader(http.StatusInternalServerError)
			case 4:
				_, err = response.Write([]byte(`{"id":`))
				require.NoError(t, err)
			default:
				response.Header().Set("Content-Type", "application/json")
				_, err = response.Write([]byte(`{"id":"batch","future_additive_field":true}`))
				require.NoError(t, err)
			}
		default:
			http.NotFound(response, request)
		}
	}))
	t.Cleanup(server.Close)

	transportAttempts := 0
	transport := server.Client().Transport
	client := *server.Client()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(request.URL.Path, "/v1/jobs/job-1/status-callbacks/") {
			return transport.RoundTrip(request)
		}
		transportAttempts++
		if transportAttempts == 1 {
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			callbackBodies = append(callbackBodies, body)
			callbackPaths = append(callbackPaths, request.URL.Path)
			callbackHeaders = append(callbackHeaders, request.Header.Clone())
			return nil, errors.New("transient transport failure")
		}
		return transport.RoundTrip(request)
	})

	api := &DiggerApi{
		DiggerHost:               server.URL,
		AuthToken:                "cli:test",
		HttpClient:               &client,
		durableStatusRetryWindow: time.Second,
		durableStatusRetryDelay:  time.Millisecond,
		oidcTokenProvider:        func(context.Context, string) (string, error) { return "test-oidc-token", nil },
	}
	claimRequest := ExecutionClaimRequest{
		ClaimExpiresAt:      time.Now().Add(time.Hour),
		RepositoryFullName:  "monoai-co/repo",
		ProjectName:         "root",
		OperationID:         "op1_" + strings.Repeat("a", 64),
		ProtocolVersion:     2,
		DispatchWriterEpoch: 7,
	}
	_, err := api.ClaimProjectJobExecution("monoai-co/repo", "root", "job-1", claimRequest)
	require.NoError(t, err)

	timestamp := time.Date(2026, time.September, 5, 1, 2, 3, 4, time.UTC)
	batch, err := api.ReportProjectJobStatus("monoai-co/repo", "root", "job-1", "succeeded", timestamp, nil, "", "https://example.test/comment", "42", "output", nil)
	require.NoError(t, err)
	require.Equal(t, "batch", batch.ID)
	require.Equal(t, 6, transportAttempts)
	require.Len(t, callbackBodies, 6)
	require.Len(t, callbackPaths, 6)
	require.Len(t, callbackHeaders, 6)
	for attempt := range callbackBodies {
		require.True(t, bytes.Equal(callbackBodies[0], callbackBodies[attempt]))
		require.Equal(t, callbackPaths[0], callbackPaths[attempt])
		require.Equal(t, "Bearer cli:test", callbackHeaders[attempt].Get("Authorization"))
		require.Equal(t, executionGrant, callbackHeaders[attempt].Get("X-Digger-Execution-Grant"))
	}

	var callback durableStatusCallbackRequest
	require.NoError(t, json.Unmarshal(callbackBodies[0], &callback))
	var callbackFields map[string]any
	require.NoError(t, json.Unmarshal(callbackBodies[0], &callbackFields))
	require.ElementsMatch(t, []string{
		"callback_id",
		"repository_full_name",
		"project_name",
		"operation_id",
		"protocol_version",
		"dispatch_writer_epoch",
		"target_status",
		"expected_status_version",
		"client_timestamp",
		"job_summary",
		"job_plan_footprint",
		"pr_comment_url",
		"pr_comment_id",
		"terraform_output",
		"workflow_url",
	}, mapKeys(callbackFields))
	require.NotEqual(t, uuid.Nil, callback.CallbackID)
	require.Equal(t, "/v1/jobs/job-1/status-callbacks/"+callback.CallbackID.String(), callbackPaths[0])
	require.Equal(t, "monoai-co/repo", callback.RepositoryFullName)
	require.Equal(t, "root", callback.ProjectName)
	require.Equal(t, claimRequest.OperationID, callback.OperationID)
	require.Equal(t, 2, callback.ProtocolVersion)
	require.Equal(t, int64(7), callback.DispatchWriterEpoch)
	require.Equal(t, "succeeded", callback.TargetStatus)
	require.Equal(t, int64(2), callback.ExpectedStatusVersion)
	require.Equal(t, timestamp, callback.ClientTimestamp)
	require.Equal(t, "https://example.test/comment", callback.PrCommentURL)
	require.Equal(t, "42", callback.PrCommentID)
	require.Equal(t, "output", callback.TerraformOutput)
}

func TestDiggerApiDurableStatusCallbackRejectsRedirectsWithoutForwardingCredentials(t *testing.T) {
	hostileRequests := 0
	hostile := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		hostileRequests++
		require.Empty(t, request.Header.Get("Authorization"))
		require.Empty(t, request.Header.Get("X-Digger-Execution-Grant"))
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(hostile.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, hostile.URL+"/collect", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	api := &DiggerApi{
		DiggerHost: redirector.URL,
		AuthToken:  "cli:test",
		HttpClient: redirector.Client(),
		durableExecutionContext: &durableExecutionContext{
			RepositoryFullName: "monoai-co/repo", ProjectName: "root", DiggerJobID: "job-1",
			OperationID: "op1_operation", ProtocolVersion: 2, DispatchWriterEpoch: 7,
			ExecutionGrant: "execution-grant", GrantExpiresAt: time.Now().Add(time.Minute),
		},
	}
	_, err := api.ReportProjectJobStatus("monoai-co/repo", "root", "job-1", "succeeded", time.Now().UTC(), nil, "", "", "", "", nil)
	require.ErrorContains(t, err, "status 307")
	require.Zero(t, hostileRequests)
}

func TestDiggerApiDurableStatusCallbackRetriesUntilGrantExpiry(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("unavailable")),
			Request:    request,
		}, nil
	})}
	api := &DiggerApi{
		DiggerHost:              "https://digger.example.test",
		AuthToken:               "cli:test",
		HttpClient:              client,
		durableStatusRetryDelay: 20 * time.Millisecond,
		durableExecutionContext: &durableExecutionContext{
			RepositoryFullName: "monoai-co/repo", ProjectName: "root", DiggerJobID: "job-1",
			OperationID: "op1_operation", ProtocolVersion: 2, DispatchWriterEpoch: 7,
			ExecutionGrant: "execution-grant", GrantExpiresAt: time.Now().Add(100 * time.Millisecond),
		},
	}
	started := time.Now()
	_, err := api.ReportProjectJobStatus("monoai-co/repo", "root", "job-1", "succeeded", time.Now().UTC(), nil, "", "", "", "", nil)
	require.Error(t, err)
	require.GreaterOrEqual(t, attempts, 2)
	require.Less(t, time.Since(started), time.Second)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
