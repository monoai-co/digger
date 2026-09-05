package backendapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/stretchr/testify/require"
)

func TestExecutionClaimRefreshesOIDCForEachRetry(t *testing.T) {
	operationID := "op1_" + strings.Repeat("a", 64)
	audience, err := operation.ExecutionClaimAudience(operationID, "job-1")
	require.NoError(t, err)
	var oidcCalls, claimCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oidc" {
			require.Equal(t, "Bearer request-token", r.Header.Get("Authorization"))
			require.Equal(t, audience, r.URL.Query().Get("audience"))
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"value": fmt.Sprintf("token-%d", oidcCalls.Add(1))}))
			return
		}
		require.Equal(t, "/v1/jobs/job-1/execution-claims", r.URL.Path)
		require.Equal(t, "Bearer job-token", r.Header.Get("Authorization"))
		var request ExecutionClaimRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		attempt := claimCalls.Add(1)
		require.Equal(t, fmt.Sprintf("token-%d", attempt), request.OIDCToken)
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(ExecutionClaimResponse{Granted: true, ExecutionGrant: "grant", GrantExpiresAt: time.Now().Add(time.Hour)}))
	}))
	t.Cleanup(server.Close)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL+"/oidc?existing=value")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "request-token")
	api := &DiggerApi{DiggerHost: server.URL, AuthToken: "job-token", HttpClient: server.Client()}
	_, err = api.ClaimProjectJobExecutionContext(context.Background(), "monoai-co/sre", "root", "job-1", ExecutionClaimRequest{
		RepositoryFullName: "monoai-co/sre", ProjectName: "root", OperationID: operationID, ProtocolVersion: operation.OIDCProtocolVersion,
		ClaimExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), oidcCalls.Load())
	require.Equal(t, int32(2), claimCalls.Load())
}

func TestExecutionClaimCancellationStopsHoldRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		cancel()
	}))
	t.Cleanup(server.Close)
	api := &DiggerApi{DiggerHost: server.URL, HttpClient: server.Client()}
	started := time.Now()
	_, err := api.ClaimProjectJobExecutionContext(ctx, "monoai-co/sre", "root", "job-1", ExecutionClaimRequest{RepositoryFullName: "monoai-co/sre", ProjectName: "root"})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(started), time.Second)
}

func TestGithubOIDCRejectsRedirectAndMalformedResponse(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1) }))
	t.Cleanup(target.Close)
	for _, body := range []string{"redirect", `{"value":""}`, `{"value":"token"} {}`, `{"value":"` + strings.Repeat("a", 32769) + `"}`} {
		t.Run(fmt.Sprintf("response-%d", len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body == "redirect" {
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
					return
				}
				_, err := w.Write([]byte(body))
				require.NoError(t, err)
			}))
			t.Cleanup(server.Close)
			t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL)
			t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "secret")
			api := &DiggerApi{}
			_, err := api.githubOIDCToken(context.Background(), server.Client(), "job-audience")
			require.Error(t, err)
		})
	}
	require.Zero(t, followed.Load())
}

func TestExecutionClaimRetriesTruncatedCommittedResponse(t *testing.T) {
	var attempts atomic.Int32
	var original ExecutionClaimRequest
	grant := ExecutionClaimResponse{Granted: true, ExecutionGrant: "persisted-grant", SigningKeyID: "key-v1", GrantExpiresAt: time.Now().Add(time.Hour)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request ExecutionClaimRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		if attempts.Add(1) == 1 {
			original = request
		} else {
			require.Equal(t, original, request)
		}
		replay := grant
		replay.AlreadyGranted = attempts.Load() > 1
		require.NoError(t, json.NewEncoder(w).Encode(replay))
	}))
	t.Cleanup(server.Close)
	client := *server.Client()
	transport := client.Transport
	client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && attempts.Load() == 1 {
			response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(`{"granted":true,"execution_grant":`))
		}
		return response, err
	})
	api := &DiggerApi{DiggerHost: server.URL, HttpClient: &client}
	receipt, err := api.ClaimProjectJobExecution("monoai-co/sre", "root", "job-1", ExecutionClaimRequest{RepositoryFullName: "monoai-co/sre", ProjectName: "root"})
	require.NoError(t, err)
	require.Equal(t, int32(2), attempts.Load())
	require.True(t, receipt.AlreadyGranted)
	require.Equal(t, grant.ExecutionGrant, receipt.ExecutionGrant)
	require.Equal(t, grant.SigningKeyID, receipt.SigningKeyID)
	require.True(t, grant.GrantExpiresAt.Equal(receipt.GrantExpiresAt))
}

func TestExecutionClaimUsesPersistedDeadlineAndRetriesOIDCOutage(t *testing.T) {
	var calls atomic.Int32
	deadline := time.Now().Add(3 * time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &DiggerApi{DiggerHost: "https://unused.test", oidcTokenProvider: func(ctx context.Context, _ string) (string, error) {
		actual, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, deadline.Equal(actual))
		if calls.Add(1) == 2 {
			cancel()
		}
		return "", errGithubOIDCUnavailable
	}}
	_, err := api.ClaimProjectJobExecutionContext(ctx, "monoai-co/sre", "root", "job-1", ExecutionClaimRequest{
		RepositoryFullName: "monoai-co/sre", ProjectName: "root", OperationID: "op1_" + strings.Repeat("a", 64), ProtocolVersion: 2, ClaimExpiresAt: deadline,
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(2), calls.Load())
}

func TestExecutionClaimRejectsExpiredDeadlineBeforeRequestingOIDC(t *testing.T) {
	api := &DiggerApi{DiggerHost: "https://unused.test", oidcTokenProvider: func(context.Context, string) (string, error) {
		t.Error("OIDC requested after claim expired")
		return "", nil
	}}
	_, err := api.ClaimProjectJobExecution("monoai-co/sre", "root", "job-1", ExecutionClaimRequest{
		RepositoryFullName: "monoai-co/sre", ProjectName: "root", ProtocolVersion: 2, ClaimExpiresAt: time.Now().Add(-time.Second),
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
