package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type missingDeliveryTargetStore struct{ *models.Database }

func (missingDeliveryTargetStore) GetGithubDeliveryTarget(context.Context, models.JobCreationIdentity) (*models.GithubDeliveryTarget, error) {
	return nil, gorm.ErrRecordNotFound
}

func TestGithubDeliveryTargetMissingDeliveryDoesNotResolveProvider(t *testing.T) {
	provider := &targetResolutionProvider{}
	_, err := prepareGithubDeliveryTarget(context.Background(), models.JobCreationIdentity{}, targetResolutionComment(t), missingDeliveryTargetStore{}, provider)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	require.Zero(t, provider.calls)
}

type targetResolutionProvider struct {
	utils.DiggerGithubClientMockProvider
	client                *github.Client
	err                   error
	appID, installationID int64
	calls                 int
}

func (provider *targetResolutionProvider) GetContext(_ context.Context, appID, installationID int64) (*github.Client, *string, error) {
	provider.calls++
	provider.appID, provider.installationID = appID, installationID
	return provider.client, nil, provider.err
}

func targetResolutionDelivery(t *testing.T, event string, payload any) *models.GithubWebhookDelivery {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	operationID, err := operation.Derive("github-webhook-delivery", "github-app:456", "delivery:target-resolution")
	require.NoError(t, err)
	installationID := int64(123)
	return &models.GithubWebhookDelivery{DeliveryID: "target-resolution", OperationID: operationID.String(),
		GithubAppID: 456, InstallationID: &installationID, RepositoryFullName: "owner/repo", EventType: event,
		Payload: raw, PayloadSHA256: hex.EncodeToString(digest[:])}
}

func targetResolutionRepo() *github.Repository {
	return &github.Repository{ID: github.Int64(91), Name: github.String("repo"), FullName: github.String("owner/repo"), Owner: &github.User{Login: github.String("owner")}}
}

func targetResolutionPR() *github.PullRequest {
	return &github.PullRequest{Number: github.Int(42), Base: &github.PullRequestBranch{Repo: targetResolutionRepo()},
		Head: &github.PullRequestBranch{SHA: github.String("original-commit"), Ref: github.String("feature/change"),
			Repo: &github.Repository{ID: github.Int64(92), FullName: github.String("fork/repo")}}}
}

func targetResolutionComment(t *testing.T) *models.GithubWebhookDelivery {
	return targetResolutionCommentWithLink(t, "https://api.github.com/repos/owner/repo/pulls/42")
}

func targetResolutionCommentWithLink(t *testing.T, link string) *models.GithubWebhookDelivery {
	return targetResolutionDelivery(t, "issue_comment", &github.IssueCommentEvent{Action: github.String("created"), Repo: targetResolutionRepo(),
		Installation: &github.Installation{ID: github.Int64(123)},
		Issue:        &github.Issue{Number: github.Int(42), PullRequestLinks: &github.PullRequestLinks{URL: github.String(link)}}})
}

func TestGithubDeliveryTargetUsesConfiguredClientNotMarkerOrigin(t *testing.T) {
	requests := 0
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/repos/owner/repo/pulls/42" {
			http.Error(w, "wrong target", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(targetResolutionPR())
	})
	delivery := targetResolutionCommentWithLink(t, "https://ignored-origin.invalid/api/v3/repos/owner/repo/pulls/42")
	target, err := resolveGithubDeliveryTarget(context.Background(), delivery, &targetResolutionProvider{client: client})
	require.NoError(t, err)
	require.Equal(t, 42, target.PullRequestNumber)
	require.Equal(t, 1, requests)
}

func TestGithubDeliveryTargetSignedPRDoesNotFetchNewerHead(t *testing.T) {
	delivery := targetResolutionDelivery(t, "pull_request", &github.PullRequestEvent{Action: github.String("opened"), Number: github.Int(42),
		Repo: targetResolutionRepo(), Installation: &github.Installation{ID: github.Int64(123)}, PullRequest: targetResolutionPR()})
	target, err := resolveGithubDeliveryTarget(context.Background(), delivery, nil)
	require.NoError(t, err)
	require.Equal(t, "original-commit", target.HeadSHA)
	require.Equal(t, "feature/change", target.HeadRef)
	require.Equal(t, int64(91), target.RepositoryID)
}

func TestGithubDeliveryTargetCommentResolvesExactSignedPR(t *testing.T) {
	for _, mismatch := range []string{"none", "number", "base repository", "empty head"} {
		t.Run(mismatch, func(t *testing.T) {
			requests := 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodGet || r.URL.Path != "/repos/owner/repo/pulls/42" {
					http.Error(w, "wrong target", 400)
					return
				}
				pr := targetResolutionPR()
				switch mismatch {
				case "number":
					pr.Number = github.Int(99)
				case "base repository":
					pr.Base.Repo.ID = github.Int64(777)
				case "empty head":
					pr.Head.SHA = nil
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(pr)
			})
			provider := &targetResolutionProvider{client: client}
			target, err := resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), provider)
			if mismatch == "none" {
				require.NoError(t, err)
				require.Equal(t, 42, target.PullRequestNumber)
				require.Equal(t, "original-commit", target.HeadSHA)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, requests)
			require.Equal(t, int64(456), provider.appID)
			require.Equal(t, int64(123), provider.installationID)
		})
	}
}

func TestGithubDeliveryTargetRejectsBeforeProviderAndPropagatesAuthFailure(t *testing.T) {
	delivery := targetResolutionComment(t)
	delivery.PayloadSHA256 = "invalid"
	provider := &targetResolutionProvider{}
	_, err := resolveGithubDeliveryTarget(context.Background(), delivery, provider)
	require.Error(t, err)
	require.Zero(t, provider.calls)
	provider.err = errors.New("installation unavailable")
	_, err = resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), provider)
	require.ErrorIs(t, err, provider.err)
	require.Equal(t, 1, provider.calls)
}

func TestGithubDeliveryTargetMissingProviderFailsClosed(t *testing.T) {
	_, err := resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), nil)
	require.Error(t, err)
	provider := &targetResolutionProvider{}
	_, err = resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), provider)
	require.Error(t, err)
	require.Equal(t, 1, provider.calls)
}

func TestGithubDeliveryTargetLookupPropagatesProviderFailure(t *testing.T) {
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	})
	_, err := resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), &targetResolutionProvider{client: client})
	var responseError *github.ErrorResponse
	require.ErrorAs(t, err, &responseError)
	require.Equal(t, http.StatusServiceUnavailable, responseError.Response.StatusCode)
}

func TestGithubDeliveryTargetLookupHonorsCancelledContext(t *testing.T) {
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unexpected request", 500) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := resolveGithubDeliveryTarget(ctx, targetResolutionComment(t), &targetResolutionProvider{client: client})
	require.ErrorIs(t, err, context.Canceled)
}

func TestGithubDeliveryTargetDoesNotForwardInstallationTokenToRedirect(t *testing.T) {
	var sinkRequests atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sinkRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(targetResolutionPR())
	}))
	t.Cleanup(sink.Close)
	client, server := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
	})
	base := client.BaseURL
	transport := server.Client().Transport
	client = github.NewClient(&http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("Authorization", "Bearer installation-test")
		return transport.RoundTrip(r)
	})})
	client.BaseURL = base
	_, err := resolveGithubDeliveryTarget(context.Background(), targetResolutionComment(t), &targetResolutionProvider{client: client})
	require.Error(t, err)
	require.Zero(t, sinkRequests.Load())
	require.Nil(t, client.Client().CheckRedirect)
}
