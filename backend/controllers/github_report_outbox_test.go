package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type reportTestStore struct {
	mu       sync.Mutex
	payload  models.GithubReportCreatePayload
	consumed bool
	calls    int
}

func (s *reportTestStore) PrepareGithubReportCreate(_ context.Context, id uuid.UUID, _, _ string, _ int64) (*models.GithubReportCreatePreparation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	canonical, err := models.CanonicalGithubReportCreatePayload(s.payload)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(canonical)
	correlation, err := models.GithubReportCreateCorrelation(id, hex.EncodeToString(digest[:]))
	if err != nil {
		return nil, err
	}
	fresh := !s.consumed
	s.consumed = true
	return &models.GithubReportCreatePreparation{Payload: s.payload, Correlation: correlation, MayCreate: fresh, AttemptedAt: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)}, nil
}

func (*reportTestStore) PrepareDurableJobDispatch(context.Context, uuid.UUID, string, time.Duration, time.Duration, string, int64) (*models.DurableJobDispatchPreparation, error) {
	return nil, errors.New("workflow path must not be used")
}

type reportTestProvider struct {
	utils.DiggerGithubClientMockProvider
	client *github.Client
	err    error
}

func (p reportTestProvider) GetContext(context.Context, int64, int64) (*github.Client, *string, error) {
	return p.client, nil, p.err
}

func reportTestPayload() models.GithubReportCreatePayload {
	return models.GithubReportCreatePayload{OrganisationID: 1, GithubAppID: 2, GithubInstallationID: 3,
		RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 4, ResourceKind: models.GithubReportResourceComment, Body: "report"}
}

func reportTestRequest(t *testing.T, payload models.GithubReportCreatePayload) OutboxDispatchRequest {
	t.Helper()
	raw, err := models.CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	return OutboxDispatchRequest{EffectID: uuid.New(), EffectKind: models.GithubReportCreateEffectKind, Payload: raw, LeaseID: "lease", DatabaseIdentity: "db", WriterEpoch: 1}
}

func reportTestClient(t *testing.T, handler http.HandlerFunc) (*github.Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := github.NewClient(server.Client())
	base, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = base
	return client, server
}

func TestGithubReportDroppedResponseReconcilesWithoutSecondPost(t *testing.T) {
	payload := reportTestPayload()
	request := reportTestRequest(t, payload)
	store := &reportTestStore{payload: payload}
	var mu sync.Mutex
	posts, lists := 0, 0
	body := ""
	client, server := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost:
			posts++
			var comment github.IssueComment
			if json.NewDecoder(r.Body).Decode(&comment) != nil {
				http.Error(w, "bad body", 400)
				return
			}
			body = comment.GetBody()
			json.NewEncoder(w).Encode(map[string]any{"id": 123, "body": body})
		case r.URL.Path == "/apps/digger":
			json.NewEncoder(w).Encode(map[string]any{"id": 2, "slug": "digger"})
		default:
			lists++
			json.NewEncoder(w).Encode([]any{map[string]any{"id": 123, "body": body, "user": map[string]any{"type": "Bot", "login": "digger[bot]"}}})
		}
	})
	transport := server.Client().Transport
	client = github.NewClient(&http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && r.Method == http.MethodPost {
			response.Body.Close()
			return nil, errors.New("response lost after provider persisted comment")
		}
		return response, err
	})})
	client.BaseURL, _ = url.Parse(server.URL + "/")
	dispatch, err := NewGithubWorkflowOutboxDispatch(store, reportTestProvider{client: client}, time.Hour)
	require.NoError(t, err)
	first, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, time.Minute, first.RetryAfter)
	second, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.Zero(t, second.RetryAfter)
	var receipt models.GithubReportCreateReceipt
	require.NoError(t, json.Unmarshal(second.ProviderReceipt, &receipt))
	require.Equal(t, int64(123), receipt.ProviderID)
	require.Equal(t, "https://github.com/owner/repo/pull/4#issuecomment-123", receipt.ProviderURL)
	require.Equal(t, 1, posts)
	require.Equal(t, 1, lists)
}

func TestGithubReportRecoveryWithoutResourceNeverPosts(t *testing.T) {
	payload := reportTestPayload()
	request := reportTestRequest(t, payload)
	store := &reportTestStore{payload: payload, consumed: true}
	posts := 0
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	})
	for i := 0; i < 3; i++ {
		result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
		require.NoError(t, err)
		require.Equal(t, time.Minute, result.RetryAfter)
	}
	require.Zero(t, posts)
}

func TestGithubReportRecoveryPaginatesAndRejectsConflictingMarkers(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "adopt", true: "conflict"}[conflict], func(t *testing.T) {
			payload := reportTestPayload()
			request := reportTestRequest(t, payload)
			store := &reportTestStore{payload: payload, consumed: true}
			prep, err := store.PrepareGithubReportCreate(context.Background(), request.EffectID, "", "", 1)
			require.NoError(t, err)
			body := githubReportCommentBody(payload.Body, prep.Correlation)
			posts, pages := 0, 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					posts++
				}
				if r.URL.Path == "/apps/digger" {
					w.Write([]byte(`{"id":2,"slug":"digger"}`))
					return
				}
				pages++
				if r.URL.Query().Get("page") == "1" {
					w.Header().Set("Link", `<http://example.test?page=2>; rel="next"`)
					w.Write([]byte(`[]`))
					return
				}
				resource := map[string]any{"id": 123, "body": body, "user": map[string]any{"type": "Bot", "login": "digger[bot]"}}
				if conflict {
					resource["body"] = "modified " + body
				}
				json.NewEncoder(w).Encode([]any{resource})
			})
			result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
			require.NoError(t, err)
			require.Equal(t, 2, pages)
			require.Zero(t, posts)
			if conflict {
				require.Equal(t, time.Minute, result.RetryAfter)
			} else {
				require.NotEmpty(t, result.ProviderReceipt)
			}
		})
	}
}

func TestGithubReportConcurrentPreparationPostsAtMostOnce(t *testing.T) {
	payload := reportTestPayload()
	request := reportTestRequest(t, payload)
	store := &reportTestStore{payload: payload}
	var mu sync.Mutex
	posts := 0
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			posts++
			http.Error(w, "unknown", 500)
			return
		}
		w.Write([]byte(`[]`))
	})
	var group sync.WaitGroup
	errorsFound := make(chan error, 8)
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
			errorsFound <- err
		}()
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		require.NoError(t, err)
	}
	require.Equal(t, 1, posts)
}

func TestGithubReportAuthenticationFailureDoesNotConsumePermit(t *testing.T) {
	payload := reportTestPayload()
	store := &reportTestStore{payload: payload}
	_, err := dispatchGithubReportCreate(context.Background(), reportTestRequest(t, payload), store, reportTestProvider{err: errors.New("auth unavailable")})
	require.Error(t, err)
	require.Zero(t, store.calls)
	require.False(t, store.consumed)
}

func TestGithubReportCheckCreationUsesStablePermitTime(t *testing.T) {
	payload := reportTestPayload()
	payload.ResourceKind, payload.Body, payload.HeadSHA = models.GithubReportResourceCheckRun, "", "sha"
	payload.Check = &models.GithubReportCheck{Name: "plan", Status: "completed", Conclusion: "success", Title: "Plan", Text: "text"}
	store := &reportTestStore{payload: payload}
	request := reportTestRequest(t, payload)
	var captured github.CreateCheckRunOptions
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if json.NewDecoder(r.Body).Decode(&captured) != nil {
			http.Error(w, "bad body", 400)
			return
		}
		json.NewEncoder(w).Encode(&github.CheckRun{ID: github.Int64(123), Name: github.String(captured.Name), HeadSHA: github.String(captured.HeadSHA), ExternalID: captured.ExternalID,
			Status: captured.Status, Conclusion: captured.Conclusion, Output: captured.Output, App: &github.App{ID: github.Int64(2)}})
	})
	result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
	require.NoError(t, err)
	require.NotEmpty(t, result.ProviderReceipt)
	require.Equal(t, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), captured.StartedAt.Time)
	require.Equal(t, captured.StartedAt, captured.CompletedAt)
	require.True(t, strings.HasPrefix(captured.GetExternalID(), "digger-report:"))
}

func TestGithubReportCopiedCommentMarkerRequiresExactAppBot(t *testing.T) {
	for _, scenario := range []string{"human", "other bot", "wrong app", "lookup unavailable", "missing user", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			payload := reportTestPayload()
			request := reportTestRequest(t, payload)
			store := &reportTestStore{payload: payload, consumed: true}
			prep, err := store.PrepareGithubReportCreate(context.Background(), request.EffectID, "", "", 1)
			require.NoError(t, err)
			posts := 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					posts++
				}
				if strings.HasPrefix(r.URL.Path, "/apps/") {
					if scenario == "lookup unavailable" {
						http.Error(w, "unavailable", 503)
						return
					}
					id, slug := int64(2), "digger"
					if scenario == "wrong app" {
						id = 99
					}
					if scenario == "other bot" {
						id, slug = 99, "other"
					}
					json.NewEncoder(w).Encode(map[string]any{"id": id, "slug": slug})
					return
				}
				user := &github.User{Type: github.String("Bot"), Login: github.String("digger[bot]")}
				if scenario == "human" {
					user.Type = github.String("User")
				}
				if scenario == "other bot" {
					user.Login = github.String("other[bot]")
				}
				if scenario == "missing user" {
					user = nil
				}
				comments := []*github.IssueComment{{ID: github.Int64(123), Body: github.String(githubReportCommentBody(payload.Body, prep.Correlation)), User: user}}
				if scenario == "duplicate" {
					comments = append(comments, &github.IssueComment{ID: github.Int64(456), Body: comments[0].Body, User: user})
				}
				json.NewEncoder(w).Encode(comments)
			})
			result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
			require.NoError(t, err)
			require.Equal(t, time.Minute, result.RetryAfter)
			require.Empty(t, result.ProviderReceipt)
			require.Zero(t, posts)
		})
	}
}

func TestGithubReportCheckRecoveryRequiresExactUniqueResource(t *testing.T) {
	for _, scenario := range []string{"adopt", "wrong app", "wrong head", "wrong name", "wrong status", "wrong output", "duplicate", "zero"} {
		t.Run(scenario, func(t *testing.T) {
			payload := reportTestPayload()
			payload.ResourceKind, payload.Body, payload.HeadSHA = models.GithubReportResourceCheckRun, "", "sha"
			payload.Check = &models.GithubReportCheck{Name: "plan", Status: "queued", Title: "Plan"}
			store := &reportTestStore{payload: payload, consumed: true}
			request := reportTestRequest(t, payload)
			prep, err := store.PrepareGithubReportCreate(context.Background(), request.EffectID, "", "", 1)
			require.NoError(t, err)
			posts, pages := 0, 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					posts++
				}
				if r.URL.Query().Get("filter") != "all" || r.URL.Path != "/repos/owner/repo/commits/sha/check-runs" {
					http.Error(w, "bad listing scope", 400)
					return
				}
				pages++
				if r.URL.Query().Get("page") == "1" {
					w.Header().Set("Link", `<http://example.test?page=2>; rel="next"`)
					w.Write([]byte(`{"check_runs":[]}`))
					return
				}
				run := &github.CheckRun{ID: github.Int64(123), Name: github.String("plan"), HeadSHA: github.String("sha"),
					ExternalID: github.String(prep.Correlation), Status: github.String("queued"), App: &github.App{ID: github.Int64(2)},
					Output: &github.CheckRunOutput{Title: github.String("Plan"), Summary: github.String("")}}
				switch scenario {
				case "wrong app":
					run.App.ID = github.Int64(99)
				case "wrong head":
					run.HeadSHA = github.String("other")
				case "wrong name":
					run.Name = github.String("other")
				case "wrong status":
					run.Status = github.String("completed")
				case "wrong output":
					run.Output.Title = github.String("other")
				}
				runs := []*github.CheckRun{run}
				if scenario == "duplicate" {
					duplicate := *run
					duplicate.ID = github.Int64(456)
					runs = append(runs, &duplicate)
				}
				if scenario == "zero" {
					runs = nil
				}
				json.NewEncoder(w).Encode(map[string]any{"check_runs": runs})
			})
			result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
			require.NoError(t, err)
			require.Equal(t, 2, pages)
			require.Zero(t, posts)
			if scenario == "adopt" {
				require.NotEmpty(t, result.ProviderReceipt)
			} else {
				require.Equal(t, time.Minute, result.RetryAfter)
				require.Empty(t, result.ProviderReceipt)
			}
		})
	}
}

func TestGithubReportCreateDoesNotFollowPostRedirects(t *testing.T) {
	for _, code := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			payload := reportTestPayload()
			request := reportTestRequest(t, payload)
			store := &reportTestStore{payload: payload}
			posts := 0
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
				}
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(code)
			})
			result, err := dispatchGithubReportCreate(context.Background(), request, store, reportTestProvider{client: client})
			require.NoError(t, err)
			require.Equal(t, time.Minute, result.RetryAfter)
			require.Equal(t, 1, posts)
			require.Nil(t, client.Client().CheckRedirect, "provider client must remain unchanged")
		})
	}
}
