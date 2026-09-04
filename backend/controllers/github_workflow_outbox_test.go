package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/models"
	backendutils "github.com/diggerhq/digger/backend/utils"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type githubWorkflowDispatchTestStore struct {
	preparation   *models.DurableJobDispatchPreparation
	err           error
	called        bool
	effectID      uuid.UUID
	leaseID       string
	identity      string
	epoch         int64
	validity      time.Duration
	leaseDuration time.Duration
}

type githubWorkflowDispatchRoundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip githubWorkflowDispatchRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type blockingGithubWorkflowDispatchProvider struct {
	started chan struct{}
}

func (provider blockingGithubWorkflowDispatchProvider) NewClient(client *http.Client) (*github.Client, error) {
	return github.NewClient(client), nil
}

func (provider blockingGithubWorkflowDispatchProvider) Get(int64, int64) (*github.Client, *string, error) {
	return nil, nil, errors.New("non-context GitHub client path must not be used")
}

func (provider blockingGithubWorkflowDispatchProvider) GetContext(ctx context.Context, _ int64, _ int64) (*github.Client, *string, error) {
	close(provider.started)
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (provider blockingGithubWorkflowDispatchProvider) FetchCredentials(string) (string, string, string, string, error) {
	return "", "", "", "", nil
}

func githubWorkflowDispatchTestProvider(t *testing.T, dispatches *int, capturedSpec *spec.Spec, capturedRunName *string) backendutils.DiggerGithubClientMockProvider {
	t.Helper()
	return backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":12345,"full_name":"monoai-co/sre","default_branch":"main"}`)), Header: make(http.Header)}, nil
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/digger_workflow.yml":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":42,"path":".github/workflows/digger_workflow.yml","state":"active"}`)), Header: make(http.Header)}, nil
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre/actions/runs/901":
			body, err := json.Marshal(github.WorkflowRun{
				ID:           github.Int64(901),
				WorkflowID:   github.Int64(42),
				Repository:   &github.Repository{ID: github.Int64(12345)},
				RunAttempt:   github.Int(1),
				DisplayTitle: github.String("A custom workflow title"),
				Event:        github.String("workflow_dispatch"),
				HeadBranch:   github.String("main"),
				HeadSHA:      github.String("0123456789012345678901234567890123456789"),
				HTMLURL:      github.String("https://github.com/monoai-co/sre/actions/runs/901"),
			})
			require.NoError(t, err)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
		case request.Method == http.MethodPost && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/42/dispatches":
			(*dispatches)++
			var body struct {
				Inputs map[string]any `json:"inputs"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			runName, ok := body.Inputs["run_name"].(string)
			require.True(t, ok)
			*capturedRunName = runName
			rawSpec, ok := body.Inputs["spec"].(string)
			require.True(t, ok)
			require.NoError(t, json.Unmarshal([]byte(rawSpec), capturedSpec))
			require.Equal(t, "2026-03-10", request.Header.Get("X-GitHub-Api-Version"))
			details := `{"workflow_run_id":901,"run_url":"https://api.github.com/repos/monoai-co/sre/actions/runs/901","html_url":"https://github.com/monoai-co/sre/actions/runs/901"}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(details)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected GitHub request %s %s", request.Method, request.URL.Path)
			return nil, nil
		}
	})}}
}

func (store *githubWorkflowDispatchTestStore) PrepareDurableJobDispatch(_ context.Context, effectID uuid.UUID, leaseID string, validity time.Duration, leaseDuration time.Duration, identity string, epoch int64) (*models.DurableJobDispatchPreparation, error) {
	store.called = true
	store.effectID = effectID
	store.leaseID = leaseID
	store.identity = identity
	store.epoch = epoch
	store.validity = validity
	store.leaseDuration = leaseDuration
	return store.preparation, store.err
}

func githubWorkflowDispatchTestPreparation(t *testing.T) *models.DurableJobDispatchPreparation {
	t.Helper()
	batchID := uuid.New()
	operationID, err := operation.Derive("digger-job", "test")
	require.NoError(t, err)
	operationValue := operationID.String()
	writerEpoch := int64(7)
	pullRequestNumber := 42
	jobJSON := scheduler.JobToJson(
		scheduler.Job{ProjectName: "root", PullRequestNumber: &pullRequestNumber},
		scheduler.DiggerCommandPlan,
		"test-organisation",
		"feature/test",
		"deadbeef",
		"cli:test-token",
		"https://opentaco.example",
		configuration.Project{Name: "root", WorkflowFile: "digger_workflow.yml"},
	)
	serialized, err := json.Marshal(jobJSON)
	require.NoError(t, err)
	batchIDString := batchID.String()
	return &models.DurableJobDispatchPreparation{
		GithubAppID: 456,
		Job: &models.DiggerJob{
			DiggerJobID:       "job-public-id",
			OperationID:       &operationValue,
			ProtocolVersion:   operation.ProtocolVersion,
			WriterEpoch:       &writerEpoch,
			ProjectName:       "root",
			BatchID:           &batchIDString,
			SerializedJobSpec: serialized,
			WorkflowFile:      "digger_workflow.yml",
			ReporterType:      "lazy",
			Batch: &models.DiggerBatch{
				ID:                   batchID,
				VCS:                  models.DiggerVCSGithub,
				BatchType:            scheduler.DiggerCommandPlan,
				GithubInstallationId: 123,
				RepoOwner:            "monoai-co",
				RepoName:             "sre",
				RepoFullName:         "monoai-co/sre",
			},
		},
	}
}

func TestGithubWorkflowOutboxDispatchPreparesTokenBeforeProvider(t *testing.T) {
	preparation := githubWorkflowDispatchTestPreparation(t)
	store := &githubWorkflowDispatchTestStore{preparation: preparation}
	dispatches := 0
	var capturedSpec spec.Spec
	var capturedRunName string
	provider := githubWorkflowDispatchTestProvider(t, &dispatches, &capturedSpec, &capturedRunName)
	dispatch, err := NewGithubWorkflowOutboxDispatch(store, provider, DefaultDurableJobTokenValidity)
	require.NoError(t, err)
	effectID := uuid.New()
	request := OutboxDispatchRequest{
		EffectID:         effectID,
		OperationID:      *preparation.Job.OperationID,
		EffectKind:       models.GithubWorkflowDispatchEffectKind,
		LeaseID:          "lease",
		DatabaseIdentity: "database",
		WriterEpoch:      8,
		LeaseDuration:    time.Minute,
	}

	result, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, result.ProviderReceipt)
	require.True(t, store.called)
	require.Equal(t, effectID, store.effectID)
	require.Equal(t, request.LeaseID, store.leaseID)
	require.Equal(t, request.DatabaseIdentity, store.identity)
	require.Equal(t, request.WriterEpoch, store.epoch)
	require.Equal(t, DefaultDurableJobTokenValidity, store.validity)
	require.Equal(t, time.Minute, store.leaseDuration)
	require.Equal(t, 1, dispatches)
	require.Equal(t, request.OperationID, capturedSpec.OperationID)
	require.Equal(t, operation.ProtocolVersion, capturedSpec.ProtocolVersion)
	require.Equal(t, int64(7), capturedSpec.WriterEpoch)
	require.Equal(t, "monoai-co/sre", capturedSpec.VCS.RepoFullname)
	require.Contains(t, capturedRunName, "[digger-operation:"+request.OperationID+"]")
	var receipt struct {
		RunID      int64  `json:"run_id"`
		RunAttempt int    `json:"run_attempt"`
		ControlRef string `json:"control_ref"`
	}
	require.NoError(t, json.Unmarshal(result.ProviderReceipt, &receipt))
	require.Equal(t, int64(901), receipt.RunID)
	require.Equal(t, 1, receipt.RunAttempt)
	require.Equal(t, "main", receipt.ControlRef)
}

func TestGithubWorkflowOutboxDispatchRetriesAmbiguousAcceptAndCommitsReturnedRun(t *testing.T) {
	preparation := githubWorkflowDispatchTestPreparation(t)
	store := &githubWorkflowDispatchTestStore{preparation: preparation}
	dispatches := 0
	var runName string
	provider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":12345,"full_name":"monoai-co/sre","default_branch":"main"}`)), Header: make(http.Header)}, nil
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/digger_workflow.yml":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":42,"path":".github/workflows/digger_workflow.yml","state":"active"}`)), Header: make(http.Header)}, nil
		case request.Method == http.MethodPost && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/42/dispatches":
			dispatches++
			var body struct {
				Inputs map[string]any `json:"inputs"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			var ok bool
			runName, ok = body.Inputs["run_name"].(string)
			require.True(t, ok)
			if dispatches == 1 {
				return nil, io.ErrUnexpectedEOF
			}
			details := `{"workflow_run_id":902,"run_url":"https://api.github.com/repos/monoai-co/sre/actions/runs/902","html_url":"https://github.com/monoai-co/sre/actions/runs/902"}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(details)), Header: make(http.Header)}, nil
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre/actions/runs/902":
			run := github.WorkflowRun{ID: github.Int64(902), RunAttempt: github.Int(1), DisplayTitle: &runName, Event: github.String("workflow_dispatch"), HeadBranch: github.String("main"), HeadSHA: github.String(strings.Repeat("a", 40)), HTMLURL: github.String("https://github.com/monoai-co/sre/actions/runs/902")}
			run.WorkflowID = github.Int64(42)
			run.Repository = &github.Repository{ID: github.Int64(12345)}
			body, err := json.Marshal(run)
			require.NoError(t, err)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected GitHub request %s %s", request.Method, request.URL.Path)
			return nil, nil
		}
	})}}
	dispatch, err := NewGithubWorkflowOutboxDispatch(store, provider, DefaultDurableJobTokenValidity)
	require.NoError(t, err)
	request := OutboxDispatchRequest{EffectID: uuid.New(), OperationID: *preparation.Job.OperationID, EffectKind: models.GithubWorkflowDispatchEffectKind, LeaseID: "lease", DatabaseIdentity: "database", WriterEpoch: 7, LeaseDuration: time.Minute}

	_, err = dispatch(context.Background(), request)
	require.Error(t, err)
	require.True(t, errors.Is(err, ci_backends.ErrWorkflowDispatchAcceptanceAmbiguous))
	result, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, dispatches)
	var receipt struct {
		RunID int64 `json:"run_id"`
	}
	require.NoError(t, json.Unmarshal(result.ProviderReceipt, &receipt))
	require.Equal(t, int64(902), receipt.RunID)
}

func TestGithubWorkflowOutboxDispatchNeverCallsProviderWhenPreparationFailsOrSkips(t *testing.T) {
	testCases := map[string]struct {
		prepareErr error
		skip       bool
	}{
		"claim failure": {prepareErr: models.ErrDurableJobDispatchClaim},
		"terminal":      {skip: true},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			preparation := githubWorkflowDispatchTestPreparation(t)
			preparation.SkipProvider = testCase.skip
			store := &githubWorkflowDispatchTestStore{preparation: preparation, err: testCase.prepareErr}
			dispatches := 0
			var capturedSpec spec.Spec
			var capturedRunName string
			provider := githubWorkflowDispatchTestProvider(t, &dispatches, &capturedSpec, &capturedRunName)
			dispatch, err := NewGithubWorkflowOutboxDispatch(store, provider, DefaultDurableJobTokenValidity)
			require.NoError(t, err)
			result, err := dispatch(context.Background(), OutboxDispatchRequest{
				EffectID:         uuid.New(),
				OperationID:      *preparation.Job.OperationID,
				EffectKind:       models.GithubWorkflowDispatchEffectKind,
				LeaseID:          "lease",
				DatabaseIdentity: "database",
				WriterEpoch:      7,
				LeaseDuration:    time.Minute,
			})
			if testCase.prepareErr != nil {
				require.ErrorIs(t, err, testCase.prepareErr)
				require.Empty(t, result.ProviderReceipt)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, result.ProviderReceipt)
			}
			require.Zero(t, dispatches)
		})
	}
}

func TestGithubWorkflowOutboxDispatchRejectsMisconfiguration(t *testing.T) {
	provider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{}}
	_, err := NewGithubWorkflowOutboxDispatch(nil, provider, time.Hour)
	require.ErrorIs(t, err, ErrGithubWorkflowOutboxDispatch)
	_, err = NewGithubWorkflowOutboxDispatch(&githubWorkflowDispatchTestStore{}, provider, 0)
	require.ErrorIs(t, err, ErrGithubWorkflowOutboxDispatch)
	require.False(t, errors.Is(err, models.ErrDurableJobDispatchClaim))
}

func TestGithubWorkflowOutboxDispatchCancelsContextDuringClientAcquisition(t *testing.T) {
	preparation := githubWorkflowDispatchTestPreparation(t)
	store := &githubWorkflowDispatchTestStore{preparation: preparation}
	provider := blockingGithubWorkflowDispatchProvider{started: make(chan struct{})}
	dispatch, err := NewGithubWorkflowOutboxDispatch(store, provider, DefaultDurableJobTokenValidity)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, dispatchErr := dispatch(ctx, OutboxDispatchRequest{
			EffectID:         uuid.New(),
			OperationID:      *preparation.Job.OperationID,
			EffectKind:       models.GithubWorkflowDispatchEffectKind,
			LeaseID:          "lease",
			DatabaseIdentity: "database",
			WriterEpoch:      7,
			LeaseDuration:    time.Minute,
		})
		done <- dispatchErr
	}()
	<-provider.started
	cancel()
	select {
	case dispatchErr := <-done:
		require.ErrorIs(t, dispatchErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("context-aware GitHub client acquisition did not stop after cancellation")
	}
}

func TestDurableWorkflowRunUsesProviderIdentityInsteadOfDisplayTitle(t *testing.T) {
	target := &ci_backends.DurableWorkflowTarget{RepositoryID: 12345, WorkflowID: 42, ControlRef: "main"}
	validRun := func() *github.WorkflowRun {
		return &github.WorkflowRun{
			ID: github.Int64(901), Repository: &github.Repository{ID: github.Int64(12345)}, WorkflowID: github.Int64(42),
			RunAttempt: github.Int(1), Event: github.String("workflow_dispatch"), HeadBranch: github.String("main"), HeadSHA: github.String(strings.Repeat("a", 40)),
		}
	}
	require.True(t, sameDurableWorkflowRun(validRun(), 901, target))
	for name, change := range map[string]func(*github.WorkflowRun){
		"run id":       func(r *github.WorkflowRun) { r.ID = github.Int64(902) },
		"repo id":      func(r *github.WorkflowRun) { r.Repository.ID = github.Int64(12346) },
		"missing repo": func(r *github.WorkflowRun) { r.Repository = nil },
		"workflow id":  func(r *github.WorkflowRun) { r.WorkflowID = github.Int64(43) },
		"run attempt":  func(r *github.WorkflowRun) { r.RunAttempt = github.Int(2) },
		"event":        func(r *github.WorkflowRun) { r.Event = github.String("push") },
		"ref":          func(r *github.WorkflowRun) { r.HeadBranch = github.String("feature") },
		"sha":          func(r *github.WorkflowRun) { r.HeadSHA = github.String("main") },
	} {
		t.Run(name, func(t *testing.T) {
			run := validRun()
			change(run)
			require.False(t, sameDurableWorkflowRun(run, 901, target))
		})
	}
}
