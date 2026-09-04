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

func githubWorkflowDispatchTestProvider(t *testing.T, dispatches *int, capturedSpec *spec.Spec) backendutils.DiggerGithubClientMockProvider {
	t.Helper()
	return backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"default_branch":"main"}`)), Header: make(http.Header)}, nil
		case request.Method == http.MethodPost && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/digger_workflow.yml/dispatches":
			(*dispatches)++
			var body struct {
				Inputs map[string]any `json:"inputs"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			rawSpec, ok := body.Inputs["spec"].(string)
			require.True(t, ok)
			require.NoError(t, json.Unmarshal([]byte(rawSpec), capturedSpec))
			return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
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
	provider := githubWorkflowDispatchTestProvider(t, &dispatches, &capturedSpec)
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
	require.Equal(t, int64(8), capturedSpec.WriterEpoch)
	require.Equal(t, "monoai-co/sre", capturedSpec.VCS.RepoFullname)
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
			provider := githubWorkflowDispatchTestProvider(t, &dispatches, &capturedSpec)
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
