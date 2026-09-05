package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	backendutils "github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type runReconciliationTestStore struct {
	preparation *models.DurableRunReconciliationPreparation
	observation models.DurableRunObservation
}

func (s *runReconciliationTestStore) PrepareDurableRunReconciliation(context.Context, uuid.UUID, string, string, int64) (*models.DurableRunReconciliationPreparation, error) {
	return s.preparation, nil
}
func (s *runReconciliationTestStore) ReconcileDurableWorkflowRun(_ context.Context, _ uuid.UUID, _ string, observation models.DurableRunObservation, _ string, _ int64) (bool, error) {
	s.observation = observation
	return true, nil
}

func TestGithubRunUnavailablePreservesCanonicalIdentityForRecovery(t *testing.T) {
	canonical := models.DurableRunObservation{RepositoryID: 12345, WorkflowID: 42, RunID: 901, RunAttempt: 1, HeadSHA: strings.Repeat("a", 40)}
	store := &runReconciliationTestStore{preparation: &models.DurableRunReconciliationPreparation{GithubAppID: 1, InstallationID: 2, RepoOwner: "monoai-co", RepoName: "sre", RunID: 901, CanonicalObservation: canonical}}
	var requests int
	provider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/repos/monoai-co/sre/actions/runs/901", r.URL.Path)
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"message":"Not Found"}`)), Request: r}, nil
	})}}
	result, err := reconcileGithubWorkflowRun(context.Background(), OutboxDispatchRequest{}, store, provider)
	require.NoError(t, err)
	require.Equal(t, 1, requests)
	canonical.Status = "unavailable"
	require.Equal(t, canonical, store.observation)
	var stored models.DurableRunObservation
	require.NoError(t, json.Unmarshal(result.ProviderReceipt, &stored))
	require.Equal(t, canonical, stored)
	require.Empty(t, stored.Conclusion)
}

func TestGithubRunTransientFailureDoesNotRecordPermanentAbsence(t *testing.T) {
	store := &runReconciliationTestStore{preparation: &models.DurableRunReconciliationPreparation{GithubAppID: 1, InstallationID: 2, RepoOwner: "monoai-co", RepoName: "sre", RunID: 901}}
	provider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"message":"Unavailable"}`)), Request: r}, nil
	})}}
	result, err := reconcileGithubWorkflowRun(context.Background(), OutboxDispatchRequest{}, store, provider)
	require.NoError(t, err)
	require.Empty(t, result.ProviderReceipt)
	require.Positive(t, result.RetryAfter)
	require.Empty(t, store.observation.Status)
}

func TestPostgresGithubRunInvalidObservationPollsPastAttemptLimitAndRecordsRecoveryAfterGrantExpiry(t *testing.T) {
	database, organisation, delivery := newDurableExecutionIntegrationDatabase(t)
	job, dispatchEffect := createDurableExecutionIntegrationGraph(t, database, organisation, delivery)

	now := time.Now().UTC()
	dispatchLeaseID := "invalid-observation-dispatch-lease"
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", dispatchEffect.ID).Updates(map[string]any{
		"status":           models.OutboxEffectProcessing,
		"lease_id":         dispatchLeaseID,
		"lease_expires_at": now.Add(time.Minute),
		"writer_epoch":     durableExecutionIntegrationWriterEpoch,
		"updated_at":       now,
	}).Error)
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), dispatchEffect.ID, dispatchLeaseID, time.Hour, time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, preparation)
	providerRun := &github.WorkflowRun{
		ID:         github.Int64(1001),
		WorkflowID: github.Int64(durableExecutionIntegrationWorkflowID),
		Repository: &github.Repository{ID: github.Int64(durableExecutionIntegrationRepositoryID)},
		RunAttempt: github.Int(1),
		HTMLURL:    github.String("https://github.com/monoai-co/sre/actions/runs/1001"),
		HeadSHA:    github.String(durableExecutionIntegrationSecondSHA),
	}
	dispatchResult, err := durableWorkflowDispatchReceipt(*job.OperationID, false, "main", providerRun, preparation.ClaimExpiresAt)
	require.NoError(t, err)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), dispatchEffect.ID, dispatchLeaseID, dispatchResult.ProviderReceipt, now, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch))

	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	audience, err := operation.ExecutionClaimAudience(*job.OperationID, job.DiggerJobID)
	require.NoError(t, err)
	claim := models.DurableExecutionClaimRequest{
		OperationID:         *job.OperationID,
		DiggerJobID:         job.DiggerJobID,
		RepositoryFullName:  "monoai-co/sre",
		ProjectName:         job.ProjectName,
		RunID:               1001,
		RunAttempt:          1,
		RepositoryID:        durableExecutionIntegrationRepositoryID,
		OIDCIssuer:          githubOIDCIssuer,
		OIDCAudience:        audience,
		OIDCSubject:         "repo:monoai-co/sre:ref:refs/heads/main",
		WorkflowRef:         "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main",
		WorkflowSHA:         durableExecutionIntegrationSecondSHA,
		ActionRef:           "diggerhq/digger@" + strings.Repeat("c", 40),
		CLISHA256:           strings.Repeat("d", 64),
		ProtocolVersion:     operation.ProtocolVersion,
		DispatchWriterEpoch: durableExecutionIntegrationWriterEpoch,
	}
	claimReceipt, err := database.ClaimDurableJobExecution(context.Background(), claim, token.Value, map[string][]byte{"reconciliation-test-key": []byte("reconciliation-test-grant-secret-32")}, "reconciliation-test-key", durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.True(t, claimReceipt.Granted)

	var watch models.OutboxEffect
	require.NoError(t, database.GormDB.First(&watch, "operation_id = ? AND effect_kind = ?", *job.OperationID, models.GithubWorkflowReconcileEffectKind).Error)
	provider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		invalid := &github.WorkflowRun{
			ID:         github.Int64(1001),
			WorkflowID: github.Int64(durableExecutionIntegrationWorkflowID),
			Repository: &github.Repository{ID: github.Int64(durableExecutionIntegrationRepositoryID + 1)},
			RunAttempt: github.Int(1),
			HeadSHA:    github.String(durableExecutionIntegrationSecondSHA),
			Status:     github.String("in_progress"),
		}
		body, _ := json.Marshal(invalid)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	dispatch, err := NewGithubWorkflowOutboxDispatch(database, provider, time.Hour)
	require.NoError(t, err)
	config := testOutboxDispatcherConfig()
	config.DatabaseIdentity = durableExecutionIntegrationDatabaseIdentity
	config.WriterEpoch = durableExecutionIntegrationWriterEpoch
	config.MaxAttempts = 2
	config.LeaseDuration = 2 * time.Second
	// Exercise ordinary database latency while retaining the retry-limit test.
	require.NoError(t, database.GormDB.Exec(`
CREATE FUNCTION delayed_reconciliation_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status <> 'processing' AND NEW.status = 'processing' THEN
    PERFORM pg_sleep(0.15);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER delayed_reconciliation_claim BEFORE UPDATE ON outbox_effects
FOR EACH ROW EXECUTE FUNCTION delayed_reconciliation_claim();`).Error)
	dispatcher := newTestOutboxDispatcher(t, database, dispatch, config)
	dispatcher.Start()
	t.Cleanup(func() { shutdownOutboxDispatcher(t, dispatcher) })

	waitForRetry := func(expectedAttempt int64) {
		require.Eventually(t, func() bool {
			var current models.OutboxEffect
			return database.GormDB.First(&current, "id = ?", watch.ID).Error == nil && current.Status == models.OutboxEffectRetrying && current.AttemptCount >= expectedAttempt
		}, 3*time.Second, 5*time.Millisecond)
	}
	forceNextAttempt := func() {
		require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ? AND status = ?", watch.ID, models.OutboxEffectRetrying).Update("next_attempt_at", time.Now().UTC().Add(-time.Second)).Error)
		dispatcher.Wake()
	}
	waitForRetry(1)
	forceNextAttempt()
	waitForRetry(2)
	forceNextAttempt()
	waitForRetry(3)

	var storedClaim models.ExecutionClaimAttempt
	require.NoError(t, database.GormDB.First(&storedClaim, "operation_id = ? AND state = ?", *job.OperationID, models.ExecutionClaimGranted).Error)
	expiredAt := time.Now().UTC().Add(-time.Minute)
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("id = ?", storedClaim.ID).Updates(map[string]any{
		"created_at":       expiredAt.Add(-2 * time.Hour),
		"granted_at":       expiredAt.Add(-time.Hour),
		"grant_expires_at": expiredAt,
	}).Error)
	require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("id = ?", token.ID).Updates(map[string]any{
		"activated_at": expiredAt.Add(-2 * time.Hour),
		"expiry":       expiredAt,
	}).Error)
	forceNextAttempt()
	waitForRetry(4)

	require.Eventually(t, func() bool {
		var recovery models.ApplyRecovery
		return database.GormDB.First(&recovery, "operation_id = ? AND organisation_id = ?", *job.OperationID, organisation.ID).Error == nil
	}, 3*time.Second, 5*time.Millisecond)
	var recovery models.ApplyRecovery
	require.NoError(t, database.GormDB.First(&recovery, "operation_id = ?", *job.OperationID).Error)
	require.Equal(t, "unknown", recovery.Outcome)
	require.Empty(t, recovery.TerminalObservation)
	var observation models.DurableRunObservation
	require.NoError(t, json.Unmarshal(recovery.Observation, &observation))
	require.Equal(t, "unobserved", observation.Status)
	var storedWatch models.OutboxEffect
	require.NoError(t, database.GormDB.First(&storedWatch, "id = ?", watch.ID).Error)
	require.Equal(t, models.OutboxEffectRetrying, storedWatch.Status)
	require.GreaterOrEqual(t, storedWatch.AttemptCount, int64(4))
}
