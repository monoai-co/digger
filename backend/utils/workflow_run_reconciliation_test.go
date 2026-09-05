package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func prepareRunReconciliationTest(t *testing.T, database *models.Database, request models.DurableExecutionClaimRequest) *models.OutboxEffect {
	t.Helper()
	var dispatch models.OutboxEffect
	require.NoError(t, database.GormDB.First(&dispatch, "operation_id = ? AND effect_kind = ?", request.OperationID, models.GithubWorkflowDispatchEffectKind).Error)
	payload, err := json.Marshal(models.GithubRunReconciliationPayload{OperationID: request.OperationID, DispatchEffectID: dispatch.ID})
	require.NoError(t, err)
	watch := models.NewOutboxEffect(request.OperationID, models.GithubWorkflowReconcileEffectKind, fmt.Sprintf("run:%d:%d", request.RepositoryID, request.RunID), payload, durableGraphTestWriterEpoch, time.Now().UTC())
	storedWatch, _, err := database.EnqueueOutboxEffect(context.Background(), watch, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, storedWatch)
	watch = storedWatch
	leaseExpires := time.Now().UTC().Add(time.Minute)
	result := database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", watch.ID).Updates(map[string]any{"status": models.OutboxEffectProcessing, "lease_id": "watch-lease", "lease_expires_at": leaseExpires})
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)
	require.NoError(t, database.GormDB.First(watch, "id = ?", watch.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, watch.Status)
	require.Equal(t, "watch-lease", watch.LeaseID)
	require.Equal(t, durableGraphTestWriterEpoch, watch.WriterEpoch)
	require.NotNil(t, watch.LeaseExpiresAt)
	require.True(t, watch.LeaseExpiresAt.After(time.Now().UTC()))
	require.True(t, watch.ValidPayloadDigest(), "reloaded reconciliation payload %q does not match stored digest %s", watch.Payload, watch.PayloadSHA256)
	return watch
}

func runObservationForClaim(request models.DurableExecutionClaimRequest) models.DurableRunObservation {
	return models.DurableRunObservation{RepositoryID: request.RepositoryID, WorkflowID: 42, RunID: request.RunID, RunAttempt: int(request.RunAttempt), HeadSHA: request.WorkflowSHA, Status: "completed", Conclusion: "cancelled"}
}

func TestDurableRunFailureRevokesTokensAndFailsUnstartedBatch(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	watch := prepareRunReconciliationTest(t, database, request)
	observation := runObservationForClaim(request)
	terminal, err := database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, terminal)
	var jobs []models.DiggerJob
	require.NoError(t, database.GormDB.Where("batch_id = ?", *job.BatchID).Find(&jobs).Error)
	require.Len(t, jobs, 3)
	for _, item := range jobs {
		require.Equal(t, scheduler.DiggerJobFailed, item.Status)
		require.Equal(t, int64(3), item.StatusVersion)
	}
	var batch models.DiggerBatch
	require.NoError(t, database.GormDB.First(&batch, "id = ?", *job.BatchID).Error)
	require.Equal(t, scheduler.BatchJobFailed, batch.Status)
	var tokens []models.JobToken
	require.NoError(t, database.GormDB.Find(&tokens).Error)
	for _, item := range tokens {
		require.NotNil(t, item.RevokedAt)
	}
	prepared, err := database.PrepareDurableRunReconciliation(context.Background(), watch.ID, watch.LeaseID, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, prepared.Terminal)
	require.NotEmpty(t, prepared.PreviousReceipt)
	terminal, err = database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, terminal)
	_, err = database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.Error(t, err)
}

func TestDurableRunCancellationWaitsForActiveCallback(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	_, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	watch := prepareRunReconciliationTest(t, database, request)
	terminal, err := database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, runObservationForClaim(request), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, terminal)
	require.NoError(t, database.GormDB.First(job, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, job.Status)
	require.Equal(t, int64(2), job.StatusVersion)
	var tokens []models.JobToken
	require.NoError(t, database.GormDB.Find(&tokens).Error)
	for _, item := range tokens {
		require.Nil(t, item.RevokedAt)
	}
	prepared, err := database.PrepareDurableRunReconciliation(context.Background(), watch.ID, watch.LeaseID, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, prepared.Terminal)
	require.Empty(t, prepared.PreviousReceipt)
}

func TestDurableRunReconciliationQuarantinesSuccessOrWrongIdentity(t *testing.T) {
	for _, change := range []string{"success", "run", "repository", "workflow", "attempt", "sha", "status"} {
		t.Run(change, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			job, request, _ := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
			watch := prepareRunReconciliationTest(t, database, request)
			observation := runObservationForClaim(request)
			switch change {
			case "success":
				observation.Conclusion = "success"
			case "run":
				observation.RunID++
			case "repository":
				observation.RepositoryID++
			case "workflow":
				observation.WorkflowID++
			case "attempt":
				observation.RunAttempt++
			case "sha":
				observation.HeadSHA = strings.Repeat("f", 40)
			case "status":
				observation.Status = "unknown"
			}
			_, err := database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrDurableRunQuarantined)
			require.NoError(t, database.GormDB.First(job, "id = ?", job.ID).Error)
			require.Equal(t, scheduler.DiggerJobTriggered, job.Status)
		})
	}
}

func TestDurableRunReconciliationRequiresCurrentLeaseAndWriter(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	_, request, _ := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	watch := prepareRunReconciliationTest(t, database, request)
	observation := runObservationForClaim(request)
	_, err := database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, "stale-lease", observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
	_, err = database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch+1)
	require.ErrorIs(t, err, models.ErrControlPlaneFenced)
	observation.Status = "in_progress"
	observation.Conclusion = ""
	terminal, err := database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, terminal)
}

func TestPostgresDurableRunCancellationRacesTerminalCallback(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequestForProjects(t, organisation, delivery, []configuration.Project{{Name: "root", WorkflowFile: "digger_workflow.yml"}})
	fixture := prepareDurableCallbackFixtureForRequest(t, database, request, "root", 1001)
	watch := prepareRunReconciliationTest(t, database, fixture.claim)
	preparation, err := database.PrepareDurableRunReconciliation(context.Background(), watch.ID, watch.LeaseID, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, preparation)
	require.False(t, preparation.Terminal)
	require.Equal(t, fixture.claim.RunID, preparation.RunID)
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	var callbackReceipt *models.DurableJobStatusCallbackReceipt
	var reconcileErr, callbackErr error
	go func() {
		defer workers.Done()
		<-start
		_, reconcileErr = database.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, runObservationForClaim(fixture.claim), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	}()
	go func() {
		defer workers.Done()
		<-start
		callbackReceipt, callbackErr = database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	}()
	close(start)
	workers.Wait()
	require.NoError(t, reconcileErr)
	require.NoError(t, callbackErr)
	require.NotNil(t, callbackReceipt)
	require.False(t, callbackReceipt.AlreadyApplied)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "id = ?", fixture.job.ID).Error)
	require.Equal(t, scheduler.DiggerJobSucceeded, job.Status)
	require.Equal(t, int64(3), job.StatusVersion)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.NotNil(t, token.RevokedAt)
	var storedWatch models.OutboxEffect
	require.NoError(t, database.GormDB.First(&storedWatch, "id = ?", watch.ID).Error)
	require.Empty(t, storedWatch.ProviderReceipt)

	replayed, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	require.True(t, replayed.AlreadyApplied)
	require.Equal(t, callbackReceipt.ResponseStatus, replayed.ResponseStatus)
	require.JSONEq(t, string(callbackReceipt.ResponseBody), string(replayed.ResponseBody))
}
