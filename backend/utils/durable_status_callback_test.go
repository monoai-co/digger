package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type durableCallbackFixture struct {
	job      *models.DiggerJob
	claim    models.DurableExecutionClaimRequest
	token    string
	grant    string
	callback models.DurableJobStatusCallbackRequest
}

func prepareDurableCallbackFixture(t *testing.T, database *models.Database, organisation *models.Organisation, delivery *models.GithubWebhookDelivery, projectName string, runID int64) durableCallbackFixture {
	return prepareDurableCallbackFixtureForRequest(t, database, durableGraphTestRequest(t, organisation, delivery), projectName, runID)
}

func prepareDurableCallbackFixtureForRequest(t *testing.T, database *models.Database, request DurableJobGraphRequest, projectName string, runID int64) durableCallbackFixture {
	t.Helper()
	job, claim, token := prepareDurableExecutionClaimForRequestTest(t, database, request, projectName, runID)
	secret := []byte("grant-secret-grant-secret-grant-secret-")
	receipt, err := database.ClaimDurableJobExecution(context.Background(), claim, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, receipt.Granted)
	return durableCallbackFixture{
		job:   job,
		claim: claim,
		token: token,
		grant: receipt.ExecutionGrant,
		callback: models.DurableJobStatusCallbackRequest{
			CallbackID:            uuid.New(),
			RepositoryFullName:    claim.RepositoryFullName,
			ProjectName:           claim.ProjectName,
			OperationID:           claim.OperationID,
			ProtocolVersion:       claim.ProtocolVersion,
			DispatchWriterEpoch:   claim.DispatchWriterEpoch,
			TargetStatus:          "succeeded",
			ExpectedStatusVersion: 2,
			ClientTimestamp:       time.Now().UTC(),
			PRCommentURL:          "https://github.example/comment/42",
			PRCommentID:           "42",
			TerraformOutput:       "output",
			WorkflowURL:           "https://github.example/actions/runs/1001",
		},
	}
}

func TestDurableJobStatusCallbackAcceptsCanonicalGraphOrderAcrossUnlockedFrontiers(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	projects := []configuration.Project{
		{Name: "a", WorkflowFile: "digger_workflow.yml"},
		{Name: "b", WorkflowFile: "digger_workflow.yml"},
		{Name: "aa", WorkflowFile: "digger_workflow.yml", DependencyProjects: []string{"a"}},
	}
	request := durableGraphTestRequestForProjects(t, organisation, delivery, projects)
	fixture := prepareDurableCallbackFixtureForRequest(t, database, request, "a", 1001)
	receipt := applyDurableCallback(t, database, fixture, durableGraphTestWriterEpoch)
	require.False(t, receipt.AlreadyApplied)
}

func applyDurableCallback(t *testing.T, database *models.Database, fixture durableCallbackFixture, writerEpoch int64) *models.DurableJobStatusCallbackReceipt {
	t.Helper()
	receipt, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, writerEpoch)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	return receipt
}

func TestDurableJobStatusCallbackIsAtomicExactAndReplayable(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)

	started := fixture
	started.callback.CallbackID = uuid.New()
	started.callback.TargetStatus = "started"
	startedReceipt := applyDurableCallback(t, database, started, durableGraphTestWriterEpoch)
	require.False(t, startedReceipt.AlreadyApplied)

	first := applyDurableCallback(t, database, fixture, durableGraphTestWriterEpoch)
	require.False(t, first.AlreadyApplied)
	require.NotEmpty(t, first.ResponseBody)
	assertDurableCallbackReceiptsContainNoJobSecrets(t, database, first.ResponseBody)
	var publicBatch scheduler.SerializedBatch
	require.NoError(t, json.Unmarshal(first.ResponseBody, &publicBatch))
	require.NotEmpty(t, publicBatch.Jobs)
	for index := range publicBatch.Jobs {
		var publicJobSpec scheduler.JobJson
		require.NoError(t, json.Unmarshal(publicBatch.Jobs[index].JobString, &publicJobSpec))
		require.Equal(t, []string{"digger plan"}, publicJobSpec.Commands)
		require.True(t, publicJobSpec.IsPlan())
	}

	var storedJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&storedJob, "id = ?", fixture.job.ID).Error)
	require.Equal(t, scheduler.DiggerJobSucceeded, storedJob.Status)
	require.Equal(t, int64(3), storedJob.StatusVersion)
	require.Equal(t, fixture.callback.PRCommentURL, storedJob.PRCommentUrl)
	require.NotNil(t, storedJob.PRCommentId)
	require.Equal(t, int64(42), *storedJob.PRCommentId)
	require.NotNil(t, storedJob.WorkflowRunUrl)
	require.Equal(t, fixture.callback.WorkflowURL, *storedJob.WorkflowRunUrl)

	var storedToken models.JobToken
	require.NoError(t, database.GormDB.First(&storedToken, "digger_job_database_id = ?", fixture.job.ID).Error)
	require.NotNil(t, storedToken.RevokedAt)
	require.False(t, storedToken.Expiry.After(*storedToken.RevokedAt))

	replay := applyDurableCallback(t, database, fixture, durableGraphTestWriterEpoch)
	require.True(t, replay.AlreadyApplied)
	require.True(t, bytes.Equal(first.ResponseBody, replay.ResponseBody))

	changed := fixture
	changed.callback.TerraformOutput = "changed"
	_, err := database.ApplyDurableJobStatusCallback(context.Background(), changed.callback, changed.job.DiggerJobID, changed.token, changed.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobStatusCallbackConflict)

	newCallback := fixture
	newCallback.callback.CallbackID = uuid.New()
	_, err = database.ApplyDurableJobStatusCallback(context.Background(), newCallback.callback, newCallback.job.DiggerJobID, newCallback.token, newCallback.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobStatusCallbackConflict)

	for _, credentials := range []struct{ token, grant string }{{"cli:wrong", fixture.grant}, {fixture.token, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}} {
		_, err = database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, credentials.token, credentials.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		require.ErrorIs(t, err, models.ErrDurableJobStatusCallbackConflict)
	}

	var callbackCount int64
	require.NoError(t, database.GormDB.Model(&models.JobStatusCallback{}).Count(&callbackCount).Error)
	require.Equal(t, int64(2), callbackCount)

	const targetEpoch = durableGraphTestWriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", targetEpoch).Error)
	handoffReplay := applyDurableCallback(t, database, fixture, targetEpoch)
	require.True(t, handoffReplay.AlreadyApplied)
	require.True(t, bytes.Equal(first.ResponseBody, handoffReplay.ResponseBody))
}

func assertDurableCallbackReceiptsContainNoJobSecrets(t *testing.T, database *models.Database, returnedBody []byte) {
	t.Helper()
	var tokens []models.JobToken
	require.NoError(t, database.GormDB.Find(&tokens).Error)
	var jobs []models.DiggerJob
	require.NoError(t, database.GormDB.Find(&jobs).Error)
	var callbacks []models.JobStatusCallback
	require.NoError(t, database.GormDB.Find(&callbacks).Error)

	responseBodies := [][]byte{returnedBody}
	for index := range callbacks {
		responseBodies = append(responseBodies, callbacks[index].ResponseBody)
	}
	for _, responseBody := range responseBodies {
		for index := range tokens {
			require.NotContains(t, string(responseBody), tokens[index].Value)
		}
		for index := range jobs {
			var jobSpec scheduler.JobJson
			require.NoError(t, json.Unmarshal(jobs[index].SerializedJobSpec, &jobSpec))
			for _, environment := range []map[string]string{jobSpec.RunEnvVars, jobSpec.StateEnvVars, jobSpec.CommandEnvVars} {
				for _, secret := range environment {
					require.NotContains(t, string(responseBody), secret)
				}
			}
		}
	}
}

func TestDurableJobStatusCallbackRejectsCorruptedImmutableGraph(t *testing.T) {
	tests := map[string]func(*testing.T, *models.Database, durableCallbackFixture){
		"sibling job spec": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			var sibling models.DiggerJob
			require.NoError(t, database.GormDB.First(&sibling, "project_name = ?", "root-two").Error)
			var siblingSpec scheduler.JobJson
			require.NoError(t, json.Unmarshal(sibling.SerializedJobSpec, &siblingSpec))
			siblingSpec.Branch = "tampered"
			serializedSpec, err := json.Marshal(siblingSpec)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", sibling.ID).Update("serialized_job_spec", serializedSpec).Error)
		},
		"sibling dependency": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("project_name = ?", "child").Update("dependency_operation_ids", []byte(`[]`)).Error)
		},
		"sibling operation epoch": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			var sibling models.DiggerJob
			require.NoError(t, database.GormDB.First(&sibling, "project_name = ?", "root-two").Error)
			require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *sibling.OperationID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
		},
		"batch epoch": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *fixture.job.BatchID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
		},
		"batch identity": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			var batch models.DiggerBatch
			require.NoError(t, database.GormDB.First(&batch, "id = ?", *fixture.job.BatchID).Error)
			require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *batch.OperationID).Update("identity_sha256", strings.Repeat("f", 64)).Error)
		},
		"sibling token type": func(t *testing.T, database *models.Database, fixture durableCallbackFixture) {
			var sibling models.DiggerJob
			require.NoError(t, database.GormDB.First(&sibling, "project_name = ?", "root-two").Error)
			require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("digger_job_database_id = ?", sibling.ID).Update("type", "tampered").Error)
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
			corrupt(t, database, fixture)
			_, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrDurableJobStatusCallbackConflict)

			var callbackCount int64
			require.NoError(t, database.GormDB.Model(&models.JobStatusCallback{}).Where("callback_id = ?", fixture.callback.CallbackID).Count(&callbackCount).Error)
			require.Zero(t, callbackCount)
			var caller models.DiggerJob
			require.NoError(t, database.GormDB.First(&caller, "id = ?", fixture.job.ID).Error)
			require.Equal(t, scheduler.DiggerJobStarted, caller.Status)
			require.Equal(t, int64(2), caller.StatusVersion)
			var callerToken models.JobToken
			require.NoError(t, database.GormDB.First(&callerToken, "digger_job_database_id = ?", fixture.job.ID).Error)
			require.Nil(t, callerToken.RevokedAt)
			var child models.DiggerJob
			require.NoError(t, database.GormDB.First(&child, "project_name = ?", "child").Error)
			var childEffects int64
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", *child.OperationID).Count(&childEffects).Error)
			require.Zero(t, childEffects)
		})
	}
}

func TestDurableJobStatusCallbackAcceptsHoldAndDrainButRejectsFencedWriter(t *testing.T) {
	for _, mode := range []models.ControlPlaneMode{models.ControlPlaneModeHold, models.ControlPlaneModeDrain} {
		t.Run(string(mode), func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
			require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("mode", mode).Error)
			applyDurableCallback(t, database, fixture, durableGraphTestWriterEpoch)
		})
	}

	database, organisation, delivery := newDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
	_, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrControlPlaneFenced)
}

func TestDurableJobStatusCallbacksAdvanceChildOnceAtCurrentWriterEpoch(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	first := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	second := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-two", 1002)
	const targetEpoch = durableGraphTestWriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", targetEpoch).Error)

	applyDurableCallback(t, database, first, targetEpoch)
	var childEffects int64
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("effect_key LIKE ?", "job:%").Count(&childEffects).Error)
	require.Equal(t, int64(2), childEffects)

	applyDurableCallback(t, database, second, targetEpoch)
	var child models.DiggerJob
	require.NoError(t, database.GormDB.First(&child, "project_name = ?", "child").Error)
	var childEffect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&childEffect, "operation_id = ?", *child.OperationID).Error)
	require.Equal(t, targetEpoch, childEffect.WriterEpoch)
	require.Equal(t, durableGraphTestWriterEpoch, *child.WriterEpoch)

	replay := applyDurableCallback(t, database, second, targetEpoch)
	require.True(t, replay.AlreadyApplied)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", *child.OperationID).Count(&childEffects).Error)
	require.Equal(t, int64(1), childEffects)
}

func TestDurableJobStatusCallbackFailureRevokesOnlyUnstartedJobs(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	failed := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	started := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-two", 1002)
	failed.callback.TargetStatus = "failed"
	applyDurableCallback(t, database, failed, durableGraphTestWriterEpoch)

	var startedJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&startedJob, "id = ?", started.job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, startedJob.Status)
	var startedToken models.JobToken
	require.NoError(t, database.GormDB.First(&startedToken, "digger_job_database_id = ?", started.job.ID).Error)
	require.Nil(t, startedToken.RevokedAt)

	var child models.DiggerJob
	require.NoError(t, database.GormDB.First(&child, "project_name = ?", "child").Error)
	require.Equal(t, scheduler.DiggerJobFailed, child.Status)
	var childToken models.JobToken
	require.NoError(t, database.GormDB.First(&childToken, "digger_job_database_id = ?", child.ID).Error)
	require.NotNil(t, childToken.RevokedAt)
}

func TestDurableJobStatusCallbackRollsBackIfReceiptCannotPersist(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	require.NoError(t, database.GormDB.Exec(`CREATE TRIGGER reject_callback_receipt BEFORE INSERT ON job_status_callbacks BEGIN SELECT RAISE(ABORT, 'receipt unavailable'); END`).Error)

	_, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.Error(t, err)

	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "id = ?", fixture.job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, job.Status)
	require.Equal(t, int64(2), job.StatusVersion)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", fixture.job.ID).Error)
	require.Nil(t, token.RevokedAt)
	var operationRow models.ControlOperation
	require.NoError(t, database.GormDB.First(&operationRow, "operation_id = ?", fixture.claim.OperationID).Error)
	require.Equal(t, models.ControlOperationPending, operationRow.Status)
}

func TestPostgresDurableJobStatusCallbackConcurrentReplayAppliesOnce(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)

	start := make(chan struct{})
	results := make(chan *models.DurableJobStatusCallbackReceipt, 2)
	errorsByWorker := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			if err != nil {
				errorsByWorker <- err
				return
			}
			results <- receipt
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsByWorker)
	for err := range errorsByWorker {
		require.NoError(t, err)
	}
	applied := 0
	replayed := 0
	for receipt := range results {
		if receipt.AlreadyApplied {
			replayed++
		} else {
			applied++
		}
	}
	require.Equal(t, 1, applied)
	require.Equal(t, 1, replayed)
}

func TestPostgresJobStatusCallbackRejectsMixedExactBindings(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	applyDurableCallback(t, database, fixture, durableGraphTestWriterEpoch)

	var original models.JobStatusCallback
	require.NoError(t, database.GormDB.First(&original, "callback_id = ?", fixture.callback.CallbackID).Error)
	var otherJob models.DiggerJob
	require.NoError(t, database.GormDB.Where("id <> ? AND operation_id IS NOT NULL", fixture.job.ID).First(&otherJob).Error)
	var otherToken models.JobToken
	require.NoError(t, database.GormDB.First(&otherToken, "digger_job_database_id = ?", otherJob.ID).Error)

	mixedJob := original
	mixedJob.CallbackID = uuid.New()
	mixedJob.DiggerJobID = otherJob.DiggerJobID
	mixedJob.Applied = false
	require.Error(t, database.GormDB.Create(&mixedJob).Error)

	mixedToken := original
	mixedToken.CallbackID = uuid.New()
	mixedToken.JobTokenID = otherToken.ID
	mixedToken.Applied = false
	require.Error(t, database.GormDB.Create(&mixedToken).Error)
}

func TestDurableJobStatusCallbackRejectsMalformedIdentity(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, database, organisation, delivery, "root-one", 1001)
	fixture.callback.ProtocolVersion = operation.ProtocolVersion + 1
	_, err := database.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.True(t, errors.Is(err, models.ErrDurableJobStatusCallbackConflict))
}
