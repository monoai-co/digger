package utils

import (
	"context"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func TestPostgresCallbackWaitingAcrossGrantExpiryCannotResolveUnknownApply(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	installRecoveryHistoryGuards(t, db)
	fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
	watch := prepareRunReconciliationTest(t, db, fixture.claim)
	var expires time.Time
	require.NoError(t, db.GormDB.Raw("SELECT clock_timestamp() + interval '1 second'").Scan(&expires).Error)
	require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", fixture.claim.OperationID).Update("grant_expires_at", expires).Error)
	blocker := db.GormDB.Begin()
	require.NoError(t, blocker.Error)
	t.Cleanup(func() { _ = blocker.Rollback().Error })
	var blockerPID int
	require.NoError(t, blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error)
	var batch models.DiggerBatch
	require.NoError(t, blocker.Raw("SELECT * FROM digger_batches WHERE id = ? FOR UPDATE", *fixture.job.BatchID).Scan(&batch).Error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, err := db.ApplyDurableJobStatusCallback(ctx, fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		completed <- err
	}()
	// Prove the callback has entered its transaction and is waiting on our row
	// lock while the grant remains valid. Avoid scheduler-dependent sleeps.
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.GormDB.Raw("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)))", blockerPID).Scan(&waiting).Error
		return err == nil && waiting
	}, time.Second, time.Millisecond)
	var stillValid bool
	require.NoError(t, db.GormDB.Raw("SELECT clock_timestamp() < ?", expires).Scan(&stillValid).Error)
	require.True(t, stillValid, "callback must begin waiting before grant expiry")
	require.Eventually(t, func() bool {
		var expired bool
		return db.GormDB.Raw("SELECT clock_timestamp() >= ?", expires).Scan(&expired).Error == nil && expired
	}, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, blocker.Commit().Error)
	require.ErrorIs(t, <-completed, models.ErrDurableJobStatusCallbackConflict)
	terminal, err := db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, runObservationForClaim(fixture.claim), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, terminal)
	_, err = db.ApplyDurableJobStatusCallback(context.Background(), fixture.callback, fixture.job.DiggerJobID, fixture.token, fixture.grant, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobStatusCallbackConflict)
	recovery, err := db.GetApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", recovery.Outcome)
	var job models.DiggerJob
	require.NoError(t, db.GormDB.First(&job, fixture.job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, job.Status)
	require.Equal(t, int64(2), job.StatusVersion)
}

func TestPostgresCommittedCallbackReceiptReplaysAfterExpiry(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
	first := applyDurableCallback(t, db, fixture, durableGraphTestWriterEpoch)
	require.False(t, first.AlreadyApplied)
	expireRecoveryClaim(t, db, fixture)
	replayed := applyDurableCallback(t, db, fixture, durableGraphTestWriterEpoch)
	require.True(t, replayed.AlreadyApplied)
	require.Equal(t, first.ResponseStatus, replayed.ResponseStatus)
	require.JSONEq(t, string(first.ResponseBody), string(replayed.ResponseBody))
}

func TestPostgresCommittedExecutionGrantReplaysDuringHoldAndDrain(t *testing.T) {
	for _, mode := range []models.ControlPlaneMode{models.ControlPlaneModeHold, models.ControlPlaneModeDrain} {
		t.Run(string(mode), func(t *testing.T) {
			db, org, delivery := newPostgresDurableGraphTestDatabase(t)
			request := durableGraphTestRequestForProjects(t, org, delivery, []configuration.Project{{Name: "one", WorkflowFile: "digger_workflow.yml"}, {Name: "two", WorkflowFile: "digger_workflow.yml"}})
			first := prepareDurableCallbackFixtureForRequest(t, db, request, "one", 1001)
			_, secondClaim, secondToken := prepareDurableExecutionClaimForRequestTest(t, db, request, "two", 1002)
			var original models.ExecutionClaimAttempt
			require.NoError(t, db.GormDB.First(&original, "operation_id = ?", first.claim.OperationID).Error)
			require.NoError(t, db.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("mode", mode).Error)
			secrets := durableGraphTestGrantSecrets([]byte("grant-secret-grant-secret-grant-secret-"))
			replayed, err := db.ClaimDurableJobExecution(context.Background(), first.claim, first.token, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.True(t, replayed.Granted)
			require.True(t, replayed.AlreadyGranted)
			require.Equal(t, first.grant, replayed.ExecutionGrant)
			require.Equal(t, original.SigningKeyID, replayed.SigningKeyID)
			require.True(t, original.GrantExpiresAt.Equal(replayed.GrantExpiresAt))
			_, err = db.ClaimDurableJobExecution(context.Background(), secondClaim, secondToken, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			expected := models.ErrControlPlaneHold
			if mode == models.ControlPlaneModeDrain {
				expected = models.ErrControlPlaneDrain
			}
			require.ErrorIs(t, err, expected)
			var count int64
			require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", secondClaim.OperationID).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestPostgresClaimWaitingAcrossPriorGrantExpiryRequiresRecovery(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequestForProjects(t, org, delivery, []configuration.Project{{Name: "one", WorkflowFile: "digger_workflow.yml"}, {Name: "two", WorkflowFile: "digger_workflow.yml"}})
	first := prepareDurableCallbackFixtureForRequest(t, db, request, "one", 1001)
	target, claim, token := prepareDurableExecutionClaimForRequestTest(t, db, request, "two", 1002)
	var expires time.Time
	require.NoError(t, db.GormDB.Raw("SELECT clock_timestamp() + interval '1 second'").Scan(&expires).Error)
	require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", first.claim.OperationID).Update("grant_expires_at", expires).Error)
	blocker := db.GormDB.Begin()
	require.NoError(t, blocker.Error)
	t.Cleanup(func() { _ = blocker.Rollback().Error })
	var blockerPID int
	require.NoError(t, blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error)
	var batch models.DiggerBatch
	require.NoError(t, blocker.Raw("SELECT * FROM digger_batches WHERE id = ? FOR UPDATE", *target.BatchID).Scan(&batch).Error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, err := db.ClaimDurableJobExecution(ctx, claim, token, durableGraphTestGrantSecrets([]byte("grant-secret-grant-secret-grant-secret-")), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		completed <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		return db.GormDB.Raw("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)))", blockerPID).Scan(&waiting).Error == nil && waiting
	}, time.Second, time.Millisecond)
	var stillValid bool
	require.NoError(t, db.GormDB.Raw("SELECT clock_timestamp() < ?", expires).Scan(&stillValid).Error)
	require.True(t, stillValid, "claim must begin waiting before prior grant expiry")
	require.Eventually(t, func() bool {
		var expired bool
		return db.GormDB.Raw("SELECT clock_timestamp() >= ?", expires).Scan(&expired).Error == nil && expired
	}, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, blocker.Commit().Error)
	require.ErrorIs(t, <-completed, models.ErrApplyRecoveryRequired)
	var count int64
	require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", claim.OperationID).Count(&count).Error)
	require.Zero(t, count)
	var stored models.DiggerJob
	require.NoError(t, db.GormDB.First(&stored, target.ID).Error)
	require.Equal(t, scheduler.DiggerJobTriggered, stored.Status)
}
