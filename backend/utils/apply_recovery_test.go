package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func installRecoveryHistoryGuards(t *testing.T, db *models.Database) {
	t.Helper()
	var schema string
	require.NoError(t, db.GormDB.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.True(t, strings.HasPrefix(schema, "durable_graph_test_"))
	migration, err := os.ReadFile("../migrations/20260905047000_apply_recovery.sql")
	require.NoError(t, err)
	statement := strings.ReplaceAll(string(migration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, db.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
}

func expireRecoveryClaim(t *testing.T, db *models.Database, fixture durableCallbackFixture) {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", fixture.claim.OperationID).Updates(map[string]any{"created_at": now.Add(-2 * time.Hour), "granted_at": now.Add(-time.Hour), "grant_expires_at": now.Add(-time.Minute)}).Error)
}

func recoveryResolution(outcome string) models.ResolveApplyRecoveryRequest {
	return models.ResolveApplyRecoveryRequest{Reason: "Executor stopped; state and resources reconciled against the retained execution evidence", ResolutionID: uuid.New(), ExpectedRevision: 1, Outcome: outcome, ExecutorStopped: true, EvidenceURI: "s3://recovery-evidence/immutable-package", ExecutorEvidenceSHA256: strings.Repeat("1", 64), StateEvidenceSHA256: strings.Repeat("2", 64), ResourceEvidenceSHA256: strings.Repeat("3", 64), ResultEvidenceSHA256: strings.Repeat("4", 64)}
}

func TestPostgresApplyRecoveryPreservesUnknownAndResolvesExactlyOnce(t *testing.T) {
	for _, outcome := range []string{"verified_succeeded", "aborted"} {
		t.Run(outcome, func(t *testing.T) {
			db, org, delivery := newPostgresDurableGraphTestDatabase(t)
			installRecoveryHistoryGuards(t, db)
			fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
			watch := prepareRunReconciliationTest(t, db, fixture.claim)
			expireRecoveryClaim(t, db, fixture)
			observation := runObservationForClaim(fixture.claim)
			terminal, err := db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.True(t, terminal)
			recovery, err := db.GetApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID)
			require.NoError(t, err)
			require.Equal(t, "unknown", recovery.Outcome)
			// Lose the outbox-completion response after the unknown record commits.
			_, err = db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			encoded, err := json.Marshal(observation)
			require.NoError(t, err)
			require.NoError(t, db.CompleteOutboxEffect(context.Background(), watch.ID, watch.LeaseID, encoded, time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
			var before models.DiggerJob
			require.NoError(t, db.GormDB.First(&before, "id = ?", fixture.job.ID).Error)
			require.Equal(t, scheduler.DiggerJobStarted, before.Status)
			require.Equal(t, int64(2), before.StatusVersion)
			request := recoveryResolution(outcome)
			var workers sync.WaitGroup
			for i := 0; i < 8; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					resolved, err := db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
					if err != nil {
						t.Errorf("resolution: %v", err)
						return
					}
					if resolved.Outcome != outcome || resolved.Revision != 2 {
						t.Errorf("unexpected resolution: %+v", resolved)
					}
				}()
			}
			workers.Wait()
			stored, err := db.GetApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID)
			require.NoError(t, err)
			require.Equal(t, outcome, stored.Outcome)
			require.Equal(t, request.ResolutionID, *stored.ResolutionID)
			var job models.DiggerJob
			require.NoError(t, db.GormDB.First(&job, "id = ?", fixture.job.ID).Error)
			expected := scheduler.DiggerJobFailed
			if outcome == "verified_succeeded" {
				expected = scheduler.DiggerJobSucceeded
			}
			require.Equal(t, expected, job.Status)
			require.Equal(t, int64(3), job.StatusVersion)
			var token models.JobToken
			require.NoError(t, db.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
			require.NotNil(t, token.RevokedAt)
			request.ResultEvidenceSHA256 = strings.Repeat("5", 64)
			_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrApplyRecoveryConflict)
		})
	}
}

func TestPostgresWorkflowCompletionAtomicallyEnqueuesReconciliation(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	_, claim, _ := prepareDurableExecutionClaimTest(t, db, org, delivery)
	var dispatch models.OutboxEffect
	require.NoError(t, db.GormDB.First(&dispatch, "operation_id = ? AND effect_kind = ?", claim.OperationID, models.GithubWorkflowDispatchEffectKind).Error)
	lease := "completion-lease"
	require.NoError(t, db.GormDB.Model(&dispatch).Updates(map[string]any{"status": models.OutboxEffectProcessing, "lease_id": lease, "lease_expires_at": time.Now().UTC().Add(time.Minute)}).Error)
	conflictingPayload, err := json.Marshal(models.GithubRunReconciliationPayload{OperationID: claim.OperationID, DispatchEffectID: uuid.New()})
	require.NoError(t, err)
	conflict := models.NewOutboxEffect(claim.OperationID, models.GithubWorkflowReconcileEffectKind, fmt.Sprintf("run:%d:%d", claim.RepositoryID, claim.RunID), conflictingPayload, durableGraphTestWriterEpoch, time.Now().UTC())
	_, _, err = db.EnqueueOutboxEffect(context.Background(), conflict, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	err = db.CompleteOutboxEffect(context.Background(), dispatch.ID, lease, dispatch.ProviderReceipt, time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrOutboxEffectConflict)
	var unchanged models.OutboxEffect
	require.NoError(t, db.GormDB.First(&unchanged, "id = ?", dispatch.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, unchanged.Status)
	require.Equal(t, lease, unchanged.LeaseID)
	require.NoError(t, db.GormDB.Delete(conflict).Error)
	require.NoError(t, db.CompleteOutboxEffect(context.Background(), dispatch.ID, lease, dispatch.ProviderReceipt, time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
	var watches []models.OutboxEffect
	require.NoError(t, db.GormDB.Where("operation_id = ? AND effect_kind = ?", claim.OperationID, models.GithubWorkflowReconcileEffectKind).Find(&watches).Error)
	require.Len(t, watches, 1)
	require.True(t, watches[0].ValidPayloadDigest())
	var payload models.GithubRunReconciliationPayload
	require.NoError(t, json.Unmarshal(watches[0].Payload, &payload))
	require.Equal(t, dispatch.ID, payload.DispatchEffectID)
}

func TestPostgresApplyRecoveryRequiresTerminalExecutorAndFences(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	installRecoveryHistoryGuards(t, db)
	fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
	watch := prepareRunReconciliationTest(t, db, fixture.claim)
	expireRecoveryClaim(t, db, fixture)
	observation := runObservationForClaim(fixture.claim)
	observation.Status = "in_progress"
	observation.Conclusion = ""
	terminal, err := db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, terminal)
	request := recoveryResolution("aborted")
	_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrApplyRecoveryConflict)
	observation = runObservationForClaim(fixture.claim)
	_, err = db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch+1)
	require.ErrorIs(t, err, models.ErrControlPlaneFenced)
	request.ExecutorStopped = false
	_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrApplyRecoveryConflict)
}

func TestPostgresExpiredExecutionBlocksNewGrantBeforeReconciliation(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequestForProjects(t, org, delivery, []configuration.Project{{Name: "one", WorkflowFile: "digger_workflow.yml"}, {Name: "two", WorkflowFile: "digger_workflow.yml"}})
	first := prepareDurableCallbackFixtureForRequest(t, db, request, "one", 1001)
	_, claim, token := prepareDurableExecutionClaimForRequestTest(t, db, request, "two", 1002)
	expireRecoveryClaim(t, db, first)
	_, err := db.ClaimDurableJobExecution(context.Background(), claim, token, durableGraphTestGrantSecrets([]byte("grant-secret-grant-secret-grant-secret-")), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrApplyRecoveryRequired)
	var count int64
	require.NoError(t, db.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("operation_id = ?", claim.OperationID).Count(&count).Error)
	require.Zero(t, count)
}

func TestPostgresApplyRecoverySurvivesUnavailableProviderAndUninstalledApp(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	installRecoveryHistoryGuards(t, db)
	fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
	watch := prepareRunReconciliationTest(t, db, fixture.claim)
	expireRecoveryClaim(t, db, fixture)
	require.NoError(t, db.GormDB.Model(&models.GithubAppInstallationLink{}).Where("organisation_id = ?", org.ID).Update("status", models.GithubAppInstallationLinkInactive).Error)
	preparation, err := db.PrepareDurableRunReconciliation(context.Background(), watch.ID, watch.LeaseID, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	observation := preparation.CanonicalObservation
	observation.Status = "unavailable"
	terminal, err := db.ReconcileDurableWorkflowRun(context.Background(), watch.ID, watch.LeaseID, observation, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, terminal)
	request := recoveryResolution("aborted")
	_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrApplyRecoveryConflict)
	request.ProviderUnavailable = true
	recovery, err := db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.Equal(t, "aborted", recovery.Outcome)
}

func TestPostgresExecutionGrantDoesNotInheritLegacyTokenRetention(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	job, claim, token := prepareDurableExecutionClaimTest(t, db, org, delivery)
	require.NoError(t, db.GormDB.Model(&models.JobToken{}).Where("digger_job_database_id = ?", job.ID).Update("expiry", time.Now().UTC().Add(31*24*time.Hour)).Error)
	secrets := durableGraphTestGrantSecrets([]byte("grant-secret-grant-secret-grant-secret-"))
	receipt, err := db.ClaimDurableJobExecution(context.Background(), claim, token, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().UTC().Add(models.DurableExecutionGrantWindow), receipt.GrantExpiresAt, 5*time.Second)
	expireRecoveryClaim(t, db, durableCallbackFixture{claim: claim})
	_, err = db.ClaimDurableJobExecution(context.Background(), claim, token, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
}

func TestPostgresUnknownRecoveryExistsBeforeProviderLookup(t *testing.T) {
	db, org, delivery := newPostgresDurableGraphTestDatabase(t)
	installRecoveryHistoryGuards(t, db)
	fixture := prepareDurableCallbackFixture(t, db, org, delivery, "root-one", 1001)
	watch := prepareRunReconciliationTest(t, db, fixture.claim)
	expireRecoveryClaim(t, db, fixture)
	_, err := db.PrepareDurableRunReconciliation(context.Background(), watch.ID, watch.LeaseID, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	recovery, err := db.GetApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", recovery.Outcome)
	require.Empty(t, recovery.TerminalObservation)
	request := recoveryResolution("aborted")
	_, err = db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrApplyRecoveryConflict)
	request.ProviderUnavailable = true
	resolved, err := db.ResolveApplyRecovery(context.Background(), fixture.claim.OperationID, org.ID, "operator:42", request, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.Equal(t, "aborted", resolved.Outcome)
	require.Empty(t, resolved.TerminalObservation, "operator resolution must not fabricate a provider completion")
}
