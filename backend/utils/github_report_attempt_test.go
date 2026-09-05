package utils

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newGithubReportAttemptFixture(t *testing.T) (*models.Database, models.JobCreationIdentity, *models.OutboxEffect) {
	t.Helper()
	database, request, _ := newGithubSubmissionFixture(t)
	var schema string
	require.NoError(t, database.GormDB.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.True(t, strings.HasPrefix(schema, "durable_graph_test_"))
	migration, err := os.ReadFile("../migrations/20260905049000_github_report_attempts.sql")
	require.NoError(t, err)
	statement := strings.ReplaceAll(string(migration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, database.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
	payload, err := models.CanonicalGithubReportCreatePayload(models.GithubReportCreatePayload{
		OrganisationID: request.OrganisationID, GithubAppID: 456, GithubInstallationID: request.GithubInstallationID,
		RepoOwner: request.RepoOwner, RepoName: request.RepoName, PullRequestNumber: 42,
		ResourceKind: models.GithubReportResourceComment, Body: "Prepared report",
	})
	require.NoError(t, err)
	effect := models.NewOutboxEffect(request.Identity.DeliveryOperationID, models.GithubReportCreateEffectKind, "summary", payload, request.Identity.WriterEpoch, time.Now().UTC())
	stored, _, err := database.EnqueueOutboxEffect(context.Background(), effect, request.Identity.DatabaseIdentity, request.Identity.WriterEpoch)
	require.NoError(t, err)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", stored.ID).Updates(map[string]any{
		"status": models.OutboxEffectProcessing, "lease_id": "report-create-lease", "lease_expires_at": time.Now().UTC().Add(time.Minute),
	}).Error)
	require.NoError(t, database.GormDB.First(stored, "id = ?", stored.ID).Error)
	return database, request.Identity, stored
}

func TestPostgresGithubReportAttemptOnlyFirstPreparationAllowsCreate(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	first, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.True(t, first.MayCreate)
	second, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.False(t, second.MayCreate, "same-lease retry must only recover, never POST again")
	require.Equal(t, first.Correlation, second.Correlation)
	require.True(t, first.AttemptedAt.Equal(second.AttemptedAt))
	newEpoch := identity.WriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", newEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{"writer_epoch": newEpoch, "lease_id": "new-report-lease"}).Error)
	_, err = database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.Error(t, err)
	resumed, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, "new-report-lease", identity.DatabaseIdentity, newEpoch)
	require.NoError(t, err)
	require.False(t, resumed.MayCreate)
	require.Equal(t, first.Correlation, resumed.Correlation)
	require.True(t, first.AttemptedAt.Equal(resumed.AttemptedAt))
	var attempt models.GithubReportCreateAttempt
	require.NoError(t, database.GormDB.First(&attempt, "effect_id = ?", effect.ID).Error)
	require.Equal(t, identity.WriterEpoch, attempt.WriterEpoch)
}

func TestPostgresGithubReportAttemptConcurrencyGrantsOneCreate(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	var workers sync.WaitGroup
	var creates atomic.Int32
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
			if err == nil && prepared.MayCreate {
				creates.Add(1)
			}
			errors <- err
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, creates.Load())
}

func TestPostgresGithubReportAttemptBindsJobAndBatchReports(t *testing.T) {
	for _, target := range []string{"job", "batch"} {
		for _, mutation := range []string{"none", "head", "batch digest", "sibling spec"} {
			database, identity, effect := newGithubReportAttemptFixture(t)
			original, err := models.DecodeGithubReportCreatePayload(effect.Payload)
			require.NoError(t, err)
			var org models.Organisation
			var delivery models.GithubWebhookDelivery
			require.NoError(t, database.GormDB.First(&org, "id = ?", original.OrganisationID).Error)
			require.NoError(t, database.GormDB.First(&delivery, "operation_id = ?", identity.DeliveryOperationID).Error)
			request := durableGraphTestRequest(t, &org, &delivery)
			batchID, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			var batch models.DiggerBatch
			require.NoError(t, database.GormDB.First(&batch, "id = ?", batchID.String()).Error)
			operationID := batch.OperationID
			if target == "job" {
				operationID = jobs["root-one"].OperationID
			}
			require.NotNil(t, operationID)
			original.ResourceKind, original.Body, original.HeadSHA = models.GithubReportResourceCheckRun, "", batch.CommitSha
			original.PullRequestNumber = batch.PrNumber
			original.Check = &models.GithubReportCheck{Name: "digger/plan", Status: "in_progress", Title: "Pending"}
			if mutation == "head" {
				original.HeadSHA = "different-commit"
			}
			if mutation == "batch digest" {
				require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", batch.OperationID).Update("identity_sha256", strings.Repeat("0", 64)).Error)
			}
			if mutation == "sibling spec" {
				require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", jobs["root-two"].ID).Update("serialized_job_spec", []byte(`{}`)).Error)
			}
			raw, err := models.CanonicalGithubReportCreatePayload(original)
			require.NoError(t, err)
			changed := models.NewOutboxEffect(*operationID, models.GithubReportCreateEffectKind, "batch-check:plan", raw, identity.WriterEpoch, time.Now())
			changed.LeaseID, changed.LeaseExpiresAt, changed.Status = effect.LeaseID, effect.LeaseExpiresAt, models.OutboxEffectProcessing
			effect = changed
			require.NoError(t, database.GormDB.Create(effect).Error)
			prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
			if mutation != "none" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, prepared.MayCreate)
			}
		}
	}
}

func TestPostgresGithubReportAttemptRejectsChangedEffectNamespace(t *testing.T) {
	for _, namespace := range []string{"effect_key", "operation_id"} {
		database, identity, effect := newGithubReportAttemptFixture(t)
		_, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
		require.NoError(t, err)
		changed := "different-report"
		if namespace == "operation_id" {
			var source models.GithubWebhookDelivery
			require.NoError(t, database.GormDB.First(&source, "operation_id = ?", identity.DeliveryOperationID).Error)
			other, _, err := database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
				DeliveryID: "different-report-delivery", Payload: source.Payload, PayloadSHA256: source.PayloadSHA256,
				EventType: source.EventType, GithubAppID: source.GithubAppID, InstallationID: source.InstallationID, RepositoryFullName: source.RepositoryFullName,
			}, identity.DatabaseIdentity, identity.WriterEpoch)
			require.NoError(t, err)
			changed = other.OperationID
		}
		require.Error(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update(namespace, changed).Error)
		// Import-corruption defense remains testable in this isolated schema.
		require.NoError(t, database.GormDB.Exec("ALTER TABLE outbox_effects DISABLE TRIGGER outbox_effect_identity_rows").Error)
		require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update(namespace, changed).Error)
		require.NoError(t, database.GormDB.Exec("ALTER TABLE outbox_effects ENABLE TRIGGER outbox_effect_identity_rows").Error)
		_, err = database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
		require.Error(t, err)
		raw, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
			ResourceKind: models.GithubReportResourceComment, ProviderID: 123, ProviderURL: "https://github.com/monoai-co/sre/pull/42#issuecomment-123"})
		require.NoError(t, err)
		require.Error(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, raw, time.Now(), identity.DatabaseIdentity, identity.WriterEpoch))
	}
}

func TestPostgresGithubReportAttemptCannotChangeKindToBypassReceipt(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	_, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.Error(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("effect_kind", "unrelated").Error)
	require.NoError(t, database.GormDB.Exec("ALTER TABLE outbox_effects DISABLE TRIGGER outbox_effect_identity_rows").Error)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("effect_kind", "unrelated").Error)
	require.NoError(t, database.GormDB.Exec("ALTER TABLE outbox_effects ENABLE TRIGGER outbox_effect_identity_rows").Error)
	require.Error(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, []byte(`{}`), time.Now(), identity.DatabaseIdentity, identity.WriterEpoch))
	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, stored.Status)
	require.Equal(t, "unrelated", stored.EffectKind)
}

func TestPostgresGithubReportReceiptSurvivesInstallationDeactivation(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.True(t, prepared.MayCreate)
	require.NoError(t, database.GormDB.Model(&models.GithubAppInstallationLink{}).Where("github_installation_id = ?", prepared.Payload.GithubInstallationID).Update("status", models.GithubAppInstallationLinkInactive).Error)
	replayed, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.False(t, replayed.MayCreate)
	unstarted := models.NewOutboxEffect(effect.ControlOperationID, effect.EffectKind, "unstarted-report", effect.Payload, effect.WriterEpoch, time.Now())
	unstarted.LeaseID, unstarted.LeaseExpiresAt, unstarted.Status = effect.LeaseID, effect.LeaseExpiresAt, models.OutboxEffectProcessing
	require.NoError(t, database.GormDB.Create(unstarted).Error)
	_, err = database.PrepareGithubReportCreate(context.Background(), unstarted.ID, unstarted.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.Error(t, err, "deactivated installation must not authorize another create")
	raw, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256, ResourceKind: models.GithubReportResourceComment,
		ProviderID: 123, ProviderURL: "https://github.com/monoai-co/sre/pull/42#issuecomment-123"})
	require.NoError(t, err)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, raw, time.Now(), identity.DatabaseIdentity, identity.WriterEpoch))
	var receipt models.GithubReportReceipt
	require.NoError(t, database.GormDB.First(&receipt, "effect_id = ?", effect.ID).Error)
	require.EqualValues(t, 123, receipt.ProviderID)
}

func TestPostgresGithubReportReceiptCompletionIsAtomicAndImmutable(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	_, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	receipt := models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256, ResourceKind: models.GithubReportResourceComment,
		ProviderID: 123, ProviderURL: "https://github.com/monoai-co/sre/pull/42#issuecomment-123"}
	wrong := receipt
	wrong.EffectID = uuid.New()
	raw, err := json.Marshal(wrong)
	require.NoError(t, err)
	require.Error(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, raw, time.Now(), identity.DatabaseIdentity, identity.WriterEpoch))
	var loaded models.OutboxEffect
	require.NoError(t, database.GormDB.First(&loaded, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, loaded.Status, "invalid receipt must roll back outbox completion")
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubReportReceipt{}).Count(&count).Error)
	require.Zero(t, count)
	raw, err = json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, raw, time.Now(), identity.DatabaseIdentity, identity.WriterEpoch))
	var stored models.GithubReportReceipt
	require.NoError(t, database.GormDB.First(&stored, "effect_id = ?", effect.ID).Error)
	require.Equal(t, receipt.ProviderID, stored.ProviderID)
	require.Equal(t, receipt.ProviderURL, stored.ProviderURL)
	require.NoError(t, database.GormDB.First(&loaded, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectSucceeded, loaded.Status)
	require.Error(t, database.GormDB.Model(&models.GithubReportReceipt{}).Where("effect_id = ?", effect.ID).Update("provider_id", 456).Error)
	require.Error(t, database.GormDB.Where("effect_id = ?", effect.ID).Delete(&models.GithubReportCreateAttempt{}).Error)
}

func TestPostgresGithubReportReceiptCannotAliasAnotherEffect(t *testing.T) {
	database, identity, first := newGithubReportAttemptFixture(t)
	payload, err := models.DecodeGithubReportCreatePayload(first.Payload)
	require.NoError(t, err)
	payload.PullRequestNumber = 43
	raw, err := models.CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	second := models.NewOutboxEffect(first.ControlOperationID, models.GithubReportCreateEffectKind, "other-summary", raw, identity.WriterEpoch, time.Now())
	second.LeaseID, second.LeaseExpiresAt, second.Status = first.LeaseID, first.LeaseExpiresAt, models.OutboxEffectProcessing
	require.NoError(t, database.GormDB.Create(second).Error)
	for _, effect := range []*models.OutboxEffect{first, second} {
		prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
		require.NoError(t, err)
		providerURL, err := models.GithubReportProviderURL(prepared.Payload, 123)
		require.NoError(t, err)
		receipt, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256, ResourceKind: models.GithubReportResourceComment, ProviderID: 123, ProviderURL: providerURL})
		require.NoError(t, err)
		err = database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, receipt, time.Now(), identity.DatabaseIdentity, identity.WriterEpoch)
		if effect.ID == first.ID {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubReportReceipt{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	var loaded models.OutboxEffect
	require.NoError(t, database.GormDB.First(&loaded, "id = ?", second.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, loaded.Status)
}

func TestPostgresGithubReportAttemptRejectsExpiredLeaseBeforeCreate(t *testing.T) {
	database, identity, effect := newGithubReportAttemptFixture(t)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("lease_expires_at", time.Now().UTC().Add(-time.Minute)).Error)
	_, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
	require.Error(t, err)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubReportCreateAttempt{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestPostgresGithubReportReceiptRejectsMissingPermitAndExpiredLease(t *testing.T) {
	for _, permit := range []bool{false, true} {
		database, identity, effect := newGithubReportAttemptFixture(t)
		if permit {
			_, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, effect.LeaseID, identity.DatabaseIdentity, identity.WriterEpoch)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("lease_expires_at", time.Now().UTC().Add(-time.Minute)).Error)
		}
		raw, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
			ResourceKind: models.GithubReportResourceComment, ProviderID: 123, ProviderURL: "https://github.com/monoai-co/sre/pull/42#issuecomment-123"})
		require.NoError(t, err)
		require.Error(t, database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, raw, time.Now().Add(-time.Hour), identity.DatabaseIdentity, identity.WriterEpoch))
		var count int64
		require.NoError(t, database.GormDB.Model(&models.GithubReportReceipt{}).Count(&count).Error)
		require.Zero(t, count)
		var stored models.OutboxEffect
		require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
		require.Equal(t, models.OutboxEffectProcessing, stored.Status)
	}
}
