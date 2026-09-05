package utils

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newGithubSubmissionReceiptFixture(t *testing.T) (*models.Database, models.JobCreationIdentity, models.GithubSubmissionIntent) {
	t.Helper()
	database, request, intent := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.GithubReportReceipt{}))
	prepared, err := PrepareGithubSubmissionWithReports(intent, 456, time.Now().UTC())
	require.NoError(t, err)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, prepared)
	require.NoError(t, err)
	return database, request.Identity, prepared
}

func completeSubmissionReport(t *testing.T, database *models.Database, identity models.JobCreationIdentity, effect *models.OutboxEffect, providerID int64) {
	t.Helper()
	lease := "receipt-test-lease"
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
		"status": models.OutboxEffectProcessing, "lease_id": lease, "lease_expires_at": time.Now().UTC().Add(time.Minute),
	}).Error)
	prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, lease, identity.DatabaseIdentity, identity.WriterEpoch)
	require.NoError(t, err)
	require.True(t, prepared.MayCreate)
	providerURL, err := models.GithubReportProviderURL(prepared.Payload, providerID)
	require.NoError(t, err)
	raw, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
		ResourceKind: prepared.Payload.ResourceKind, ProviderID: providerID, ProviderURL: providerURL})
	require.NoError(t, err)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, lease, raw, time.Now().UTC(), identity.DatabaseIdentity, identity.WriterEpoch))
}

func TestPostgresGithubSubmissionReceiptsWaitForRequiredReportsAndSurviveHandoff(t *testing.T) {
	database, identity, intent := newGithubSubmissionReceiptFixture(t)
	results, ready, err := database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.NoError(t, err)
	require.False(t, ready)
	require.Empty(t, results)
	effects, err := database.EnqueueGithubSubmissionReports(context.Background(), identity)
	require.NoError(t, err)
	var optional *models.OutboxEffect
	for index, effect := range effects {
		if intent.Reports[index].Optional {
			optional = effect
			continue
		}
		completeSubmissionReport(t, database, identity, effect, int64(index+100))
	}
	results, ready, err = database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, results, len(intent.Reports)-1)
	for index, result := range results {
		require.Positive(t, result.Receipt.ProviderID)
		if index > 0 {
			require.Greater(t, result.Report.Order, results[index-1].Report.Order)
		}
	}
	require.NotNil(t, optional)
	completeSubmissionReport(t, database, identity, optional, 999)
	before, ready, err := database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, before, len(intent.Reports))
	oldIdentity := identity
	identity.WriterEpoch++
	identity.DeliveryLeaseID = "receipt-handoff"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", identity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": identity.WriterEpoch, "lease_id": identity.DeliveryLeaseID}).Error)
	after, ready, err := database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, before, after)
	_, ready, err = database.ReadGithubSubmissionReportReceipts(context.Background(), oldIdentity)
	require.Error(t, err)
	require.False(t, ready)
}

func TestPostgresGithubSubmissionReceiptsRejectIncompleteOrCorruptSuccess(t *testing.T) {
	database, identity, _ := newGithubSubmissionReceiptFixture(t)
	effects, err := database.EnqueueGithubSubmissionReports(context.Background(), identity)
	require.NoError(t, err)
	effect := effects[0]
	completeSubmissionReport(t, database, identity, effect, 123)
	for name, mutate := range map[string]func(*gorm.DB) error{
		"missing receipt": func(tx *gorm.DB) error {
			return tx.Delete(&models.GithubReportReceipt{}, "effect_id = ?", effect.ID).Error
		},
		"wrong receipt URL": func(tx *gorm.DB) error {
			return tx.Model(&models.GithubReportReceipt{}).Where("effect_id = ?", effect.ID).Update("provider_url", "https://github.com/other/repo").Error
		},
		"wrong provider identity": func(tx *gorm.DB) error {
			return tx.Model(&models.GithubReportReceipt{}).Where("effect_id = ?", effect.ID).Update("provider_identity_sha256", effect.PayloadSHA256).Error
		},
		"wrong attempt lease": func(tx *gorm.DB) error {
			return tx.Model(&models.GithubReportCreateAttempt{}).Where("effect_id = ?", effect.ID).Update("lease_id", "").Error
		},
		"wrong completion copy": func(tx *gorm.DB) error {
			return tx.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("provider_receipt", []byte(`{}`)).Error
		},
		"wrong payload digest": func(tx *gorm.DB) error {
			return tx.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("payload_sha256", "corrupt").Error
		},
	} {
		t.Run(name, func(t *testing.T) {
			tx := database.GormDB.Begin()
			require.NoError(t, tx.Error)
			defer tx.Rollback()
			require.NoError(t, mutate(tx))
			results, ready, err := (&models.Database{GormDB: tx}).ReadGithubSubmissionReportReceipts(context.Background(), identity)
			require.Error(t, err)
			require.False(t, ready)
			require.Nil(t, results)
		})
	}
	_, _, err = database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.NoError(t, err, "each corruption must be rolled back independently")
}

func TestPostgresGithubSubmissionReceiptsRequiredFailureAndConcurrentWorker(t *testing.T) {
	database, identity, _ := newGithubSubmissionReceiptFixture(t)
	effects, err := database.EnqueueGithubSubmissionReports(context.Background(), identity)
	require.NoError(t, err)
	effect := effects[0]
	worker := database.GormDB.Begin()
	require.NoError(t, worker.Error)
	defer worker.Rollback()
	require.NoError(t, worker.Exec("SELECT id FROM outbox_effects WHERE id = ? FOR UPDATE", effect.ID).Error)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, ready, err := database.ReadGithubSubmissionReportReceipts(ctx, identity)
	require.NoError(t, err, "receipt reads must not wait on an effect lock held by a worker")
	require.False(t, ready)
	require.NoError(t, worker.Rollback().Error)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("status", models.OutboxEffectDeadLetter).Error)
	_, ready, err = database.ReadGithubSubmissionReportReceipts(context.Background(), identity)
	require.ErrorIs(t, err, models.ErrGithubSubmissionReportFailed)
	require.False(t, ready)
}
