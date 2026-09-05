package controllers

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func TestGithubWebhookProcessorKeepsPendingReportsBeyondRetryHorizon(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	var calls atomic.Int32
	processor := NewGithubWebhookProcessor(database, func(context.Context, *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		if calls.Add(1) <= 5 {
			return GithubWebhookProcessingResult{}, errGithubSubmissionReportsPending
		}
		return completedGithubWebhookResult("reports_ready", nil)
	}, testGithubWebhookProcessorConfig())
	processor.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, processor.Shutdown(ctx))
	})
	delivery := newGithubWebhookProcessorTestDelivery("pending-submission-reports")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliverySucceeded)
	require.EqualValues(t, 6, stored.AttemptCount)
	require.Nil(t, stored.DeadLetteredAt)
}

func TestPostgresGithubSubmissionResumeCreatesOneGraphAfterReportReceipts(t *testing.T) {
	database, organisation, delivery := newDurableExecutionIntegrationDatabase(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.GithubSubmission{}, &models.GithubReportReceipt{}))
	identity := models.JobCreationIdentity{DatabaseIdentity: durableExecutionIntegrationDatabaseIdentity,
		WriterEpoch: durableExecutionIntegrationWriterEpoch, ProtocolVersion: operation.ProtocolVersion,
		DeliveryOperationID: delivery.OperationID, DeliveryLeaseID: delivery.LeaseID}
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	target, err := preparation.Resolve(nil)
	require.NoError(t, err)
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), identity, target)
	require.NoError(t, err)
	_, err = database.GetGithubSubmission(context.Background(), identity)
	require.ErrorIs(t, err, models.ErrGithubSubmissionNotFound)
	wrongIdentity := identity
	wrongIdentity.DeliveryOperationID = "missing-delivery"
	_, err = database.GetGithubSubmission(context.Background(), wrongIdentity)
	require.Error(t, err)
	require.NotErrorIs(t, err, models.ErrGithubSubmissionNotFound)
	project := configuration.Project{Name: "root", WorkflowFile: "digger_workflow.yml"}
	projects, err := configuration.CreateProjectDependencyGraph([]configuration.Project{project})
	require.NoError(t, err)
	pr := target.PullRequestNumber
	graph, err := utils.PrepareDurableGraphIntent(utils.DurableJobGraphRequest{
		Identity: identity, JobType: scheduler.DiggerCommandApply, JobReporterType: "noop", OrganisationID: organisation.ID,
		Jobs:     map[string]scheduler.Job{"root": {ProjectName: "root", Commands: []string{"digger apply"}, PullRequestNumber: &pr}},
		Projects: map[string]configuration.Project{"root": project}, ProjectsGraph: projects,
		GithubInstallationID: delivery.InstallationIDValue(), Branch: target.HeadRef, PullRequestNumber: pr,
		RepoOwner: target.RepoOwner, RepoName: target.RepoName, RepoFullName: delivery.RepositoryFullName,
		CommitSHA: target.HeadSHA, DiggerConfig: "projects: []",
	})
	require.NoError(t, err)
	intent, err := utils.PrepareGithubSubmissionWithReports(models.GithubSubmissionIntent{Graph: *graph}, delivery.GithubAppID, time.Now().UTC())
	require.NoError(t, err)
	stored, _, err := database.RecordGithubSubmission(context.Background(), identity, intent)
	require.NoError(t, err)
	controller := DiggerController{ControlPlaneDatabaseIdentity: identity.DatabaseIdentity, ControlPlaneWriterEpoch: identity.WriterEpoch}
	for attempt := 0; attempt < 2; attempt++ {
		require.ErrorIs(t, controller.resumeGithubSubmission(context.Background(), identity, stored), errGithubSubmissionReportsPending)
	}
	var batchCount int64
	require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Count(&batchCount).Error)
	require.Zero(t, batchCount)
	var effects []models.OutboxEffect
	require.NoError(t, database.GormDB.Where("operation_id = ?", delivery.OperationID).Order("effect_key").Find(&effects).Error)
	require.Len(t, effects, len(intent.Reports))
	for index, effect := range effects {
		lease := "submission-report-create"
		require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
			"status": models.OutboxEffectProcessing, "lease_id": lease, "lease_expires_at": time.Now().UTC().Add(time.Minute),
		}).Error)
		prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, lease, identity.DatabaseIdentity, identity.WriterEpoch)
		require.NoError(t, err)
		require.True(t, prepared.MayCreate)
		providerID := int64(index + 100)
		providerURL, err := models.GithubReportProviderURL(prepared.Payload, providerID)
		require.NoError(t, err)
		raw, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
			ResourceKind: prepared.Payload.ResourceKind, ProviderID: providerID, ProviderURL: providerURL})
		require.NoError(t, err)
		require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, lease, raw, time.Now().UTC(), identity.DatabaseIdentity, identity.WriterEpoch))
	}
	require.NoError(t, controller.resumeGithubSubmission(context.Background(), identity, stored))
	identity.WriterEpoch++
	identity.DeliveryLeaseID = "submission-resume-handoff"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", identity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": identity.WriterEpoch, "lease_id": identity.DeliveryLeaseID}).Error)
	replayed, err := database.GetGithubSubmission(context.Background(), identity)
	require.NoError(t, err)
	require.NoError(t, controller.resumeGithubSubmission(context.Background(), identity, replayed))
	require.Equal(t, stored.IntentSHA256, replayed.IntentSHA256)
	require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Count(&batchCount).Error)
	require.EqualValues(t, 1, batchCount)
	var dispatchCount int64
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("effect_kind = ?", models.GithubWorkflowDispatchEffectKind).Count(&dispatchCount).Error)
	require.EqualValues(t, 1, dispatchCount)
}
