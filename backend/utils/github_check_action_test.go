package utils

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func legacyGithubCheckActionDatabase(t *testing.T) (*models.Database, *models.DiggerBatch, *models.GithubWebhookDelivery, *github.CheckRunEvent) {
	t.Helper()
	database, _, delivery := newPostgresDurableGraphTestDatabase(t)
	batch := models.DiggerBatch{ID: uuid.New(), DiggerBatchID: "legacy-batch", VCS: models.DiggerVCSGithub, PrNumber: 42,
		CommitSha: "original-head", BranchName: "feature/legacy", GithubInstallationId: 123, RepoOwner: "monoai-co", RepoName: "sre", RepoFullName: "monoai-co/sre", CheckRunId: github.String("900")}
	require.NoError(t, database.GormDB.Create(&batch).Error)
	summary := models.DiggerJobSummary{}
	require.NoError(t, database.GormDB.Create(&summary).Error)
	job := models.DiggerJob{DiggerJobID: "legacy-job", BatchID: github.String(batch.ID.String()), CheckRunId: github.String("901"), DiggerJobSummaryID: summary.ID}
	require.NoError(t, database.GormDB.Create(&job).Error)
	event := &github.CheckRunEvent{Action: github.String("requested_action"), Installation: &github.Installation{ID: github.Int64(123)},
		Repo:            &github.Repository{ID: github.Int64(91), Name: github.String("sre"), FullName: github.String("monoai-co/sre"), Owner: &github.User{Login: github.String("monoai-co")}},
		CheckRun:        &github.CheckRun{ID: github.Int64(901), HeadSHA: github.String("original-head"), App: &github.App{ID: github.Int64(456)}},
		RequestedAction: &github.RequestedAction{Identifier: "abatch:legacy-batch"}}
	return database, &batch, delivery, event
}

func TestPostgresLegacyGithubCheckActionScopesLookupAndRejectsAmbiguousBatch(t *testing.T) {
	database, batch, _, event := legacyGithubCheckActionDatabase(t)
	foreign := *batch
	foreign.ID, foreign.GithubInstallationId = uuid.New(), 999
	require.NoError(t, database.GormDB.Create(&foreign).Error)
	resolved, jobs, err := database.ResolveLegacyGithubCheckAction(context.Background(), event, 456)
	require.NoError(t, err)
	require.Equal(t, batch.ID, resolved.ID)
	require.Len(t, jobs, 1)
	require.Equal(t, "legacy-job", jobs[0].DiggerJobID)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = database.ResolveLegacyGithubCheckAction(canceled, event, 456)
	require.ErrorIs(t, err, context.Canceled)
	duplicate := *batch
	duplicate.ID = uuid.New()
	require.NoError(t, database.GormDB.Create(&duplicate).Error)
	_, _, err = database.ResolveLegacyGithubCheckAction(context.Background(), event, 456)
	require.ErrorIs(t, err, models.ErrGithubCheckActionBinding)
}

func TestPostgresLegacyGithubCheckTargetCannotSelectAnotherPRAndReplaysSavedBranch(t *testing.T) {
	database, batch, previous, event := legacyGithubCheckActionDatabase(t)
	require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), previous.DeliveryID, previous.LeaseID, models.GithubWebhookDeliveryIgnored, "fixture_only", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	installationID := batch.GithubInstallationId
	_, _, err = database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
		DeliveryID: "legacy-check-target", Payload: raw, PayloadSHA256: fmt.Sprintf("%x", sha256.Sum256(raw)), EventType: "check_run",
		GithubAppID: 456, InstallationID: &installationID, RepositoryFullName: batch.RepoFullName,
	}, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	delivery, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "check-target-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	identity := models.JobCreationIdentity{DatabaseIdentity: durableGraphTestDatabaseIdentity, WriterEpoch: durableGraphTestWriterEpoch,
		ProtocolVersion: operation.ProtocolVersion, DeliveryOperationID: delivery.OperationID, DeliveryLeaseID: delivery.LeaseID}
	intent, err := database.ResolveGithubCheckDeliveryTarget(context.Background(), delivery)
	require.NoError(t, err)
	require.Equal(t, models.GithubDeliveryTargetLegacyCheckAction, intent.Source)
	require.Equal(t, batch.PrNumber, intent.PullRequestNumber)
	wrong := intent
	wrong.PullRequestNumber++
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), identity, wrong)
	require.ErrorIs(t, err, models.ErrGithubDeliveryTargetConflict)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubDeliveryTarget{}).Where("delivery_operation_id = ?", identity.DeliveryOperationID).Count(&count).Error)
	require.Zero(t, count)
	stored, created, err := database.RecordGithubDeliveryTarget(context.Background(), identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, database.GormDB.Model(batch).Update("branch_name", "changed-after-selection").Error)
	identity.WriterEpoch++
	identity.DeliveryLeaseID = "new-check-target-lease"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", identity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": identity.WriterEpoch, "lease_id": identity.DeliveryLeaseID}).Error)
	loaded, err := database.GetGithubDeliveryTarget(context.Background(), identity)
	require.NoError(t, err)
	require.Equal(t, stored.TargetSHA256, loaded.TargetSHA256)
	replayed, created, err := database.RecordGithubDeliveryTarget(context.Background(), identity, intent)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, stored.TargetSHA256, replayed.TargetSHA256)
}
