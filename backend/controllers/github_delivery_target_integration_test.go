package controllers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func githubDeliveryTargetDatabase(t *testing.T) (*models.Database, models.JobCreationIdentity, *models.GithubWebhookDelivery) {
	t.Helper()
	database, _, previous := newDurableExecutionIntegrationDatabase(t)
	require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), previous.DeliveryID, previous.LeaseID, models.GithubWebhookDeliveryIgnored, "fixture_only", time.Now().UTC(), durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch))
	var schema string
	require.NoError(t, database.GormDB.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.True(t, strings.HasPrefix(schema, "durable_execution_integration_"))
	migration, err := os.ReadFile("../migrations/20260905050000_github_delivery_targets.sql")
	require.NoError(t, err)
	statement := strings.ReplaceAll(string(migration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, database.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
	_, _, err = database.RecordGithubWebhookDelivery(context.Background(), targetResolutionComment(t), durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	delivery, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "target-lease", time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	return database, models.JobCreationIdentity{DeliveryOperationID: delivery.OperationID, DeliveryLeaseID: delivery.LeaseID,
		DatabaseIdentity: durableExecutionIntegrationDatabaseIdentity, WriterEpoch: durableExecutionIntegrationWriterEpoch, ProtocolVersion: operation.ProtocolVersion}, delivery
}

func TestPostgresGithubDeliveryTargetReplayKeepsFirstHeadAcrossWriterHandoff(t *testing.T) {
	database, identity, delivery := githubDeliveryTargetDatabase(t)
	requests := 0
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		pr := targetResolutionPR()
		pr.Base.SHA, pr.Base.Ref = github.String("original-base"), github.String("main")
		if requests > 1 {
			pr.Head.SHA = github.String("newer-head")
			pr.Base.SHA = github.String("newer-base")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pr)
	})
	first, err := prepareGithubDeliveryTarget(context.Background(), identity, delivery, database, &targetResolutionProvider{client: client})
	require.NoError(t, err)
	identity.WriterEpoch++
	identity.DeliveryLeaseID = "new-target-lease"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", identity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": identity.WriterEpoch, "lease_id": identity.DeliveryLeaseID}).Error)
	second, err := prepareGithubDeliveryTarget(context.Background(), identity, delivery, database, nil)
	require.NoError(t, err)
	require.Equal(t, first.TargetSHA256, second.TargetSHA256)
	require.True(t, first.CreatedAt.Equal(second.CreatedAt))
	require.Equal(t, durableExecutionIntegrationWriterEpoch, second.WriterEpoch)
	intent, err := models.DecodeGithubDeliveryTarget(second.Target)
	require.NoError(t, err)
	require.Equal(t, "original-commit", intent.HeadSHA)
	require.Equal(t, "original-base", intent.BaseSHA)
	require.Equal(t, 1, requests)
	intent.HeadSHA = "newer-head"
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), identity, intent)
	require.ErrorIs(t, err, models.ErrGithubDeliveryTargetConflict)
}

func TestPostgresGithubDeliveryTargetConcurrentRecordAndImmutableHistory(t *testing.T) {
	database, identity, delivery := githubDeliveryTargetDatabase(t)
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	intent, err := preparation.Resolve(targetResolutionPR())
	require.NoError(t, err)
	var workers sync.WaitGroup
	var created atomic.Int32
	failures := make(chan error, 8)
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, fresh, err := database.RecordGithubDeliveryTarget(context.Background(), identity, intent)
			if fresh {
				created.Add(1)
			}
			failures <- err
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), created.Load())
	require.Error(t, database.GormDB.Model(&models.GithubDeliveryTarget{}).Where("delivery_operation_id = ?", identity.DeliveryOperationID).Update("writer_epoch", 999).Error)
	require.Error(t, database.GormDB.Where("delivery_operation_id = ?", identity.DeliveryOperationID).Delete(&models.GithubDeliveryTarget{}).Error)
	require.Error(t, database.GormDB.Exec("TRUNCATE github_delivery_targets").Error)
	_, err = database.GetGithubDeliveryTarget(context.Background(), identity)
	require.NoError(t, err)
}

func TestPostgresGithubDeliveryTargetRejectsChangedScopeAndExpiredLease(t *testing.T) {
	database, identity, delivery := githubDeliveryTargetDatabase(t)
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	intent, err := preparation.Resolve(targetResolutionPR())
	require.NoError(t, err)
	wrongPR := intent
	wrongPR.PullRequestNumber++
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), identity, wrongPR)
	require.ErrorIs(t, err, models.ErrGithubDeliveryTargetIntent)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubDeliveryTarget{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Update("lease_expires_at", time.Now().Add(-time.Second)).Error)
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), identity, intent)
	require.ErrorIs(t, err, models.ErrGithubSubmissionClaim)
	_, err = database.GetGithubDeliveryTarget(context.Background(), identity)
	require.ErrorIs(t, err, models.ErrGithubSubmissionClaim)
}

func TestPostgresGithubDeliveryTargetCorruptImportDoesNotResolveAgain(t *testing.T) {
	database, identity, delivery := githubDeliveryTargetDatabase(t)
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	intent, err := preparation.Resolve(targetResolutionPR())
	require.NoError(t, err)
	raw, err := json.Marshal(intent)
	require.NoError(t, err)
	var organisation models.Organisation
	require.NoError(t, database.GormDB.First(&organisation).Error)
	require.NoError(t, database.GormDB.Create(&models.GithubDeliveryTarget{DeliveryOperationID: identity.DeliveryOperationID,
		OrganisationID: organisation.ID, Target: raw, TargetSHA256: strings.Repeat("0", 64), DeliveryPayloadSHA256: delivery.PayloadSHA256,
		WriterEpoch: identity.WriterEpoch, CreatedAt: time.Now().UTC()}).Error)
	provider := &targetResolutionProvider{}
	_, err = prepareGithubDeliveryTarget(context.Background(), identity, delivery, database, provider)
	require.ErrorIs(t, err, models.ErrGithubDeliveryTargetConflict)
	require.Zero(t, provider.calls)
}
