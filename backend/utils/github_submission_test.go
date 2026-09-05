package utils

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newGithubSubmissionFixture(t *testing.T) (*models.Database, DurableJobGraphRequest, models.GithubSubmissionIntent) {
	t.Helper()
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	var schema string
	require.NoError(t, database.GormDB.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.True(t, strings.HasPrefix(schema, "durable_graph_test_"))
	migration, err := os.ReadFile("../migrations/20260905048000_github_submissions.sql")
	require.NoError(t, err)
	statement := strings.ReplaceAll(string(migration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, database.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
	request := durableGraphTestRequest(t, organisation, delivery)
	targetMigration, err := os.ReadFile("../migrations/20260905050000_github_delivery_targets.sql")
	require.NoError(t, err)
	statement = strings.ReplaceAll(string(targetMigration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, database.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
	repository := &github.Repository{ID: github.Int64(12345), Name: github.String(request.RepoName), FullName: github.String(request.RepoFullName), Owner: &github.User{Login: github.String(request.RepoOwner)}}
	event := github.PullRequestEvent{Repo: repository, Installation: &github.Installation{ID: delivery.InstallationID}, PullRequest: &github.PullRequest{
		Number: github.Int(request.PullRequestNumber), Base: &github.PullRequestBranch{Repo: repository}, Head: &github.PullRequestBranch{SHA: github.String(request.CommitSHA), Ref: github.String(request.Branch)},
	}}
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), delivery.DeliveryID, delivery.LeaseID, models.GithubWebhookDeliveryIgnored, "fixture_only", time.Now().UTC(), request.Identity.DatabaseIdentity, request.Identity.WriterEpoch))
	_, _, err = database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
		DeliveryID: "submission-delivery", Payload: payload, PayloadSHA256: fmt.Sprintf("%x", sha256.Sum256(payload)), EventType: "pull_request",
		GithubAppID: delivery.GithubAppID, InstallationID: delivery.InstallationID, RepositoryFullName: delivery.RepositoryFullName,
	}, request.Identity.DatabaseIdentity, request.Identity.WriterEpoch)
	require.NoError(t, err)
	delivery, err = database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "submission-lease", time.Minute, request.Identity.DatabaseIdentity, request.Identity.WriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	request = durableGraphTestRequest(t, organisation, delivery)
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	target, err := preparation.Resolve(nil)
	require.NoError(t, err)
	_, _, err = database.RecordGithubDeliveryTarget(context.Background(), request.Identity, target)
	require.NoError(t, err)
	graph, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	return database, request, models.GithubSubmissionIntent{Graph: graph, Sources: []models.GithubSubmissionSource{{Location: "modules/network", Projects: []string{"root-two", "root-one"}}}}
}

func TestPostgresGithubSubmissionReloadsFrozenIntentAndBuildsSameGraph(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	first, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, request.Identity.WriterEpoch, first.WriterEpoch)
	intent.Sources[0].Projects[0], intent.Sources[0].Projects[1] = intent.Sources[0].Projects[1], intent.Sources[0].Projects[0]
	replayed, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.IntentSHA256, replayed.IntentSHA256)
	require.Equal(t, first.CreatedAt, replayed.CreatedAt)
	intent.Graph.CommitSHA = "changed-after-preparation"
	loaded, err := database.GetGithubSubmission(context.Background(), request.Identity)
	require.NoError(t, err)
	decoded, err := models.DecodeGithubSubmissionIntent(loaded.Intent)
	require.NoError(t, err)
	require.Equal(t, request.CommitSHA, decoded.Graph.CommitSHA)
	batchID, _, err := CreateDurableGraphFromIntent(context.Background(), request.Identity, *decoded.Graph)
	require.NoError(t, err)
	replayBatchID, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, batchID, replayBatchID)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.Error(t, err)
}

func TestPostgresGithubSubmissionConcurrentPreparationCreatesOneReceipt(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	var createdCount atomic.Int32
	var workers sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
			if created {
				createdCount.Add(1)
			}
			errors <- err
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, createdCount.Load())
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestPostgresGithubSubmissionRejectsDifferentSelectedTarget(t *testing.T) {
	for _, field := range []string{"pull request", "commit", "branch"} {
		t.Run(field, func(t *testing.T) {
			database, request, intent := newGithubSubmissionFixture(t)
			changed := request
			switch field {
			case "pull request":
				changed.PullRequestNumber++
				changed.Jobs = maps.Clone(request.Jobs)
				for name, job := range changed.Jobs {
					job.PullRequestNumber = &changed.PullRequestNumber
					changed.Jobs[name] = job
				}
			case "commit":
				changed.CommitSHA = "different-commit"
			case "branch":
				changed.Branch = "different-branch"
			}
			graph, err := PrepareDurableGraphIntent(changed)
			require.NoError(t, err)
			invalid := intent
			invalid.Graph = graph
			_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, invalid)
			require.ErrorIs(t, err, models.ErrGithubDeliveryTargetConflict)
			var count int64
			require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Count(&count).Error)
			require.Zero(t, count)
			_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
			require.NoError(t, err)
			require.True(t, created)
		})
	}
}

func TestPostgresGithubSubmissionRejectsChangedTenantAndDeliveryPayload(t *testing.T) {
	for _, mutation := range []string{"repository", "organisation", "delivery payload", "delivery protocol"} {
		t.Run(mutation, func(t *testing.T) {
			database, request, intent := newGithubSubmissionFixture(t)
			_, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
			require.NoError(t, err)
			switch mutation {
			case "repository":
				require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", request.Identity.DeliveryOperationID).Update("repository_full_name", "another/repository").Error)
			case "organisation":
				require.NoError(t, database.GormDB.Model(&models.GithubAppInstallationLink{}).Where("github_installation_id = ?", intent.Graph.GithubInstallationID).Update("status", models.GithubAppInstallationLinkInactive).Error)
			case "delivery payload":
				require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", request.Identity.DeliveryOperationID).Update("payload", []byte(`{"action":"changed"}`)).Error)
			case "delivery protocol":
				require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", request.Identity.DeliveryOperationID).Update("protocol_version", request.Identity.ProtocolVersion+1).Error)
			}
			_, err = database.GetGithubSubmission(context.Background(), request.Identity)
			require.Error(t, err)
			_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
			require.Error(t, err)
		})
	}
}

func TestPostgresGithubSubmissionEpochHandoffPreservesOriginalReceipt(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	first, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	newIdentity := request.Identity
	newIdentity.WriterEpoch++
	newIdentity.DeliveryLeaseID = "submission-new-writer-lease"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", newIdentity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", newIdentity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": newIdentity.WriterEpoch, "lease_id": newIdentity.DeliveryLeaseID}).Error)
	_, err = database.GetGithubSubmission(context.Background(), request.Identity)
	require.Error(t, err)
	loaded, err := database.GetGithubSubmission(context.Background(), newIdentity)
	require.NoError(t, err)
	require.Equal(t, first.WriterEpoch, loaded.WriterEpoch)
	require.Equal(t, first.IntentSHA256, loaded.IntentSHA256)
	_, created, err := database.RecordGithubSubmission(context.Background(), newIdentity, intent)
	require.NoError(t, err)
	require.False(t, created)
}

func TestPostgresGithubSubmissionRejectsCorruptStoredIntent(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	_, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	// Simulate corrupted imported history in this isolated test schema only.
	require.NoError(t, database.GormDB.Exec("ALTER TABLE github_submissions DISABLE TRIGGER github_submission_history_rows").Error)
	require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Where("delivery_operation_id = ?", request.Identity.DeliveryOperationID).Update("intent_sha256", strings.Repeat("0", 64)).Error)
	require.NoError(t, database.GormDB.Exec("ALTER TABLE github_submissions ENABLE TRIGGER github_submission_history_rows").Error)
	_, err = database.GetGithubSubmission(context.Background(), request.Identity)
	require.ErrorIs(t, err, models.ErrGithubSubmissionConflict)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.ErrorIs(t, err, models.ErrGithubSubmissionConflict)
}

func TestPostgresGithubSubmissionRejectsExpiredLeaseAndImmutableHistoryChanges(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	_, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.Error(t, database.GormDB.Model(&models.GithubSubmission{}).Where("delivery_operation_id = ?", request.Identity.DeliveryOperationID).Update("writer_epoch", 99).Error)
	require.Error(t, database.GormDB.Where("delivery_operation_id = ?", request.Identity.DeliveryOperationID).Delete(&models.GithubSubmission{}).Error)
	foreign := request.Identity
	foreign.DeliveryLeaseID = "another-worker"
	_, err = database.GetGithubSubmission(context.Background(), foreign)
	require.Error(t, err)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", request.Identity.DeliveryOperationID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error)
	_, err = database.GetGithubSubmission(context.Background(), request.Identity)
	require.Error(t, err)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.Error(t, err)
}

func TestGithubSubmissionDecoderRejectsUnknownFieldsAndSourceAmbiguity(t *testing.T) {
	_, organisation, delivery := newDurableGraphTestDatabase(t)
	graph, err := PrepareDurableGraphIntent(durableGraphTestRequest(t, organisation, delivery))
	require.NoError(t, err)
	intent := models.GithubSubmissionIntent{Graph: graph, Sources: []models.GithubSubmissionSource{{Location: "modules/network", Projects: []string{"root-one"}}}}
	encoded, err := json.Marshal(intent)
	require.NoError(t, err)
	_, err = models.DecodeGithubSubmissionIntent(append(encoded, []byte(` {}`)...))
	require.Error(t, err)
	unknown := strings.Replace(string(encoded), `"graph":`, `"unsupported":true,"graph":`, 1)
	_, err = models.DecodeGithubSubmissionIntent([]byte(unknown))
	require.Error(t, err)
	intent.Sources[0].Projects = []string{"missing-project"}
	encoded, err = json.Marshal(intent)
	require.NoError(t, err)
	_, err = models.DecodeGithubSubmissionIntent(encoded)
	require.Error(t, err)
}

func TestPostgresGithubSubmissionRejectsInvalidGraphBeforeImmutableInsert(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*models.GithubSubmissionIntent)
	}{
		{"cycle", func(i *models.GithubSubmissionIntent) {
			i.Graph.Jobs[0].Parents = []string{i.Graph.Jobs[1].ProjectName}
			i.Graph.Jobs[1].Parents = []string{i.Graph.Jobs[0].ProjectName}
		}},
		{"missing parent", func(i *models.GithubSubmissionIntent) { i.Graph.Jobs[0].Parents = []string{"missing-project"} }},
		{"self parent", func(i *models.GithubSubmissionIntent) {
			i.Graph.Jobs[0].Parents = []string{i.Graph.Jobs[0].ProjectName}
		}},
		{"duplicate parent", func(i *models.GithubSubmissionIntent) {
			i.Graph.Jobs[0].Parents = []string{i.Graph.Jobs[1].ProjectName, i.Graph.Jobs[1].ProjectName}
		}},
		{"wrong operation", func(i *models.GithubSubmissionIntent) {
			i.Graph.Jobs[0].OperationID, i.Graph.Jobs[1].OperationID = i.Graph.Jobs[1].OperationID, i.Graph.Jobs[0].OperationID
		}},
		{"blank workflow", func(i *models.GithubSubmissionIntent) { i.Graph.Jobs[0].WorkflowFile = "" }},
		{"blank reporter", func(i *models.GithubSubmissionIntent) { i.Graph.JobReporterType = "" }},
		{"unsupported command", func(i *models.GithubSubmissionIntent) { i.Graph.JobType = "unsupported" }},
		{"unpaired check", func(i *models.GithubSubmissionIntent) {
			id := "123"
			i.Graph.Jobs[0].CheckRunID = &id
			i.Graph.Jobs[0].CheckRunURL = nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			database, request, valid := newGithubSubmissionFixture(t)
			raw, err := json.Marshal(valid)
			require.NoError(t, err)
			var invalid models.GithubSubmissionIntent
			require.NoError(t, json.Unmarshal(raw, &invalid))
			tc.mutate(&invalid)
			_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, invalid)
			require.Error(t, err)
			var count int64
			require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Count(&count).Error)
			require.Zero(t, count)
			_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, valid)
			require.NoError(t, err)
			require.True(t, created)
		})
	}
}
