package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestPostgresGithubReportLostResponseRecoversAcrossWriterHandoff(t *testing.T) {
	database, organisation, _ := newDurableExecutionIntegrationDatabase(t)
	// Apply the real permit/receipt migration, including identity triggers.
	require.NoError(t, database.GormDB.Migrator().DropTable(&models.GithubReportCreateAttempt{}))
	var schema string
	require.NoError(t, database.GormDB.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.True(t, strings.HasPrefix(schema, "durable_execution_integration_"))
	migration, err := os.ReadFile("../migrations/20260905049000_github_report_attempts.sql")
	require.NoError(t, err)
	statement := strings.ReplaceAll(string(migration), `"public"`, `"`+schema+`"`)
	statement = strings.ReplaceAll(statement, "public.", schema+".")
	require.NoError(t, database.GormDB.Transaction(func(tx *gorm.DB) error { return tx.Exec(statement).Error }))
	installationID := int64(123)
	rawDelivery := []byte(`{"action":"opened"}`)
	digest := sha256.Sum256(rawDelivery)
	delivery, _, err := database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
		DeliveryID: "report-integration", Payload: rawDelivery, PayloadSHA256: hex.EncodeToString(digest[:]),
		EventType: "pull_request", GithubAppID: 456, InstallationID: &installationID, RepositoryFullName: "monoai-co/sre",
	}, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	payload := models.GithubReportCreatePayload{OrganisationID: organisation.ID, GithubAppID: 456,
		GithubInstallationID: installationID, RepoOwner: "monoai-co", RepoName: "sre", PullRequestNumber: 42,
		ResourceKind: models.GithubReportResourceComment, Body: "Report prepared"}
	raw, err := models.CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	effect := models.NewOutboxEffect(delivery.OperationID, models.GithubReportCreateEffectKind, "summary", raw, durableExecutionIntegrationWriterEpoch, time.Now().UTC())
	_, _, err = database.EnqueueOutboxEffect(context.Background(), effect, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "initial-report-lease", time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	posts, reads := 0, 0
	body := ""
	client, server := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/repos/monoai-co/sre/issues/42/comments":
			posts++
			var posted github.IssueComment
			if json.NewDecoder(r.Body).Decode(&posted) != nil {
				http.Error(w, "bad body", 400)
				return
			}
			body = posted.GetBody()
			json.NewEncoder(w).Encode(map[string]any{"id": 321, "body": body})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/monoai-co/sre/issues/42/comments":
			reads++
			json.NewEncoder(w).Encode([]any{map[string]any{"id": 321, "body": body, "user": map[string]any{"type": "Bot", "login": "digger[bot]"}}})
		case r.Method == http.MethodGet && r.URL.Path == "/apps/digger":
			json.NewEncoder(w).Encode(map[string]any{"id": 456, "slug": "digger"})
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	})
	base := client.BaseURL
	transport := server.Client().Transport
	client = github.NewClient(&http.Client{Transport: githubWorkflowDispatchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && r.Method == http.MethodPost {
			response.Body.Close()
			return nil, errors.New("lost successful provider response")
		}
		return response, err
	})})
	client.BaseURL = base
	dispatch, err := NewGithubWorkflowOutboxDispatch(database, reportTestProvider{client: client}, time.Hour)
	require.NoError(t, err)
	request := OutboxDispatchRequest{EffectID: claimed.ID, OperationID: claimed.ControlOperationID, EffectKind: claimed.EffectKind,
		EffectKey: claimed.EffectKey, Payload: claimed.Payload, LeaseID: claimed.LeaseID,
		DatabaseIdentity: durableExecutionIntegrationDatabaseIdentity, WriterEpoch: durableExecutionIntegrationWriterEpoch}
	first, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, time.Minute, first.RetryAfter)
	require.Empty(t, first.ProviderReceipt)
	require.NoError(t, database.RetryOutboxEffect(context.Background(), claimed.ID, claimed.LeaseID, "", 0, time.Now().UTC(), request.DatabaseIdentity, request.WriterEpoch))
	request.WriterEpoch++
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", request.WriterEpoch).Error)
	claimed, err = database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "new-writer-report-lease", time.Minute, request.DatabaseIdentity, request.WriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	request.LeaseID = claimed.LeaseID
	second, err := dispatch(context.Background(), request)
	require.NoError(t, err)
	require.Zero(t, second.RetryAfter)
	require.NotEmpty(t, second.ProviderReceipt)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), claimed.ID, claimed.LeaseID, second.ProviderReceipt, time.Now().UTC(), request.DatabaseIdentity, request.WriterEpoch))
	var stored models.OutboxEffect
	var receipt models.GithubReportReceipt
	var attempt models.GithubReportCreateAttempt
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.NoError(t, database.GormDB.First(&receipt, "effect_id = ?", effect.ID).Error)
	require.NoError(t, database.GormDB.First(&attempt, "effect_id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectSucceeded, stored.Status)
	require.Equal(t, int64(321), receipt.ProviderID)
	require.Equal(t, durableExecutionIntegrationWriterEpoch, attempt.WriterEpoch)
	require.Equal(t, int64(2), stored.AttemptCount)
	require.Equal(t, 1, posts)
	require.Equal(t, 1, reads)
}
