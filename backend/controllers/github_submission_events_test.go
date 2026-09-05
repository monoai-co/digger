package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func submissionEventGitFixture(t *testing.T) (string, string, string) {
	t.Helper()
	if err := exec.Command("git", "var", "GIT_AUTHOR_IDENT").Run(); err != nil {
		t.Skip("git fixture commits require a configured author")
	}
	directory := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = directory
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
		return strings.TrimSpace(string(output))
	}
	run("init", "-b", "main")
	commit := func(name string) string {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(directory, "digger.yml"), []byte("pr_locks: true\nprojects:\n- name: "+name+"\n  dir: .\n"), 0600))
		run("add", "digger.yml")
		run("commit", "-m", "Submission fixture "+name)
		return run("rev-parse", "HEAD")
	}
	base := commit("base")
	head := commit("root")
	return (&url.URL{Scheme: "file", Path: directory}).String(), base, head
}

func TestPostgresGithubEventSubmissionReplaysWithoutProviderWritesOrRepreparation(t *testing.T) {
	for _, eventType := range []string{"pull_request", "issue_comment"} {
		t.Run(eventType, func(t *testing.T) {
			database, _, previous := newDurableExecutionIntegrationDatabase(t)
			require.NoError(t, database.GormDB.AutoMigrate(&models.GithubSubmission{}, &models.GithubReportReceipt{}, &models.DiggerLock{}, &models.DetectionRun{}, &models.ImpactedProject{}))
			t.Setenv("DIGGER_FILENAME", "digger.yml")
			cloneURL, baseSHA, headSHA := submissionEventGitFixture(t)
			repository := &github.Repository{ID: github.Int64(12345), Name: github.String("sre"), FullName: github.String("monoai-co/sre"), Owner: &github.User{Login: github.String("monoai-co")}, DefaultBranch: github.String("main"), CloneURL: github.String(cloneURL)}
			pr := &github.PullRequest{Number: github.Int(42), State: github.String("open"), Base: &github.PullRequestBranch{SHA: github.String(baseSHA), Ref: github.String("main"), Repo: repository}, Head: &github.PullRequestBranch{SHA: github.String(headSHA), Ref: github.String("feature/submission"), Repo: repository}}
			sender := &github.User{Login: github.String("author"), Type: github.String("User")}
			installation := &github.Installation{ID: github.Int64(123)}
			var event any = &github.PullRequestEvent{Action: github.String("opened"), Repo: repository, PullRequest: pr, Sender: sender, Installation: installation}
			if eventType == "issue_comment" {
				event = &github.IssueCommentEvent{Action: github.String("created"), Repo: repository, Sender: sender, Installation: installation,
					Issue: &github.Issue{Number: github.Int(42), PullRequestLinks: &github.PullRequestLinks{URL: github.String("https://api.github.com/repos/monoai-co/sre/pulls/42")}}, Comment: &github.IssueComment{ID: github.Int64(99), Body: github.String("digger plan")}}
			}
			payload, err := json.Marshal(event)
			require.NoError(t, err)
			require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), previous.DeliveryID, previous.LeaseID, models.GithubWebhookDeliveryIgnored, "fixture_only", time.Now().UTC(), durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch))
			_, _, err = database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{DeliveryID: "runtime-submission", Payload: payload, PayloadSHA256: fmt.Sprintf("%x", sha256.Sum256(payload)), EventType: eventType, GithubAppID: 456, InstallationID: previous.InstallationID, RepositoryFullName: "monoai-co/sre"}, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
			require.NoError(t, err)
			delivery, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "runtime-submission", time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
			require.NoError(t, err)
			require.NotNil(t, delivery)
			client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method, "preparation must not write to GitHub")
				if r.URL.Path == "/repos/monoai-co/sre/pulls/42" {
					require.NoError(t, json.NewEncoder(w).Encode(pr))
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode([]any{}))
			})
			provider := &submissionConfigProvider{client: client}
			controller := DiggerController{GithubClientProvider: provider, ControlPlaneDatabaseIdentity: durableExecutionIntegrationDatabaseIdentity, ControlPlaneWriterEpoch: durableExecutionIntegrationWriterEpoch}
			_, err = controller.ProcessGithubWebhookDelivery(context.Background(), delivery)
			require.ErrorIs(t, err, errGithubSubmissionReportsPending)
			calls := provider.calls
			provider.client = nil
			_, err = controller.ProcessGithubWebhookDelivery(context.Background(), delivery)
			require.ErrorIs(t, err, errGithubSubmissionReportsPending)
			require.Equal(t, calls, provider.calls)
			var submission models.GithubSubmission
			require.NoError(t, database.GormDB.First(&submission).Error)
			intent, err := models.DecodeGithubSubmissionIntent(submission.Intent)
			require.NoError(t, err)
			require.NotNil(t, intent.Graph)
			require.Equal(t, headSHA, intent.Graph.CommitSHA)
			require.NotNil(t, intent.Locks)
			var count int64
			require.NoError(t, database.GormDB.Model(&models.DetectionRun{}).Count(&count).Error)
			require.EqualValues(t, 1, count)
			require.NoError(t, database.GormDB.Model(&models.DiggerLock{}).Count(&count).Error)
			require.EqualValues(t, 1, count)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Count(&count).Error)
			require.Zero(t, count)
			var effects []models.OutboxEffect
			require.NoError(t, database.GormDB.Where("operation_id = ?", delivery.OperationID).Find(&effects).Error)
			require.NotEmpty(t, effects)
			for index, effect := range effects {
				lease := fmt.Sprintf("runtime-report-%d", index)
				require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{"status": models.OutboxEffectProcessing, "lease_id": lease, "lease_expires_at": time.Now().UTC().Add(time.Minute)}).Error)
				prepared, err := database.PrepareGithubReportCreate(context.Background(), effect.ID, lease, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
				require.NoError(t, err)
				providerID := int64(100 + index)
				providerURL, err := models.GithubReportProviderURL(prepared.Payload, providerID)
				require.NoError(t, err)
				receipt, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256, ResourceKind: prepared.Payload.ResourceKind, ProviderID: providerID, ProviderURL: providerURL})
				require.NoError(t, err)
				require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, lease, receipt, time.Now().UTC(), durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch))
			}
			for attempt := 0; attempt < 2; attempt++ {
				_, err = controller.ProcessGithubWebhookDelivery(context.Background(), delivery)
				require.NoError(t, err)
			}
			require.Equal(t, calls, provider.calls)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Count(&count).Error)
			require.EqualValues(t, 1, count)
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("effect_kind = ?", models.GithubWorkflowDispatchEffectKind).Count(&count).Error)
			require.EqualValues(t, 1, count)
		})
	}
}
