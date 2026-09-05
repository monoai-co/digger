package utils

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
)

func TestGithubSubmissionReportsUseFrozenOrderAndTime(t *testing.T) {
	_, org, delivery := newDurableGraphTestDatabase(t)
	graph, err := PrepareDurableGraphIntent(durableGraphTestRequest(t, org, delivery))
	require.NoError(t, err)
	intent := models.GithubSubmissionIntent{Graph: graph, Sources: []models.GithubSubmissionSource{{Location: "modules/network", Projects: []string{"root-two", "root-one"}}}}
	selectedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	prepared, err := PrepareGithubSubmissionWithReports(intent, 456, selectedAt)
	require.NoError(t, err)
	require.Empty(t, intent.Reports)
	require.Len(t, prepared.Reports, 7)
	byKey := make(map[string]models.GithubReportCreatePayload)
	for _, report := range prepared.Reports {
		payload, err := models.DecodeGithubReportCreatePayload(report.Payload)
		require.NoError(t, err)
		byKey[report.Key] = payload
		require.Equal(t, report.Key == "initial:check:4", report.Optional)
		if report.Key == "initial:check:1" {
			require.Equal(t, models.GithubSubmissionReportProject, report.Role)
			require.Equal(t, "root-one", report.ProjectName)
			require.Equal(t, 2, report.Order)
		}
		if report.Key == "initial:source:0" {
			require.Equal(t, models.GithubSubmissionReportSource, report.Role)
			require.Equal(t, "modules/network", report.SourceLocation)
		}
	}
	require.Equal(t, "child/plan", byKey["initial:check:0"].Check.Name)
	require.Equal(t, "root-one/plan", byKey["initial:check:1"].Check.Name)
	require.Equal(t, "root-two/plan", byKey["initial:check:2"].Check.Name)
	require.Equal(t, "digger/plan", byKey["initial:check:3"].Check.Name)
	require.Equal(t, "digger/apply", byKey["initial:check:4"].Check.Name)
	require.Equal(t, "| Project | Status |\n|---------|--------|\n|:clock11: **child**|pending...|\n|:clock11: **root-one**|pending...|\n|:clock11: **root-two**|pending...|\n", byKey["initial:summary"].Body)
	require.Equal(t, "<details ><summary>Report for location: modules/network 2026-09-05 01:02:03 (UTC)</summary>\n  \n</details>", byKey["initial:source:0"].Body)
	graph.Jobs[0], graph.Jobs[2] = graph.Jobs[2], graph.Jobs[0]
	intent.Graph = graph
	again, err := PrepareGithubSubmissionWithReports(intent, 456, selectedAt.In(time.FixedZone("other", 3600)))
	require.NoError(t, err)
	firstRaw, err := json.Marshal(prepared)
	require.NoError(t, err)
	secondRaw, err := json.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, firstRaw, secondRaw)
	_, err = PrepareGithubSubmissionWithReports(prepared, 456, selectedAt)
	require.ErrorIs(t, err, models.ErrGithubSubmissionIntent)
}

func TestGithubSubmissionReportsRejectInvalidBindings(t *testing.T) {
	_, org, delivery := newDurableGraphTestDatabase(t)
	graph, err := PrepareDurableGraphIntent(durableGraphTestRequest(t, org, delivery))
	require.NoError(t, err)
	prepared, err := PrepareGithubSubmissionWithReports(models.GithubSubmissionIntent{Graph: graph}, 456, time.Now().UTC())
	require.NoError(t, err)
	raw, err := json.Marshal(prepared)
	require.NoError(t, err)
	for name, mutate := range map[string]func(*models.GithubSubmissionIntent){
		"role":             func(i *models.GithubSubmissionIntent) { i.Reports[0].Role = "unsupported" },
		"project":          func(i *models.GithubSubmissionIntent) { i.Reports[0].ProjectName = "missing-project" },
		"source":           func(i *models.GithubSubmissionIntent) { i.Reports[0].SourceLocation = "unbound-source" },
		"optional project": func(i *models.GithubSubmissionIntent) { i.Reports[0].Optional = true },
		"negative order":   func(i *models.GithubSubmissionIntent) { i.Reports[0].Order = -1 },
		"duplicate order":  func(i *models.GithubSubmissionIntent) { i.Reports[1].Order = i.Reports[0].Order },
	} {
		var invalid models.GithubSubmissionIntent
		require.NoError(t, json.Unmarshal(raw, &invalid))
		mutate(&invalid)
		encoded, err := json.Marshal(invalid)
		require.NoError(t, err)
		_, err = models.DecodeGithubSubmissionIntent(encoded)
		require.ErrorIs(t, err, models.ErrGithubSubmissionIntent, name)
	}
}

func TestPostgresGithubSubmissionFreezesReportsAcrossReplay(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	prepared, err := PrepareGithubSubmissionWithReports(intent, 456, time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
	require.NoError(t, err)
	stored, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, prepared)
	require.NoError(t, err)
	require.True(t, created)
	loaded, err := database.GetGithubSubmission(context.Background(), request.Identity)
	require.NoError(t, err)
	require.Equal(t, stored.IntentSHA256, loaded.IntentSHA256)
	decoded, err := models.DecodeGithubSubmissionIntent(loaded.Intent)
	require.NoError(t, err)
	require.Equal(t, prepared.Reports, decoded.Reports)
	decoded.Reports[0].Payload = []byte(strings.Replace(string(decoded.Reports[0].Payload), "Waiting for plan...", "New title", 1))
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, decoded)
	require.ErrorIs(t, err, models.ErrGithubSubmissionConflict)
	decoded.Reports[0].Payload = []byte(strings.Replace(string(decoded.Reports[0].Payload), `"github_app_id":456`, `"github_app_id":999`, 1))
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, decoded)
	require.ErrorIs(t, err, models.ErrGithubSubmissionTenant)
}

func TestGithubSubmissionReportsRejectForeignTargetsAndDuplicateKeys(t *testing.T) {
	_, org, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, org, delivery)
	request.JobReporterType = "noop"
	graph, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	prepared, err := PrepareGithubSubmissionWithReports(models.GithubSubmissionIntent{Graph: graph}, 456, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, prepared.Reports, 5, "noop reporter still creates checks, but no summary comment")
	valid, err := json.Marshal(prepared)
	require.NoError(t, err)
	for _, mutation := range []string{"duplicate", "head", "pull request", "organisation", "installation", "repository"} {
		var invalid models.GithubSubmissionIntent
		require.NoError(t, json.Unmarshal(valid, &invalid))
		if mutation == "duplicate" {
			invalid.Reports = append(invalid.Reports, invalid.Reports[0])
		} else {
			payload, err := models.DecodeGithubReportCreatePayload(invalid.Reports[0].Payload)
			require.NoError(t, err)
			switch mutation {
			case "head":
				payload.HeadSHA = "different-head"
			case "pull request":
				payload.PullRequestNumber++
			case "organisation":
				payload.OrganisationID++
			case "installation":
				payload.GithubInstallationID++
			case "repository":
				payload.RepoName = "other"
			}
			invalid.Reports[0].Payload, err = models.CanonicalGithubReportCreatePayload(payload)
			require.NoError(t, err)
		}
		raw, err := json.Marshal(invalid)
		require.NoError(t, err)
		_, err = models.DecodeGithubSubmissionIntent(raw)
		require.ErrorIs(t, err, models.ErrGithubSubmissionIntent, mutation)
	}
}

func TestPostgresGithubSubmissionReportsEnqueueAtomicReplay(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	prepared, err := PrepareGithubSubmissionWithReports(intent, 456, time.Now().UTC())
	require.NoError(t, err)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, prepared)
	require.NoError(t, err)
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	results := make(chan []*models.OutboxEffect, 8)
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			effects, err := database.EnqueueGithubSubmissionReports(context.Background(), request.Identity)
			failures <- err
			results <- effects
		}()
	}
	workers.Wait()
	close(failures)
	close(results)
	for err := range failures {
		require.NoError(t, err)
	}
	var first []*models.OutboxEffect
	for effects := range results {
		require.Len(t, effects, len(prepared.Reports))
		if first == nil {
			first = effects
		}
		for index, effect := range effects {
			require.Equal(t, first[index].ID, effect.ID)
			require.Equal(t, prepared.Reports[index].Key, effect.EffectKey)
			require.True(t, effect.ValidPayloadDigest())
		}
	}
	var count int64
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Count(&count).Error)
	require.EqualValues(t, len(prepared.Reports), count)
	identity := request.Identity
	identity.WriterEpoch++
	identity.DeliveryLeaseID = "report-manifest-handoff"
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", identity.WriterEpoch).Error)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("operation_id = ?", identity.DeliveryOperationID).Updates(map[string]any{"writer_epoch": identity.WriterEpoch, "lease_id": identity.DeliveryLeaseID}).Error)
	replayed, err := database.EnqueueGithubSubmissionReports(context.Background(), identity)
	require.NoError(t, err)
	for index, effect := range replayed {
		require.Equal(t, first[index].ID, effect.ID)
	}
}

func TestPostgresGithubSubmissionReportsConflictRollsBackEntireManifest(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	prepared, err := PrepareGithubSubmissionWithReports(intent, 456, time.Now().UTC())
	require.NoError(t, err)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, prepared)
	require.NoError(t, err)
	last := prepared.Reports[len(prepared.Reports)-1]
	payload, err := models.DecodeGithubReportCreatePayload(last.Payload)
	require.NoError(t, err)
	payload.Body = "Conflicting initial summary"
	raw, err := models.CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	conflicting := models.NewOutboxEffect(request.Identity.DeliveryOperationID, models.GithubReportCreateEffectKind, last.Key, raw, request.Identity.WriterEpoch, time.Now().UTC())
	_, _, err = database.EnqueueOutboxEffect(context.Background(), conflicting, request.Identity.DatabaseIdentity, request.Identity.WriterEpoch)
	require.NoError(t, err)
	effects, err := database.EnqueueGithubSubmissionReports(context.Background(), request.Identity)
	require.ErrorIs(t, err, models.ErrOutboxEffectConflict)
	require.Nil(t, effects)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Count(&count).Error)
	require.EqualValues(t, 1, count, "earlier inserts in the manifest must roll back")
}
