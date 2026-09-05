package utils

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
)

func TestGithubSubmissionReportsUseFrozenOrderAndTime(t *testing.T) {
	_, org, delivery := newDurableGraphTestDatabase(t)
	graph, err := PrepareDurableGraphIntent(durableGraphTestRequest(t, org, delivery))
	require.NoError(t, err)
	intent := models.GithubSubmissionIntent{Graph: *graph, Sources: []models.GithubSubmissionSource{{Location: "modules/network", Projects: []string{"root-two", "root-one"}}}}
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
	}
	require.Equal(t, "child/plan", byKey["initial:check:0"].Check.Name)
	require.Equal(t, "root-one/plan", byKey["initial:check:1"].Check.Name)
	require.Equal(t, "root-two/plan", byKey["initial:check:2"].Check.Name)
	require.Equal(t, "digger/plan", byKey["initial:check:3"].Check.Name)
	require.Equal(t, "digger/apply", byKey["initial:check:4"].Check.Name)
	require.Equal(t, "| Project | Status |\n|---------|--------|\n|:clock11: **child**|pending...|\n|:clock11: **root-one**|pending...|\n|:clock11: **root-two**|pending...|\n", byKey["initial:summary"].Body)
	require.Equal(t, "<details ><summary>Report for location: modules/network 2026-09-05 01:02:03 (UTC)</summary>\n  \n</details>", byKey["initial:source:0"].Body)
	graph.Jobs[0], graph.Jobs[2] = graph.Jobs[2], graph.Jobs[0]
	intent.Graph = *graph
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
	prepared, err := PrepareGithubSubmissionWithReports(models.GithubSubmissionIntent{Graph: *graph}, 456, time.Now().UTC())
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
