package utils

import (
	"context"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPostgresLegacyGithubCheckActionScopesLookupAndRejectsAmbiguousBatch(t *testing.T) {
	database, _, _ := newPostgresDurableGraphTestDatabase(t)
	batch := models.DiggerBatch{ID: uuid.New(), DiggerBatchID: "legacy-batch", VCS: models.DiggerVCSGithub, PrNumber: 42,
		CommitSha: "original-head", BranchName: "feature/legacy", GithubInstallationId: 123, RepoOwner: "monoai-co", RepoName: "sre", RepoFullName: "monoai-co/sre", CheckRunId: github.String("900")}
	require.NoError(t, database.GormDB.Create(&batch).Error)
	summary := models.DiggerJobSummary{}
	require.NoError(t, database.GormDB.Create(&summary).Error)
	job := models.DiggerJob{DiggerJobID: "legacy-job", BatchID: github.String(batch.ID.String()), CheckRunId: github.String("901"), DiggerJobSummaryID: summary.ID}
	require.NoError(t, database.GormDB.Create(&job).Error)
	foreign := batch
	foreign.ID, foreign.GithubInstallationId = uuid.New(), 999
	require.NoError(t, database.GormDB.Create(&foreign).Error)
	event := &github.CheckRunEvent{Action: github.String("requested_action"), Installation: &github.Installation{ID: github.Int64(123)},
		Repo:            &github.Repository{ID: github.Int64(91), Name: github.String("sre"), FullName: github.String("monoai-co/sre"), Owner: &github.User{Login: github.String("monoai-co")}},
		CheckRun:        &github.CheckRun{ID: github.Int64(901), HeadSHA: github.String("original-head"), App: &github.App{ID: github.Int64(456)}},
		RequestedAction: &github.RequestedAction{Identifier: "abatch:legacy-batch"}}
	resolved, jobs, err := database.ResolveLegacyGithubCheckAction(context.Background(), event, 456)
	require.NoError(t, err)
	require.Equal(t, batch.ID, resolved.ID)
	require.Len(t, jobs, 1)
	require.Equal(t, job.DiggerJobID, jobs[0].DiggerJobID)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = database.ResolveLegacyGithubCheckAction(canceled, event, 456)
	require.ErrorIs(t, err, context.Canceled)
	duplicate := batch
	duplicate.ID = uuid.New()
	require.NoError(t, database.GormDB.Create(&duplicate).Error)
	_, _, err = database.ResolveLegacyGithubCheckAction(context.Background(), event, 456)
	require.ErrorIs(t, err, models.ErrGithubCheckActionBinding)
}
