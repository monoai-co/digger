package utils

import (
	"context"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/stretchr/testify/require"
)

func TestPostgresGithubSubmissionDetectionCommitsOnceWithLocks(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.DetectionRun{}, &models.ImpactedProject{}, &models.DiggerLock{}))
	intent.Detection = &models.GithubSubmissionDetection{DefaultBranch: "main", ChangedFiles: []string{"root-one/main.tf"}, Projects: []configuration.Project{{Name: "root-one", Dir: "root-one"}, {Name: "root-two", Dir: "root-two"}}}
	intent.Locks = &models.GithubSubmissionLocks{Acquire: []string{"root-one"}}
	_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, database.GormDB.Model(&models.ImpactedProject{}).Where("project_name = ?", "root-one").Update("planned", true).Error)
	_, created, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.False(t, created)
	var runs []models.DetectionRun
	require.NoError(t, database.GormDB.Find(&runs).Error)
	require.Len(t, runs, 1)
	require.Equal(t, request.OrganisationID, runs[0].OrganisationID)
	require.Equal(t, request.CommitSHA, runs[0].CommitSHA)
	var projects []models.ImpactedProject
	require.NoError(t, database.GormDB.Order("project_name").Find(&projects).Error)
	require.Len(t, projects, 2)
	require.True(t, projects[0].Planned, "replay must not reset project progress")
}

func TestPostgresGithubSubmissionDetectionFailureRollsBackLocksAndReceipt(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.DiggerLock{}))
	// The missing detection table is a failure after lock acquisition. Both the
	// new lock and submission must disappear when the transaction rolls back.
	require.NoError(t, database.GormDB.Migrator().DropTable(&models.DetectionRun{}))
	intent.Detection = &models.GithubSubmissionDetection{DefaultBranch: "main", Projects: []configuration.Project{{Name: "root-one"}}}
	intent.Locks = &models.GithubSubmissionLocks{Acquire: []string{"root-one"}}
	_, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.Error(t, err)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.DiggerLock{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Count(&count).Error)
	require.Zero(t, count)
}
