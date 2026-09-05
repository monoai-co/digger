package utils

import (
	"context"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
)

func TestPostgresGithubSubmissionClosedLockReplacementComparesExactRows(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.DiggerLock{}))
	old := models.DiggerLock{OrganisationID: request.OrganisationID, Resource: request.RepoFullName + "#root-one", LockId: request.PullRequestNumber + 1}
	require.NoError(t, database.GormDB.Create(&old).Error)
	owners, err := database.ReadGithubSubmissionLockOwners(context.Background(), request.Identity, []string{"root-one"})
	require.NoError(t, err)
	require.Equal(t, []models.GithubSubmissionLockOwner{{ID: old.ID, Project: "root-one", PullRequestNumber: old.LockId}}, owners)
	intent.Locks = &models.GithubSubmissionLocks{Acquire: []string{"root-one"}, ClosedOwners: owners}
	// A different row appeared after the provider observation, even though its
	// project and owning PR are identical. The old observation cannot release it.
	require.NoError(t, database.GormDB.Delete(&old).Error)
	replacement := models.DiggerLock{OrganisationID: old.OrganisationID, Resource: old.Resource, LockId: old.LockId}
	require.NoError(t, database.GormDB.Create(&replacement).Error)
	_, _, err = database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.ErrorIs(t, err, models.ErrGithubSubmissionLockConflict)
	intent.Locks.ClosedOwners, err = database.ReadGithubSubmissionLockOwners(context.Background(), request.Identity, intent.Locks.Acquire)
	require.NoError(t, err)
	_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	var held []models.DiggerLock
	require.NoError(t, database.GormDB.Find(&held).Error)
	require.Len(t, held, 1)
	require.Equal(t, request.PullRequestNumber, held[0].LockId)
}

func TestPostgresGithubSubmissionLocksRollbackAndReplay(t *testing.T) {
	database, request, intent := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.DiggerLock{}))
	intent.Locks = &models.GithubSubmissionLocks{Acquire: []string{"root-one", "root-two"}}
	conflict := models.DiggerLock{OrganisationID: request.OrganisationID, Resource: request.RepoFullName + "#root-two", LockId: request.PullRequestNumber + 1}
	require.NoError(t, database.GormDB.Create(&conflict).Error)
	_, _, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.ErrorIs(t, err, models.ErrGithubSubmissionLockConflict)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubSubmission{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, database.GormDB.Model(&models.DiggerLock{}).Where("lock_id = ?", request.PullRequestNumber).Count(&count).Error)
	require.Zero(t, count, "an earlier acquisition must roll back with the conflicting project")
	require.NoError(t, database.GormDB.Delete(&conflict).Error)
	stored, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, database.GormDB.Model(&models.DiggerLock{}).Where("lock_id = ?", request.PullRequestNumber).Count(&count).Error)
	require.EqualValues(t, 2, count)
	// Represent a later PR-close release. Replaying the earlier preparation must
	// not reacquire those locks or change its saved submission receipt.
	require.NoError(t, database.GormDB.Where("lock_id = ?", request.PullRequestNumber).Delete(&models.DiggerLock{}).Error)
	replayed, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, stored.IntentSHA256, replayed.IntentSHA256)
	require.NoError(t, database.GormDB.Model(&models.DiggerLock{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestPostgresGithubSubmissionUnlockUsesExactTenantRepositoryAndOwner(t *testing.T) {
	database, request, _ := newGithubSubmissionFixture(t)
	require.NoError(t, database.GormDB.AutoMigrate(&models.DiggerLock{}))
	var delivery models.GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&delivery, "operation_id = ?", request.Identity.DeliveryOperationID).Error)
	intent, err := models.PrepareGithubReportOnlySubmission("unlocked", []models.GithubReportCreatePayload{{OrganisationID: request.OrganisationID, GithubAppID: delivery.GithubAppID, GithubInstallationID: request.GithubInstallationID,
		RepoOwner: request.RepoOwner, RepoName: request.RepoName, PullRequestNumber: request.PullRequestNumber, ResourceKind: models.GithubReportResourceComment, Body: "Project locks released"}})
	require.NoError(t, err)
	intent.Locks = &models.GithubSubmissionLocks{ReleaseAll: true}
	otherOrg := models.Organisation{Name: "other-lock-tenant"}
	require.NoError(t, database.GormDB.Create(&otherOrg).Error)
	locks := []models.DiggerLock{
		{OrganisationID: request.OrganisationID, Resource: request.RepoFullName + "#root", LockId: request.PullRequestNumber},
		{OrganisationID: request.OrganisationID, Resource: request.RepoFullName + "-suffix#root", LockId: request.PullRequestNumber},
		{OrganisationID: request.OrganisationID, Resource: request.RepoFullName + "#other", LockId: request.PullRequestNumber + 1},
		{OrganisationID: otherOrg.ID, Resource: request.RepoFullName + "#root", LockId: request.PullRequestNumber},
	}
	require.NoError(t, database.GormDB.Create(&locks).Error)
	_, created, err := database.RecordGithubSubmission(context.Background(), request.Identity, intent)
	require.NoError(t, err)
	require.True(t, created)
	var remaining []models.DiggerLock
	require.NoError(t, database.GormDB.Order("id").Find(&remaining).Error)
	require.Len(t, remaining, 3)
	require.Equal(t, locks[1].ID, remaining[0].ID)
	require.Equal(t, locks[2].ID, remaining[1].ID)
	require.Equal(t, locks[3].ID, remaining[2].ID)
}
