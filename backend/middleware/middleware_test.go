package middleware

import (
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCheckJobTokenPreservesLegacyTokensAndEnforcesDurableLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "job-token.sqlite")+"?_foreign_keys=on"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&models.Organisation{}, &models.DiggerBatch{}, &models.DiggerJobSummary{}, &models.DiggerJob{}, &models.JobToken{}))
	previousDatabase := models.DB
	models.DB = &models.Database{GormDB: database}
	t.Cleanup(func() { models.DB = previousDatabase })

	organisation := models.Organisation{Name: "token-test", ExternalSource: "test", ExternalId: "token-test"}
	require.NoError(t, database.Create(&organisation).Error)
	legacy := models.JobToken{Value: "cli:legacy", Expiry: time.Now().Add(time.Hour), OrganisationID: organisation.ID, Type: models.CliJobAccessType}
	require.NoError(t, database.Create(&legacy).Error)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	checked, err := CheckJobToken(context, legacy.Value)
	require.NoError(t, err)
	require.Equal(t, legacy.ID, checked.ID)

	summary := models.DiggerJobSummary{}
	require.NoError(t, database.Create(&summary).Error)
	job := models.DiggerJob{DiggerJobID: "durable-job", Status: 1, DiggerJobSummaryID: summary.ID}
	require.NoError(t, database.Create(&job).Error)
	durable := models.JobToken{Value: "cli:durable", Expiry: time.Now().Add(time.Hour), OrganisationID: organisation.ID, DiggerJobDatabaseID: &job.ID, Type: models.CliJobAccessType}
	require.NoError(t, database.Create(&durable).Error)
	context, _ = gin.CreateTestContext(httptest.NewRecorder())
	_, err = CheckJobToken(context, durable.Value)
	require.Error(t, err)

	activatedAt := time.Now().Add(-time.Minute)
	require.NoError(t, database.Model(&models.JobToken{}).Where("id = ?", durable.ID).Update("activated_at", activatedAt).Error)
	context, _ = gin.CreateTestContext(httptest.NewRecorder())
	checked, err = CheckJobToken(context, durable.Value)
	require.NoError(t, err)
	require.Equal(t, durable.ID, checked.ID)

	revokedAt := time.Now()
	require.NoError(t, database.Model(&models.JobToken{}).Where("id = ?", durable.ID).Update("revoked_at", revokedAt).Error)
	context, _ = gin.CreateTestContext(httptest.NewRecorder())
	_, err = CheckJobToken(context, durable.Value)
	require.Error(t, err)
}
