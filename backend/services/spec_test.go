package services

import (
	"encoding/json"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func TestDurableWorkflowSpecDisablesCLIProviderReporting(t *testing.T) {
	serialized, err := json.Marshal(scheduler.JobJson{ProjectName: "network"})
	require.NoError(t, err)
	commentID := int64(123)
	job := models.DiggerJob{DiggerJobID: "job", ReporterType: "lazy", SerializedJobSpec: serialized,
		Batch: &models.DiggerBatch{VCS: models.DiggerVCSGithub, RepoOwner: "owner", RepoName: "repo", RepoFullName: "owner/repo", CommentId: &commentID, ReportTerraformOutputs: true}}
	legacy, err := GetSpecFromJob(job)
	require.NoError(t, err)
	require.Equal(t, "lazy", legacy.Reporter.ReporterType)
	require.Equal(t, "123", legacy.CommentId)
	operationID := "durable-operation"
	job.OperationID = &operationID
	durable, err := GetSpecFromJob(job)
	require.NoError(t, err)
	require.Equal(t, "noop", durable.Reporter.ReporterType)
	require.Equal(t, "noop", durable.CommentUpdater.CommentUpdaterType)
	require.Empty(t, durable.CommentId)
	require.True(t, durable.Reporter.ReportTerraformOutput)
	require.Equal(t, "lazy", job.ReporterType, "stored graph identity must not be rewritten")
	require.Equal(t, commentID, *job.Batch.CommentId)
}
