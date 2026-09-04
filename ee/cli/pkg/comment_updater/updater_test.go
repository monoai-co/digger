package comment_updater

import (
	"testing"

	"github.com/diggerhq/digger/libs/ci"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

type recordingPullRequestService struct {
	ci.MockPullRequestManager
	editedComment string
}

func (service *recordingPullRequestService) EditComment(_ int, _ string, comment string) error {
	service.editedComment = comment
	return nil
}

func TestAdvancedCommentUpdaterRecognizesRedactedDurablePlanResponse(t *testing.T) {
	service := &recordingPullRequestService{}
	jobs := []scheduler.SerializedJob{{
		DiggerJobId: "job-1", ProjectName: "root", Status: scheduler.DiggerJobSucceeded,
		JobString:        []byte(`{"job_type":"plan","projectName":"root","projectAlias":"","commands":["digger plan"]}`),
		ResourcesCreated: 1, ResourcesUpdated: 2, ResourcesDeleted: 3,
	}}

	require.NoError(t, AdvancedCommentUpdater{}.UpdateComment(jobs, 42, service, "comment-1"))
	require.Contains(t, service.editedComment, "Resources: 1 to create, 2 to update, 3 to delete")
}
