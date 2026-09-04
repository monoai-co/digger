package spec

import (
	"testing"

	commentSummary "github.com/diggerhq/digger/libs/comment_utils/summary"
	libspec "github.com/diggerhq/digger/libs/spec"
	"github.com/stretchr/testify/require"
)

func TestRunSpecManualCommandRejectsDurableJobBeforeProviderUse(t *testing.T) {
	err := RunSpecManualCommand(
		libspec.Spec{OperationID: "durable-operation"},
		nil,
		libspec.JobSpecProvider{},
		libspec.LockProvider{},
		libspec.ReporterProvider{},
		libspec.BackendApiProvider{},
		nil,
		libspec.PlanStorageProvider{},
		commentSummary.CommentUpdaterProviderBasic{},
	)
	require.EqualError(t, err, "durable jobs cannot use manual execution mode")
}
