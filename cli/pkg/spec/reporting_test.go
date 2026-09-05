package spec

import (
	"testing"

	"github.com/diggerhq/digger/libs/comment_utils/reporting"
	comment_summary "github.com/diggerhq/digger/libs/comment_utils/summary"
	libspec "github.com/diggerhq/digger/libs/spec"
	"github.com/stretchr/testify/require"
)

type recordingCommentUpdaterProvider struct{ calls int }

func (provider *recordingCommentUpdaterProvider) Get(string) (comment_summary.CommentUpdater, error) {
	provider.calls++
	return comment_summary.NoopCommentUpdater{}, nil
}

func TestDurableExecutionLeavesProviderReportingToBackend(t *testing.T) {
	jobSpec := libspec.Spec{OperationID: "durable-operation", Reporter: libspec.ReporterSpec{ReporterType: "basic", ReportTerraformOutput: true}}
	routed := reporterSpecForExecution(jobSpec)
	require.Equal(t, "noop", routed.ReporterType)
	require.True(t, routed.ReportTerraformOutput, "callback output must remain enabled")
	require.Equal(t, "basic", jobSpec.Reporter.ReporterType)
	reporter, err := (libspec.ReporterProvider{}).GetReporter("report", routed, nil, 42, "github")
	require.NoError(t, err)
	require.IsType(t, reporting.NoopReporter{}, reporter)
	_, _, err = reporter.Report("report", func(s string) string { return s })
	require.NoError(t, err)
	_, _, err = reporter.Flush()
	require.NoError(t, err)
	provider := &recordingCommentUpdaterProvider{}
	updater, err := commentUpdaterForExecution(jobSpec, provider)
	require.NoError(t, err)
	require.Zero(t, provider.calls)
	require.NoError(t, updater.UpdateComment(nil, 42, nil, ""))
	jobSpec.OperationID = ""
	require.Equal(t, jobSpec.Reporter, reporterSpecForExecution(jobSpec))
	_, err = commentUpdaterForExecution(jobSpec, provider)
	require.NoError(t, err)
	require.Equal(t, 1, provider.calls)
}
