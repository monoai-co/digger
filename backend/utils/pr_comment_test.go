package utils

import (
	"errors"
	"strings"
	"testing"

	"github.com/diggerhq/digger/libs/ci"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

type initialCommentCapture struct {
	ci.PullRequestService
	body string
	err  error
}

func (capture *initialCommentCapture) EditComment(_ int, _ string, body string) error {
	capture.body = body
	return capture.err
}

func TestReportLayersDoesNotReorderCallerJobs(t *testing.T) {
	jobs := []scheduler.Job{{ProjectName: "later", Layer: 2}, {ProjectName: "z", ProjectAlias: "Zed", Layer: 0}, {ProjectName: "a", Layer: 0}}
	original := append([]scheduler.Job(nil), jobs...)
	capture := &initialCommentCapture{}
	require.NoError(t, ReportLayersTableForJobs(&CommentReporter{PrNumber: 1, PrService: capture, CommentId: "2"}, jobs))
	require.Equal(t, original, jobs)
	require.Contains(t, capture.body, "|:clock11: **a**|0|\n|:clock11: **Zed**|0|\n|:clock11: **later**|2|\n")
	require.Equal(t, GetLayersTableForJobs(jobs), capture.body)
}

func TestInitialSummaryFrozenSpecsMatchLegacyOutput(t *testing.T) {
	for _, test := range []struct {
		name     string
		jobs     []scheduler.Job
		expected string
	}{
		{"empty", nil, ":construction_worker: No projects impacted"},
		{"aliases and order", []scheduler.Job{{ProjectName: "z", ProjectAlias: "Zed"}, {ProjectName: "a"}}, "| Project | Status |\n|---------|--------|\n|:clock11: **Zed**|pending...|\n|:clock11: **a**|pending...|\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			specs := make([]scheduler.JobJson, len(test.jobs))
			for index, job := range test.jobs {
				specs[index] = scheduler.JobJson{ProjectName: job.ProjectName, ProjectAlias: job.ProjectAlias}
			}
			require.Equal(t, test.expected, GetInitialJobSummary(test.jobs))
			require.Equal(t, test.expected, GetInitialJobSummaryFromJobSpecs(specs))
			capture := &initialCommentCapture{}
			require.NoError(t, ReportInitialJobsStatus(&CommentReporter{PrNumber: 1, PrService: capture, CommentId: "2"}, test.jobs))
			require.Equal(t, test.expected, capture.body)
		})
	}
}

func TestLayerSummaryEmptyRetainsInstructions(t *testing.T) {
	message := GetLayersTableForJobs(nil)
	require.True(t, strings.HasPrefix(message, ":construction_worker: No projects impacted----------------\n\n"))
	require.Contains(t, message, `"digger plan --layer 0"`)
	require.Contains(t, message, `"digger apply --layer 1"`)
	require.True(t, strings.HasSuffix(message, "</details>\n"))
}

func TestInitialReportWrappersPreserveProviderErrors(t *testing.T) {
	providerError := errors.New("comment unavailable")
	capture := &initialCommentCapture{err: providerError}
	reporter := &CommentReporter{PrNumber: 1, PrService: capture, CommentId: "2"}
	require.ErrorIs(t, ReportInitialJobsStatus(reporter, nil), providerError)
	require.ErrorIs(t, ReportLayersTableForJobs(reporter, nil), providerError)
}
