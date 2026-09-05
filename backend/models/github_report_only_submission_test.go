package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGithubReportOnlySubmissionRejectsMixedOrMissingOutcomes(t *testing.T) {
	check := githubReportCheckFixture()
	comment := check
	comment.ResourceKind, comment.HeadSHA, comment.Check, comment.Body = GithubReportResourceComment, "", nil, "No impacted projects"
	intent, err := PrepareGithubReportOnlySubmission("no_impacted_projects", []GithubReportCreatePayload{comment, check})
	require.NoError(t, err)
	require.Nil(t, intent.Graph)
	canonical, normalized, err := canonicalGithubSubmissionIntent(intent)
	require.NoError(t, err)
	reencoded, _, err := canonicalGithubSubmissionIntent(normalized)
	require.NoError(t, err)
	require.Equal(t, canonical, reencoded)
	for name, mutate := range map[string]func(*GithubSubmissionIntent){
		"mixed execution":   func(i *GithubSubmissionIntent) { i.Graph = submissionIntentFixture(t).Graph },
		"neither variant":   func(i *GithubSubmissionIntent) { i.ReportOnly = nil },
		"empty reason":      func(i *GithubSubmissionIntent) { i.ReportOnly.Reason = "" },
		"empty reports":     func(i *GithubSubmissionIntent) { i.Reports = nil },
		"execution binding": func(i *GithubSubmissionIntent) { i.Reports[0].Role = GithubSubmissionReportSummary },
		"project binding":   func(i *GithubSubmissionIntent) { i.Reports[0].ProjectName = "root" },
		"source binding": func(i *GithubSubmissionIntent) {
			i.Sources = []GithubSubmissionSource{{Location: "root", Projects: []string{"root"}}}
		},
		"optional outcome": func(i *GithubSubmissionIntent) { i.Reports[0].Optional = true },
		"duplicate key":    func(i *GithubSubmissionIntent) { i.Reports[1].Key = i.Reports[0].Key },
		"duplicate order":  func(i *GithubSubmissionIntent) { i.Reports[1].Order = i.Reports[0].Order },
	} {
		t.Run(name, func(t *testing.T) {
			var invalid GithubSubmissionIntent
			require.NoError(t, json.Unmarshal(canonical, &invalid))
			mutate(&invalid)
			_, _, err := canonicalGithubSubmissionIntent(invalid)
			require.ErrorIs(t, err, ErrGithubSubmissionIntent)
		})
	}
	other := check
	other.PullRequestNumber++
	_, err = PrepareGithubReportOnlySubmission("mixed_pr", []GithubReportCreatePayload{check, other})
	require.ErrorIs(t, err, ErrGithubSubmissionIntent)
	other = check
	other.HeadSHA = "other-head"
	_, err = PrepareGithubReportOnlySubmission("mixed_head", []GithubReportCreatePayload{comment, check, other})
	require.ErrorIs(t, err, ErrGithubSubmissionIntent)
}

func TestGithubSubmissionExecutionSerializationRemainsCompatible(t *testing.T) {
	_, normalized, err := canonicalGithubSubmissionIntent(submissionIntentFixture(t))
	require.NoError(t, err)
	// Preserve the original execution-only JSON shape, including field order.
	legacy := struct {
		Graph   DurableGraphIntent       `json:"graph"`
		Sources []GithubSubmissionSource `json:"sources"`
		Reports []GithubSubmissionReport `json:"reports"`
	}{Graph: *normalized.Graph, Sources: normalized.Sources, Reports: normalized.Reports}
	before, err := json.Marshal(legacy)
	require.NoError(t, err)
	after, err := json.Marshal(normalized)
	require.NoError(t, err)
	require.Equal(t, before, after)
	decoded, err := DecodeGithubSubmissionIntent(before)
	require.NoError(t, err)
	require.Nil(t, decoded.ReportOnly)
	decoded.Graph.Jobs = nil
	_, _, err = canonicalGithubSubmissionIntent(decoded)
	require.ErrorIs(t, err, ErrGithubSubmissionIntent, "report-only support must not admit an empty execution graph")
}
