package models

import (
	"encoding/json"
	"testing"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func submissionIntentFixture(t *testing.T) GithubSubmissionIntent {
	t.Helper()
	pr := 42
	intent := GithubSubmissionIntent{Graph: &DurableGraphIntent{ProtocolVersion: operation.ProtocolVersion,
		JobType: scheduler.DiggerCommandPlan, JobReporterType: "lazy", RepoOwner: "owner", RepoName: "repo", RepoFullName: "owner/repo",
		OrganisationID: 1, GithubInstallationID: 2, CommitSHA: "commit", Branch: "branch", PullRequestNumber: pr}}
	for _, project := range []string{"two", "one"} {
		id, err := operation.Derive("submission-test", project)
		require.NoError(t, err)
		spec, err := json.Marshal(scheduler.JobJson{ProjectName: project, JobType: "plan", Branch: "branch", Commit: "commit", PullRequestNumber: &pr})
		require.NoError(t, err)
		intent.Graph.Jobs = append(intent.Graph.Jobs, DurableGraphJobIntent{ProjectName: project, OperationID: id.String(), SerializedSpec: spec, WorkflowFile: "digger.yml"})
	}
	intent.Sources = []GithubSubmissionSource{{Location: "z", Projects: []string{"two", "one"}}, {Location: "a", Projects: []string{"one"}}}
	return intent
}

func TestGithubSubmissionNormalizationIsStableAndDetached(t *testing.T) {
	original := submissionIntentFixture(t)
	first, normalized, err := canonicalGithubSubmissionIntent(original)
	require.NoError(t, err)
	require.Equal(t, "one", normalized.Graph.Jobs[0].ProjectName)
	require.Equal(t, "a", normalized.Sources[0].Location)
	require.Equal(t, []string{"one", "two"}, normalized.Sources[1].Projects)
	require.Equal(t, "two", original.Graph.Jobs[0].ProjectName)
	require.Equal(t, []string{"two", "one"}, original.Sources[0].Projects)
	// Formatting of nested persisted JSON must not change the intent digest.
	var spec map[string]any
	require.NoError(t, json.Unmarshal(normalized.Graph.Jobs[0].SerializedSpec, &spec))
	normalized.Graph.Jobs[0].SerializedSpec, err = json.MarshalIndent(spec, "", "  ")
	require.NoError(t, err)
	second, _, err := canonicalGithubSubmissionIntent(normalized)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestGithubSubmissionRejectsAmbiguousMetadata(t *testing.T) {
	mutations := map[string]func(*GithubSubmissionIntent){
		"duplicate project":   func(i *GithubSubmissionIntent) { i.Graph.Jobs = append(i.Graph.Jobs, i.Graph.Jobs[0]) },
		"duplicate operation": func(i *GithubSubmissionIntent) { i.Graph.Jobs[1].OperationID = i.Graph.Jobs[0].OperationID },
		"duplicate parent":    func(i *GithubSubmissionIntent) { i.Graph.Jobs[0].Parents = []string{"one", "one"} },
		"unknown parent":      func(i *GithubSubmissionIntent) { i.Graph.Jobs[0].Parents = []string{"missing"} },
		"self parent":         func(i *GithubSubmissionIntent) { i.Graph.Jobs[0].Parents = []string{i.Graph.Jobs[0].ProjectName} },
		"blank workflow":      func(i *GithubSubmissionIntent) { i.Graph.Jobs[0].WorkflowFile = " " },
		"blank reporter":      func(i *GithubSubmissionIntent) { i.Graph.JobReporterType = " " },
		"cycle": func(i *GithubSubmissionIntent) {
			i.Graph.Jobs[0].Parents = []string{i.Graph.Jobs[1].ProjectName}
			i.Graph.Jobs[1].Parents = []string{i.Graph.Jobs[0].ProjectName}
		},
		"duplicate location":       func(i *GithubSubmissionIntent) { i.Sources = append(i.Sources, i.Sources[0]) },
		"duplicate source project": func(i *GithubSubmissionIntent) { i.Sources[0].Projects = []string{"one", "one"} },
		"unknown source project":   func(i *GithubSubmissionIntent) { i.Sources[0].Projects = []string{"other"} },
		"unknown job field": func(i *GithubSubmissionIntent) {
			var spec map[string]any
			require.NoError(t, json.Unmarshal(i.Graph.Jobs[0].SerializedSpec, &spec))
			spec["unrecognized"] = true
			encoded, err := json.Marshal(spec)
			require.NoError(t, err)
			i.Graph.Jobs[0].SerializedSpec = encoded
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			intent := submissionIntentFixture(t)
			mutate(&intent)
			_, _, err := canonicalGithubSubmissionIntent(intent)
			require.ErrorIs(t, err, ErrGithubSubmissionIntent)
		})
	}
}

func TestGithubSubmissionGraphIdentityUsesDelivery(t *testing.T) {
	intent := submissionIntentFixture(t)
	deliveryID, err := operation.Derive("submission-delivery", "one")
	require.NoError(t, err)
	batchID, err := operation.DeriveBatch(deliveryID, string(intent.Graph.JobType), intent.Graph.RepoFullName, intent.Graph.PullRequestNumber, intent.Graph.CommitSHA)
	require.NoError(t, err)
	for index := range intent.Graph.Jobs {
		job := &intent.Graph.Jobs[index]
		jobID, err := operation.DeriveJob(batchID, job.ProjectName, job.WorkflowFile)
		require.NoError(t, err)
		job.OperationID = jobID.String()
	}
	normalized, order, err := NormalizeDurableGraphIntent(deliveryID.String(), *intent.Graph)
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two"}, order)
	require.Equal(t, "one", normalized.Jobs[0].ProjectName)
	intent.Graph.Jobs[0].OperationID, intent.Graph.Jobs[1].OperationID = intent.Graph.Jobs[1].OperationID, intent.Graph.Jobs[0].OperationID
	_, _, err = NormalizeDurableGraphIntent(deliveryID.String(), *intent.Graph)
	require.ErrorIs(t, err, ErrFrozenGraphIntent)
}

func TestGithubSubmissionRejectsUnknownEnvelopeAndTrailingJSON(t *testing.T) {
	encoded, err := json.Marshal(submissionIntentFixture(t))
	require.NoError(t, err)
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(encoded, &envelope))
	envelope["unknown"] = true
	unknown, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = DecodeGithubSubmissionIntent(unknown)
	require.ErrorIs(t, err, ErrGithubSubmissionIntent)
	_, err = DecodeGithubSubmissionIntent(append(encoded, []byte(" {}")...))
	require.ErrorIs(t, err, ErrGithubSubmissionIntent)
}
