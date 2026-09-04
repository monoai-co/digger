package operation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveFixedVector(t *testing.T) {
	id, err := Derive("delivery", "github-app:123", "delivery:abc")
	require.NoError(t, err)
	require.Equal(t, "op1_bd0231d0664d71be9f8344351db277ea65b3cc5179a54b8de87afc4ec5302fac", id.String())
	require.True(t, id.Valid())
}

func TestDeriveIsLengthDelimitedAndKindScoped(t *testing.T) {
	first, err := Derive("delivery", "ab", "c")
	require.NoError(t, err)
	second, err := Derive("delivery", "a", "bc")
	require.NoError(t, err)
	otherKind, err := Derive("job", "ab", "c")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	require.NotEqual(t, first, otherKind)
}

func TestDeriveRejectsEmptyIdentityComponents(t *testing.T) {
	_, err := Derive("", "delivery")
	require.ErrorIs(t, err, ErrInvalidComponent)
	_, err = Derive("delivery", "")
	require.ErrorIs(t, err, ErrInvalidComponent)
}

func TestDeriveBatchAndJobAreStableAndScoped(t *testing.T) {
	delivery, err := Derive("github-webhook-delivery", "github-app:123", "delivery:abc")
	require.NoError(t, err)
	batch, err := DeriveBatch(delivery, "apply", "monoai-co/sre", 42, "deadbeef")
	require.NoError(t, err)
	job, err := DeriveJob(batch, "production", "digger_workflow.yml")
	require.NoError(t, err)

	require.Equal(t, "op1_4206677fdf1a0a0ea0c97aa428d317eda34e19c192980eb1dfb8f99f4d19695e", batch.String())
	require.Equal(t, "op1_a67a77e1d7f57c21a943f3f131a52491b23580af4ea8fc6ff8f7cdf0a9f3d298", job.String())
	retryBatch, err := DeriveBatch(delivery, "apply", "monoai-co/sre", 42, "deadbeef")
	require.NoError(t, err)
	retryJob, err := DeriveJob(retryBatch, "production", "digger_workflow.yml")
	require.NoError(t, err)
	require.Equal(t, batch, retryBatch)
	require.Equal(t, job, retryJob)

	otherCommand, err := DeriveBatch(delivery, "plan", "monoai-co/sre", 42, "deadbeef")
	require.NoError(t, err)
	otherRepository, err := DeriveBatch(delivery, "apply", "monoai-co/marketplace-infra", 42, "deadbeef")
	require.NoError(t, err)
	otherPullRequest, err := DeriveBatch(delivery, "apply", "monoai-co/sre", 43, "deadbeef")
	require.NoError(t, err)
	otherCommit, err := DeriveBatch(delivery, "apply", "monoai-co/sre", 42, "feedface")
	require.NoError(t, err)
	otherProject, err := DeriveJob(batch, "staging", "digger_workflow.yml")
	require.NoError(t, err)
	otherWorkflow, err := DeriveJob(batch, "production", "custom.yml")
	require.NoError(t, err)
	require.NotEqual(t, batch, otherCommand)
	require.NotEqual(t, batch, otherRepository)
	require.NotEqual(t, batch, otherPullRequest)
	require.NotEqual(t, batch, otherCommit)
	require.NotEqual(t, job, otherProject)
	require.NotEqual(t, job, otherWorkflow)
}

func TestDeriveBatchRejectsInvalidIdentityComponents(t *testing.T) {
	delivery, err := Derive("github-webhook-delivery", "github-app:123", "delivery:abc")
	require.NoError(t, err)

	_, err = DeriveBatch("not-an-operation", "apply", "monoai-co/sre", 42, "deadbeef")
	require.ErrorIs(t, err, ErrInvalidComponent)
	invalidComponents := []struct {
		name              string
		command           string
		repository        string
		pullRequestNumber int
		commitSHA         string
	}{
		{name: "empty command", command: "", repository: "monoai-co/sre", pullRequestNumber: 42, commitSHA: "deadbeef"},
		{name: "blank command", command: "  ", repository: "monoai-co/sre", pullRequestNumber: 42, commitSHA: "deadbeef"},
		{name: "empty repository", command: "apply", repository: "", pullRequestNumber: 42, commitSHA: "deadbeef"},
		{name: "zero pull request", command: "apply", repository: "monoai-co/sre", pullRequestNumber: 0, commitSHA: "deadbeef"},
		{name: "negative pull request", command: "apply", repository: "monoai-co/sre", pullRequestNumber: -1, commitSHA: "deadbeef"},
		{name: "empty commit", command: "apply", repository: "monoai-co/sre", pullRequestNumber: 42, commitSHA: ""},
	}
	for _, testCase := range invalidComponents {
		t.Run(testCase.name, func(t *testing.T) {
			_, deriveErr := DeriveBatch(delivery, testCase.command, testCase.repository, testCase.pullRequestNumber, testCase.commitSHA)
			require.ErrorIs(t, deriveErr, ErrInvalidComponent)
		})
	}
}

func TestDeriveJobRejectsInvalidIdentityComponents(t *testing.T) {
	delivery, err := Derive("github-webhook-delivery", "github-app:123", "delivery:abc")
	require.NoError(t, err)
	batch, err := DeriveBatch(delivery, "apply", "monoai-co/sre", 42, "deadbeef")
	require.NoError(t, err)

	_, err = DeriveJob("not-an-operation", "production", "digger_workflow.yml")
	require.ErrorIs(t, err, ErrInvalidComponent)
	_, err = DeriveJob(batch, "", "digger_workflow.yml")
	require.ErrorIs(t, err, ErrInvalidComponent)
	_, err = DeriveJob(batch, "production", "")
	require.ErrorIs(t, err, ErrInvalidComponent)
	_, err = DeriveJob(batch, "  ", "digger_workflow.yml")
	require.ErrorIs(t, err, ErrInvalidComponent)
}
