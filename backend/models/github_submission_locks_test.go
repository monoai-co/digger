package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGithubSubmissionLocksRejectMixedAndUnselectedChanges(t *testing.T) {
	for _, locks := range []*GithubSubmissionLocks{
		{}, {Acquire: []string{"one"}, ReleaseAll: true}, {Acquire: []string{"one", "one"}},
		{Acquire: []string{"unknown"}}, {Acquire: []string{" one"}}, {ReleaseAll: true},
	} {
		intent := submissionIntentFixture(t)
		intent.Locks = locks
		_, _, err := canonicalGithubSubmissionIntent(intent)
		require.ErrorIs(t, err, ErrGithubSubmissionIntent)
	}
	intent := submissionIntentFixture(t)
	intent.Locks = &GithubSubmissionLocks{Acquire: []string{"two", "one"}}
	_, normalized, err := canonicalGithubSubmissionIntent(intent)
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two"}, normalized.Locks.Acquire)
	require.Equal(t, []string{"two", "one"}, intent.Locks.Acquire)
}
