package spec

import (
	"testing"

	libspec "github.com/diggerhq/digger/libs/spec"
	"github.com/stretchr/testify/require"
)

func TestStatusRepositoryUsesTheClaimRepositoryIdentity(t *testing.T) {
	jobSpec := libspec.Spec{VCS: libspec.VcsSpec{RepoFullname: "monoai-co/sre", RepoName: "sre"}}
	require.Equal(t, "monoai-co/sre", statusRepository(jobSpec))
}

func TestStatusRepositoryPreservesLegacySpecsWithoutFullName(t *testing.T) {
	jobSpec := libspec.Spec{VCS: libspec.VcsSpec{RepoName: "sre"}}
	require.Equal(t, "sre", statusRepository(jobSpec))
}
