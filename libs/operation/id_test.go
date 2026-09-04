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
