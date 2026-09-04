package bootstrap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildExecutionGrantSecretsRetainsPreviousKeys(t *testing.T) {
	activeSecret := strings.Repeat("active-secret-", 3)
	previousSecret := strings.Repeat("previous-secret-", 3)
	secrets, err := buildExecutionGrantSecrets(
		"key-v2",
		activeSecret,
		`{"key-v1":"`+previousSecret+`"}`,
	)
	require.NoError(t, err)
	require.Equal(t, []byte(activeSecret), secrets["key-v2"])
	require.Equal(t, []byte(previousSecret), secrets["key-v1"])
}

func TestBuildExecutionGrantSecretsRejectsConflictingOrInvalidKeys(t *testing.T) {
	activeSecret := strings.Repeat("active-secret-", 3)
	for name, values := range map[string][3]string{
		"missing active key": {"", activeSecret, ""},
		"short secret":       {"key-v2", "short", ""},
		"invalid retained":   {"key-v2", activeSecret, `{" key-v1":"` + strings.Repeat("previous-secret-", 3) + `"}`},
		"active conflict":    {"key-v2", activeSecret, `{"key-v2":"` + strings.Repeat("different-secret-", 3) + `"}`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildExecutionGrantSecrets(values[0], values[1], values[2])
			require.Error(t, err)
		})
	}
}
