package models

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGithubDeliveryTargetDecodeAndSignedValidation(t *testing.T) {
	preparation, err := PrepareGithubDeliveryTargetIntent(deliveryTargetSignedFixture(t))
	require.NoError(t, err)
	intent, err := preparation.Resolve(nil)
	require.NoError(t, err)
	raw, err := json.Marshal(intent)
	require.NoError(t, err)
	decoded, err := DecodeGithubDeliveryTarget(raw)
	require.NoError(t, err)
	require.Equal(t, intent, decoded)
	for _, invalid := range [][]byte{[]byte(`{}`), append(append([]byte(nil), raw...), []byte(` {}`)...), []byte(strings.TrimSuffix(string(raw), "}") + `,"unknown":true}`), {0xff}} {
		_, err := DecodeGithubDeliveryTarget(invalid)
		require.Error(t, err)
	}
	for name, mutate := range map[string]func(*GithubDeliveryTargetIntent){
		"PR":         func(i *GithubDeliveryTargetIntent) { i.PullRequestNumber++ },
		"repository": func(i *GithubDeliveryTargetIntent) { i.RepositoryID++ },
		"head":       func(i *GithubDeliveryTargetIntent) { i.HeadSHA = "newer-head" },
		"ref":        func(i *GithubDeliveryTargetIntent) { i.HeadRef = "newer-branch" },
		"source":     func(i *GithubDeliveryTargetIntent) { i.Source = GithubDeliveryTargetIssueCommentLookup },
	} {
		t.Run(name, func(t *testing.T) {
			changed := intent
			mutate(&changed)
			require.ErrorIs(t, preparation.ValidateIntent(changed), ErrGithubDeliveryTargetIntent)
		})
	}
}
