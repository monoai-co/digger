package models

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGithubReportProviderURLBindsResourceIdentity(t *testing.T) {
	payload := githubReportCheckFixture()
	providerURL, err := GithubReportProviderURL(payload, 123)
	require.NoError(t, err)
	require.Equal(t, "https://github.com/owner/repo/pull/4/checks?check_run_id=123", providerURL)
	payload.ResourceKind, payload.Check, payload.HeadSHA, payload.Body = GithubReportResourceComment, nil, "", "report"
	providerURL, err = GithubReportProviderURL(payload, 123)
	require.NoError(t, err)
	require.Equal(t, "https://github.com/owner/repo/pull/4#issuecomment-123", providerURL)
	_, err = GithubReportProviderURL(payload, 0)
	require.ErrorIs(t, err, ErrGithubReportCreateConflict)
	payload.RepoName = "repo/other"
	_, err = GithubReportProviderURL(payload, 123)
	require.ErrorIs(t, err, ErrGithubReportCreateConflict)
}

func TestGithubReportReceiptRequiresExactEffectAndProviderIdentity(t *testing.T) {
	payload := githubReportCheckFixture()
	effect := &OutboxEffect{ID: uuid.New(), PayloadSHA256: payloadSHA256([]byte("intent"))}
	providerURL, err := GithubReportProviderURL(payload, 123)
	require.NoError(t, err)
	valid := GithubReportCreateReceipt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
		ResourceKind: payload.ResourceKind, ProviderID: 123, ProviderURL: providerURL}
	require.NoError(t, validateGithubReportReceipt(valid, effect, payload))
	for name, mutate := range map[string]func(*GithubReportCreateReceipt){
		"effect":             func(r *GithubReportCreateReceipt) { r.EffectID = uuid.New() },
		"digest":             func(r *GithubReportCreateReceipt) { r.PayloadSHA256 = payloadSHA256([]byte("other")) },
		"kind":               func(r *GithubReportCreateReceipt) { r.ResourceKind = GithubReportResourceComment },
		"zero provider":      func(r *GithubReportCreateReceipt) { r.ProviderID = 0 },
		"different provider": func(r *GithubReportCreateReceipt) { r.ProviderID = 456 },
		"http": func(r *GithubReportCreateReceipt) {
			r.ProviderURL = "http://github.com/owner/repo/pull/4/checks?check_run_id=123"
		},
		"different repository": func(r *GithubReportCreateReceipt) {
			r.ProviderURL = "https://github.com/owner/other/pull/4/checks?check_run_id=123"
		},
		"different pull request": func(r *GithubReportCreateReceipt) {
			r.ProviderURL = "https://github.com/owner/repo/pull/5/checks?check_run_id=123"
		},
		"unrelated provider path": func(r *GithubReportCreateReceipt) { r.ProviderURL = "https://github.com/owner/repo/runs/123" },
		"userinfo": func(r *GithubReportCreateReceipt) {
			r.ProviderURL = "https://other@github.com/owner/repo/pull/4/checks?check_run_id=123"
		},
	} {
		t.Run(name, func(t *testing.T) {
			receipt := valid
			mutate(&receipt)
			require.ErrorIs(t, validateGithubReportReceipt(receipt, effect, payload), ErrGithubReportCreateConflict)
		})
	}
}

func TestGithubReportAttemptRetainsHistoricalWriterIdentity(t *testing.T) {
	effect := &OutboxEffect{ID: uuid.New(), ControlOperationID: "operation", EffectKey: "report", PayloadSHA256: payloadSHA256([]byte("intent")), WriterEpoch: 2, LeaseID: "new-lease"}
	attempt := GithubReportCreateAttempt{EffectID: effect.ID, ControlOperationID: effect.ControlOperationID, EffectKey: effect.EffectKey, PayloadSHA256: effect.PayloadSHA256, WriterEpoch: 1, LeaseID: "original-lease", CreatedAt: time.Now().UTC()}
	require.True(t, validGithubReportAttempt(&attempt, effect))
	for _, mutate := range []func(*GithubReportCreateAttempt){
		func(a *GithubReportCreateAttempt) { a.EffectID = uuid.New() },
		func(a *GithubReportCreateAttempt) { a.ControlOperationID = "other-operation" },
		func(a *GithubReportCreateAttempt) { a.EffectKey = "other-report" },
		func(a *GithubReportCreateAttempt) { a.PayloadSHA256 = "wrong" },
		func(a *GithubReportCreateAttempt) { a.WriterEpoch = 0 },
		func(a *GithubReportCreateAttempt) { a.WriterEpoch = 3 },
		func(a *GithubReportCreateAttempt) { a.LeaseID = " " },
		func(a *GithubReportCreateAttempt) { a.CreatedAt = time.Time{} },
	} {
		invalid := attempt
		mutate(&invalid)
		require.False(t, validGithubReportAttempt(&invalid, effect))
	}
}

func TestGithubReportCreateRejectsMalformedLeaseBeforeDatabaseAccess(t *testing.T) {
	db := &Database{}
	for _, input := range []struct {
		id    uuid.UUID
		lease string
		epoch int64
	}{{uuid.Nil, "lease", 1}, {uuid.New(), " ", 1}, {uuid.New(), "lease", 0}} {
		result, err := db.PrepareGithubReportCreate(context.Background(), input.id, input.lease, "database", input.epoch)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrGithubReportCreateClaim)
	}
}

func TestGithubReportProviderIdentityIgnoresPRAndRepositoryCase(t *testing.T) {
	payload := githubReportCheckFixture()
	identity, err := githubReportProviderIdentitySHA256(payload, 123)
	require.NoError(t, err)
	require.Equal(t, payloadSHA256([]byte(`["github.com","owner","repo","check_run",123]`)), identity)
	payload.RepoOwner, payload.RepoName, payload.PullRequestNumber = "OWNER", "REPO", 99
	replayed, err := githubReportProviderIdentitySHA256(payload, 123)
	require.NoError(t, err)
	require.Equal(t, identity, replayed)
	for _, mutate := range []func(*GithubReportCreatePayload){
		func(p *GithubReportCreatePayload) { p.RepoOwner = "other" },
		func(p *GithubReportCreatePayload) { p.RepoName = "other" },
		func(p *GithubReportCreatePayload) {
			p.ResourceKind, p.Check, p.HeadSHA, p.Body = GithubReportResourceComment, nil, "", "report"
		},
	} {
		other := githubReportCheckFixture()
		mutate(&other)
		digest, err := githubReportProviderIdentitySHA256(other, 123)
		require.NoError(t, err)
		require.NotEqual(t, identity, digest)
	}
	otherID, err := githubReportProviderIdentitySHA256(payload, 456)
	require.NoError(t, err)
	require.NotEqual(t, identity, otherID)
}
