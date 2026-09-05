package models

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func githubReportCheckFixture() GithubReportCreatePayload {
	return GithubReportCreatePayload{OrganisationID: 1, GithubAppID: 2, GithubInstallationID: 3,
		RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 4, HeadSHA: "commit",
		ResourceKind: GithubReportResourceCheckRun,
		Check:        &GithubReportCheck{Name: "plan", Status: "queued", Title: "Plan"}}
}

func TestGithubReportPreparationBoundsTextBeforeFreezing(t *testing.T) {
	for _, text := range []string{strings.Repeat("x", GithubReportTextMaxBytes), strings.Repeat("界🙂", GithubReportTextMaxBytes)} {
		payload := githubReportCheckFixture()
		payload.Check.Summary, payload.Check.Text = text, text
		raw, err := PrepareGithubReportCreatePayload(payload)
		require.NoError(t, err)
		require.Equal(t, text, payload.Check.Text, "preparation must not mutate caller-owned checks")
		prepared, err := DecodeGithubReportCreatePayload(raw)
		require.NoError(t, err)
		require.True(t, utf8.ValidString(prepared.Check.Text))
		require.LessOrEqual(t, len(prepared.Check.Text), GithubReportTextMaxBytes)
		require.Equal(t, prepared.Check.Summary, prepared.Check.Text)
		if len(text) > GithubReportTextMaxBytes {
			require.True(t, strings.HasSuffix(prepared.Check.Text, githubReportTruncatedSuffix))
		} else {
			require.Equal(t, text, prepared.Check.Text)
		}
		replayed, err := PrepareGithubReportCreatePayload(prepared)
		require.NoError(t, err)
		require.Equal(t, raw, replayed)
		payload.ResourceKind, payload.Check, payload.HeadSHA, payload.Body = GithubReportResourceComment, nil, "", text
		raw, err = PrepareGithubReportCreatePayload(payload)
		require.NoError(t, err)
		prepared, err = DecodeGithubReportCreatePayload(raw)
		require.NoError(t, err)
		require.True(t, utf8.ValidString(prepared.Body))
		require.LessOrEqual(t, len(prepared.Body), GithubReportTextMaxBytes)
	}
}

func TestGithubReportFrozenPayloadCannotSilentlyTrim(t *testing.T) {
	for _, field := range []string{"body", "summary", "text", "name", "title"} {
		payload := githubReportCheckFixture()
		oversized := strings.Repeat("x", GithubReportTextMaxBytes+1)
		switch field {
		case "body":
			payload.ResourceKind, payload.Check, payload.HeadSHA, payload.Body = GithubReportResourceComment, nil, "", oversized
		case "summary":
			payload.Check.Summary = oversized
		case "text":
			payload.Check.Text = oversized
		case "name":
			payload.Check.Name = strings.Repeat("x", GithubReportLabelMaxBytes+1)
		case "title":
			payload.Check.Title = strings.Repeat("x", GithubReportLabelMaxBytes+1)
		}
		_, err := CanonicalGithubReportCreatePayload(payload)
		require.ErrorIs(t, err, ErrGithubReportCreatePayload)
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		_, err = DecodeGithubReportCreatePayload(raw)
		require.ErrorIs(t, err, ErrGithubReportCreatePayload)
	}
	payload := githubReportCheckFixture()
	payload.Check.Text = strings.Repeat("x", GithubReportTextMaxBytes+1) + string([]byte{0xff})
	_, err := PrepareGithubReportCreatePayload(payload)
	require.ErrorIs(t, err, ErrGithubReportCreatePayload, "invalid UTF-8 beyond the cutoff must still fail")
}

func TestGithubReportCreateCanonicalPayload(t *testing.T) {
	payload := githubReportCheckFixture()
	canonical, err := CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	require.Nil(t, payload.Check.Actions, "canonicalization must not mutate the caller")
	payload.Check.Actions = []GithubReportCheckAction{}
	emptyActions, err := CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	require.Equal(t, canonical, emptyActions)
	var reordered map[string]any
	require.NoError(t, json.Unmarshal(canonical, &reordered))
	pretty, err := json.MarshalIndent(reordered, "", "  ")
	require.NoError(t, err)
	normalized, err := normalizeOutboxEffectPayload(GithubReportCreateEffectKind, pretty)
	require.NoError(t, err)
	require.Equal(t, canonical, normalized)

	payload.ResourceKind, payload.Check, payload.Body, payload.HeadSHA = GithubReportResourceComment, nil, "  report\n", ""
	canonical, err = CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
	decoded, err := DecodeGithubReportCreatePayload(canonical)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

func TestGithubReportCreateRejectsInvalidContract(t *testing.T) {
	mutations := map[string]func(*GithubReportCreatePayload){
		"zero org":          func(p *GithubReportCreatePayload) { p.OrganisationID = 0 },
		"negative app":      func(p *GithubReportCreatePayload) { p.GithubAppID = -1 },
		"zero installation": func(p *GithubReportCreatePayload) { p.GithubInstallationID = 0 },
		"zero pull request": func(p *GithubReportCreatePayload) { p.PullRequestNumber = 0 },
		"blank owner":       func(p *GithubReportCreatePayload) { p.RepoOwner = " " },
		"blank repo":        func(p *GithubReportCreatePayload) { p.RepoName = " " },
		"blank commit":      func(p *GithubReportCreatePayload) { p.HeadSHA = " " },
		"padded owner":      func(p *GithubReportCreatePayload) { p.RepoOwner = " owner" },
		"padded repo":       func(p *GithubReportCreatePayload) { p.RepoName = "repo " },
		"padded commit":     func(p *GithubReportCreatePayload) { p.HeadSHA = "commit\n" },
		"commit query":      func(p *GithubReportCreatePayload) { p.HeadSHA = "commit?filter=all" },
		"commit traversal":  func(p *GithubReportCreatePayload) { p.HeadSHA = "../other" },
		"commit fragment":   func(p *GithubReportCreatePayload) { p.HeadSHA = "commit#other" },
		"unknown kind":      func(p *GithubReportCreatePayload) { p.ResourceKind = "unknown" },
		"check missing":     func(p *GithubReportCreatePayload) { p.Check = nil },
		"mixed variants":    func(p *GithubReportCreatePayload) { p.Body = "comment" },
		"comment check":     func(p *GithubReportCreatePayload) { p.ResourceKind = GithubReportResourceComment; p.Body = "comment" },
		"empty comment": func(p *GithubReportCreatePayload) {
			p.ResourceKind = GithubReportResourceComment
			p.Check = nil
			p.Body = " "
			p.HeadSHA = ""
		},
		"comment commit": func(p *GithubReportCreatePayload) {
			p.ResourceKind = GithubReportResourceComment
			p.Check = nil
			p.Body = "comment"
		},
		"blank name":               func(p *GithubReportCreatePayload) { p.Check.Name = " " },
		"blank title":              func(p *GithubReportCreatePayload) { p.Check.Title = " " },
		"provider only status":     func(p *GithubReportCreatePayload) { p.Check.Status = "waiting" },
		"missing conclusion":       func(p *GithubReportCreatePayload) { p.Check.Status = "completed" },
		"premature conclusion":     func(p *GithubReportCreatePayload) { p.Check.Conclusion = "success" },
		"provider only conclusion": func(p *GithubReportCreatePayload) { p.Check.Status = "completed"; p.Check.Conclusion = "stale" },
		"invalid utf8":             func(p *GithubReportCreatePayload) { p.Check.Text = string([]byte{0xff}) },
		"oversized envelope":       func(p *GithubReportCreatePayload) { p.Check.Text = strings.Repeat("x", GithubReportCreateMaxBytes) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			payload := githubReportCheckFixture()
			mutate(&payload)
			_, err := CanonicalGithubReportCreatePayload(payload)
			require.ErrorIs(t, err, ErrGithubReportCreatePayload)
		})
	}
}

func TestGithubReportCreateStrictDecode(t *testing.T) {
	canonical, err := CanonicalGithubReportCreatePayload(githubReportCheckFixture())
	require.NoError(t, err)
	for _, raw := range [][]byte{
		append(append([]byte{}, canonical...), []byte(` {}`)...),
		[]byte(strings.Replace(string(canonical), `"check":`, `"provider_id":12,"check":`, 1)),
		[]byte(strings.Replace(string(canonical), `"name":`, `"external_id":"caller","name":`, 1)),
		[]byte(`null`), []byte(`{`), {0xff},
		[]byte(strings.Repeat(" ", GithubReportCreateMaxBytes+1)),
	} {
		_, err := DecodeGithubReportCreatePayload(raw)
		require.ErrorIs(t, err, ErrGithubReportCreatePayload)
		_, err = normalizeOutboxEffectPayload(GithubReportCreateEffectKind, raw)
		require.ErrorIs(t, err, ErrOutboxEffectPayload)
	}
}

func TestGithubReportCreateRejectsAmbiguousRepositoryPaths(t *testing.T) {
	for _, segment := range []string{"other/repo", "other\\repo", "repo?query", "repo#fragment", "repo%2frepo", ".", "..", "repo\x00name", "repo\nname", "repo\x7fname"} {
		for _, field := range []string{"owner", "repo", "head"} {
			payload := githubReportCheckFixture()
			switch field {
			case "owner":
				payload.RepoOwner = segment
			case "repo":
				payload.RepoName = segment
			case "head":
				payload.HeadSHA = segment
			}
			_, err := CanonicalGithubReportCreatePayload(payload)
			require.ErrorIs(t, err, ErrGithubReportCreatePayload, "field=%s segment=%q", field, segment)
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			_, err = DecodeGithubReportCreatePayload(raw)
			require.ErrorIs(t, err, ErrGithubReportCreatePayload)
		}
	}
	payload := githubReportCheckFixture()
	payload.RepoName = "repo.with-dots_and-underscores"
	_, err := CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err)
}

func TestGithubReportCreateCheckLifecycleAndActions(t *testing.T) {
	for _, conclusion := range []string{"success", "failure", "neutral", "cancelled", "skipped", "timed_out", "action_required"} {
		payload := githubReportCheckFixture()
		payload.Check.Status, payload.Check.Conclusion = "completed", conclusion
		_, err := CanonicalGithubReportCreatePayload(payload)
		require.NoError(t, err)
	}
	payload := githubReportCheckFixture()
	payload.Check.Status = "in_progress"
	action := GithubReportCheckAction{Label: strings.Repeat("界", 20), Description: strings.Repeat("界", 40), Identifier: strings.Repeat("界", 20)}
	payload.Check.Actions = []GithubReportCheckAction{action, action, action}
	_, err := CanonicalGithubReportCreatePayload(payload)
	require.NoError(t, err, "limits count characters, not UTF-8 bytes")
	for _, mutate := range []func(*GithubReportCheck){
		func(c *GithubReportCheck) { c.Actions = append(c.Actions, action) },
		func(c *GithubReportCheck) { c.Actions[0].Label += "x" },
		func(c *GithubReportCheck) { c.Actions[0].Description += "x" },
		func(c *GithubReportCheck) { c.Actions[0].Identifier += "x" },
		func(c *GithubReportCheck) { c.Actions[0].Label = " " },
		func(c *GithubReportCheck) { c.Actions[0].Description = " " },
		func(c *GithubReportCheck) { c.Actions[0].Identifier = " " },
	} {
		payload.Check.Actions = []GithubReportCheckAction{action, action, action}
		mutate(payload.Check)
		_, err := CanonicalGithubReportCreatePayload(payload)
		require.ErrorIs(t, err, ErrGithubReportCreatePayload)
	}
}

func TestGithubReportCreateCorrelationBindsEffectAndDigest(t *testing.T) {
	id := uuid.New()
	digest := payloadSHA256([]byte("report"))
	first, err := GithubReportCreateCorrelation(id, digest)
	require.NoError(t, err)
	replay, err := GithubReportCreateCorrelation(id, digest)
	require.NoError(t, err)
	require.Equal(t, first, replay)
	other, err := GithubReportCreateCorrelation(uuid.New(), digest)
	require.NoError(t, err)
	require.NotEqual(t, first, other)
	other, err = GithubReportCreateCorrelation(id, payloadSHA256([]byte("other")))
	require.NoError(t, err)
	require.NotEqual(t, first, other)
	_, err = GithubReportCreateCorrelation(uuid.Nil, digest)
	require.ErrorIs(t, err, ErrGithubReportCreateCorrelation)
	for _, invalid := range []string{"", "digest", strings.ToUpper(digest), strings.Repeat("z", 64)} {
		_, err := GithubReportCreateCorrelation(id, invalid)
		require.ErrorIs(t, err, ErrGithubReportCreateCorrelation)
	}
}
