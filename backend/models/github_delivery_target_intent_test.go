package models

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func deliveryTargetRepository() *github.Repository {
	return &github.Repository{ID: github.Int64(100), Name: github.String("repo"), FullName: github.String("owner/repo"), Owner: &github.User{Login: github.String("owner")}}
}

func deliveryTargetPR() *github.PullRequest {
	return &github.PullRequest{Number: github.Int(42), Base: &github.PullRequestBranch{Repo: deliveryTargetRepository()},
		Head: &github.PullRequestBranch{SHA: github.String("abc123"), Ref: github.String("feature/branch"), Repo: &github.Repository{ID: github.Int64(200), FullName: github.String("fork/repo")}}}
}

func deliveryTargetFixture(t *testing.T, eventType string, event any) *GithubWebhookDelivery {
	t.Helper()
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	installation := int64(20)
	delivery := &GithubWebhookDelivery{DeliveryID: "target-delivery", EventType: eventType, GithubAppID: 10, InstallationID: &installation, RepositoryFullName: "owner/repo", Payload: raw, PayloadSHA256: payloadSHA256(raw)}
	id, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
	require.NoError(t, err)
	delivery.OperationID = id.String()
	return delivery
}

func deliveryTargetSignedFixture(t *testing.T) *GithubWebhookDelivery {
	return deliveryTargetFixture(t, "pull_request", &github.PullRequestEvent{Number: github.Int(42), PullRequest: deliveryTargetPR(), Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}})
}

func deliveryTargetCommentFixture(t *testing.T) *GithubWebhookDelivery {
	return deliveryTargetFixture(t, "issue_comment", &github.IssueCommentEvent{Issue: &github.Issue{Number: github.Int(42), PullRequestLinks: &github.PullRequestLinks{URL: github.String("https://api.github.com/repos/owner/repo/pulls/42")}}, Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}})
}

func TestGithubDeliveryTargetPreservesComparisonBase(t *testing.T) {
	pr := deliveryTargetPR()
	pr.Base.SHA, pr.Base.Ref = github.String("base-commit"), github.String("main")
	preparation, err := PrepareGithubDeliveryTargetIntent(deliveryTargetCommentFixture(t))
	require.NoError(t, err)
	target, err := preparation.Resolve(pr)
	require.NoError(t, err)
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"base_sha":"base-commit"`)
	require.Contains(t, string(raw), `"base_ref":"main"`)
	require.NoError(t, preparation.ValidateIntent(target))
	decoded, err := DecodeGithubDeliveryTarget(raw)
	require.NoError(t, err)
	require.Equal(t, target, decoded)
	signed := deliveryTargetFixture(t, "pull_request", &github.PullRequestEvent{Number: github.Int(42), PullRequest: pr, Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}})
	signedPreparation, err := PrepareGithubDeliveryTargetIntent(signed)
	require.NoError(t, err)
	signedTarget, err := signedPreparation.Resolve(nil)
	require.NoError(t, err)
	require.Equal(t, "base-commit", signedTarget.BaseSHA)
	signedTarget.BaseSHA = "new-base"
	require.ErrorIs(t, signedPreparation.ValidateIntent(signedTarget), ErrGithubDeliveryTargetIntent)
	for _, mutate := range []func(*github.PullRequest){
		func(pr *github.PullRequest) { pr.Base.SHA = github.String("") },
		func(pr *github.PullRequest) { pr.Base.Ref = github.String("") },
		func(pr *github.PullRequest) { pr.Base.Ref = github.String("main\x00") },
	} {
		pr.Base.SHA, pr.Base.Ref = github.String("base-commit"), github.String("main")
		mutate(pr)
		_, err := preparation.Resolve(pr)
		require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
	}
}

func TestGithubDeliveryTargetSignedPRAllowsForkAndRemainsDetached(t *testing.T) {
	delivery := deliveryTargetSignedFixture(t)
	preparation, err := PrepareGithubDeliveryTargetIntent(delivery)
	require.NoError(t, err)
	require.Nil(t, preparation.PullRequestLookup())
	target, err := preparation.Resolve(nil)
	require.NoError(t, err)
	require.Equal(t, GithubDeliveryTargetIntent{RepositoryID: 100, RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 42, HeadSHA: "abc123", HeadRef: "feature/branch", Source: GithubDeliveryTargetSignedPullRequest}, target)
	delivery.Payload = []byte("changed")
	target.HeadSHA = "changed"
	replayed, err := preparation.Resolve(nil)
	require.NoError(t, err)
	require.Equal(t, "abc123", replayed.HeadSHA)
	_, err = preparation.Resolve(deliveryTargetPR())
	require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
}

func TestGithubDeliveryTargetIssueCommentRequiresExactResolvedPR(t *testing.T) {
	preparation, err := PrepareGithubDeliveryTargetIntent(deliveryTargetCommentFixture(t))
	require.NoError(t, err)
	lookup := preparation.PullRequestLookup()
	require.Equal(t, &GithubDeliveryTargetLookup{GithubAppID: 10, GithubInstallationID: 20, RepositoryID: 100, RepoOwner: "owner", RepoName: "repo", PullRequestNumber: 42}, lookup)
	lookup.RepoName = "other"
	lookup.PullRequestNumber = 99
	require.Equal(t, "repo", preparation.PullRequestLookup().RepoName)
	require.Equal(t, 42, preparation.PullRequestLookup().PullRequestNumber)
	_, err = preparation.Resolve(nil)
	require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
	target, err := preparation.Resolve(deliveryTargetPR())
	require.NoError(t, err)
	require.Equal(t, GithubDeliveryTargetIssueCommentLookup, target.Source)
	require.Equal(t, "abc123", target.HeadSHA)
	for name, mutate := range map[string]func(*github.PullRequest){
		"wrong PR":        func(pr *github.PullRequest) { pr.Number = github.Int(99) },
		"missing number":  func(pr *github.PullRequest) { pr.Number = nil },
		"wrong base ID":   func(pr *github.PullRequest) { pr.Base.Repo.ID = github.Int64(999) },
		"wrong base name": func(pr *github.PullRequest) { pr.Base.Repo.FullName = github.String("other/repo") },
		"missing base":    func(pr *github.PullRequest) { pr.Base = nil },
		"missing head":    func(pr *github.PullRequest) { pr.Head = nil },
		"blank SHA":       func(pr *github.PullRequest) { pr.Head.SHA = github.String(" ") },
		"ambiguous SHA":   func(pr *github.PullRequest) { pr.Head.SHA = github.String("../head") },
		"blank ref":       func(pr *github.PullRequest) { pr.Head.Ref = github.String("") },
		"padded ref":      func(pr *github.PullRequest) { pr.Head.Ref = github.String(" branch") },
		"control ref":     func(pr *github.PullRequest) { pr.Head.Ref = github.String("branch\x00name") },
	} {
		t.Run(name, func(t *testing.T) {
			pr := deliveryTargetPR()
			mutate(pr)
			_, err := preparation.Resolve(pr)
			require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
		})
	}
}

func TestGithubDeliveryTargetRejectsReceiptAndSignedMetadataMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*GithubWebhookDelivery){
		"digest":               func(d *GithubWebhookDelivery) { d.PayloadSHA256 = "wrong" },
		"operation":            func(d *GithubWebhookDelivery) { d.OperationID = "wrong" },
		"app":                  func(d *GithubWebhookDelivery) { d.GithubAppID = 0 },
		"changed app":          func(d *GithubWebhookDelivery) { d.GithubAppID = 99 },
		"installation":         func(d *GithubWebhookDelivery) { d.InstallationID = github.Int64(99) },
		"missing installation": func(d *GithubWebhookDelivery) { d.InstallationID = nil },
		"repo":                 func(d *GithubWebhookDelivery) { d.RepositoryFullName = "other/repo" },
		"malformed JSON":       func(d *GithubWebhookDelivery) { d.Payload = []byte("{"); d.PayloadSHA256 = payloadSHA256(d.Payload) },
	} {
		t.Run(name, func(t *testing.T) {
			d := deliveryTargetSignedFixture(t)
			mutate(d)
			_, err := PrepareGithubDeliveryTargetIntent(d)
			require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
		})
	}
	for name, mutate := range map[string]func(*github.PullRequestEvent){
		"event number":        func(e *github.PullRequestEvent) { e.Number = github.Int(99) },
		"base repository":     func(e *github.PullRequestEvent) { e.PullRequest.Base.Repo.ID = github.Int64(999) },
		"signed installation": func(e *github.PullRequestEvent) { e.Installation.ID = github.Int64(999) },
		"missing PR":          func(e *github.PullRequestEvent) { e.PullRequest = nil },
		"missing repo":        func(e *github.PullRequestEvent) { e.Repo = nil },
		"missing owner":       func(e *github.PullRequestEvent) { e.Repo.Owner = nil },
		"path ambiguity":      func(e *github.PullRequestEvent) { e.Repo.Name = github.String("../repo") },
	} {
		t.Run(name, func(t *testing.T) {
			e := &github.PullRequestEvent{Number: github.Int(42), PullRequest: deliveryTargetPR(), Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}}
			mutate(e)
			_, err := PrepareGithubDeliveryTargetIntent(deliveryTargetFixture(t, "pull_request", e))
			require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
		})
	}
}

func TestGithubDeliveryTargetRejectsNonPRCommentAndCheckActionAssociations(t *testing.T) {
	comment := &github.IssueCommentEvent{Issue: &github.Issue{Number: github.Int(42)}, Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}}
	_, err := PrepareGithubDeliveryTargetIntent(deliveryTargetFixture(t, "issue_comment", comment))
	require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
	for _, link := range []string{"", "https://api.github.com/repos/owner/repo/pulls/99", "https://api.github.com/repos/other/repo/pulls/42", "http://other.example/repos/owner/repo/pulls/42", "https://user@api.github.com/repos/owner/repo/pulls/42", "https://api.github.com/repos/owner/repo/pulls/42?other=1", "https://api.github.com/repos/owner/repo/pulls/42#other"} {
		comment.Issue.PullRequestLinks = &github.PullRequestLinks{URL: github.String(link)}
		_, err := PrepareGithubDeliveryTargetIntent(deliveryTargetFixture(t, "issue_comment", comment))
		require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
	}
	check := &github.CheckRunEvent{Action: github.String("requested_action"), Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}, CheckRun: &github.CheckRun{HeadSHA: github.String("abc123"), PullRequests: []*github.PullRequest{deliveryTargetPR()}}}
	_, err = PrepareGithubDeliveryTargetIntent(deliveryTargetFixture(t, "check_run", check))
	require.ErrorIs(t, err, ErrGithubDeliveryTargetUnsupported)
	_, err = PrepareGithubDeliveryTargetIntent(nil)
	require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
	var preparation *GithubDeliveryTargetPreparation
	require.Nil(t, preparation.PullRequestLookup())
	_, err = preparation.Resolve(nil)
	require.ErrorIs(t, err, ErrGithubDeliveryTargetIntent)
}

func TestGithubDeliveryTargetPRLinkOriginIsNotLookupAuthority(t *testing.T) {
	for _, link := range []string{"https://enterprise.example/api/v3/repos/owner/repo/pulls/42", "https://another.example/repos/owner/repo/pulls/42"} {
		comment := &github.IssueCommentEvent{Issue: &github.Issue{Number: github.Int(42), PullRequestLinks: &github.PullRequestLinks{URL: github.String(link)}}, Repo: deliveryTargetRepository(), Installation: &github.Installation{ID: github.Int64(20)}}
		preparation, err := PrepareGithubDeliveryTargetIntent(deliveryTargetFixture(t, "issue_comment", comment))
		require.NoError(t, err)
		lookup := preparation.PullRequestLookup()
		require.Equal(t, "owner", lookup.RepoOwner)
		require.Equal(t, "repo", lookup.RepoName)
		require.Equal(t, 42, lookup.PullRequestNumber)
	}
}
