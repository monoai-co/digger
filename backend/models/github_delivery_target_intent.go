package models

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
)

var ErrGithubDeliveryTargetIntent = errors.New("github delivery target is invalid")
var ErrGithubDeliveryTargetUnsupported = errors.New("github delivery target source is not supported")

type GithubDeliveryTargetSource string

const (
	GithubDeliveryTargetSignedPullRequest  GithubDeliveryTargetSource = "pull_request"
	GithubDeliveryTargetIssueCommentLookup GithubDeliveryTargetSource = "issue_comment_pr_lookup"
	GithubDeliveryTargetLegacyCheckAction  GithubDeliveryTargetSource = "legacy_check_action"
)

type GithubDeliveryTargetIntent struct {
	RepositoryID      int64                      `json:"repository_id"`
	RepoOwner         string                     `json:"repo_owner"`
	RepoName          string                     `json:"repo_name"`
	PullRequestNumber int                        `json:"pull_request_number"`
	HeadSHA           string                     `json:"head_sha"`
	HeadRef           string                     `json:"head_ref"`
	Source            GithubDeliveryTargetSource `json:"source"`
}

type GithubDeliveryTargetLookup struct {
	GithubAppID          int64
	GithubInstallationID int64
	RepositoryID         int64
	RepoOwner            string
	RepoName             string
	PullRequestNumber    int
}

// GithubDeliveryTargetPreparation keeps the authenticated selection private.
// Lookup returns a detached routing value, never authority to change the target.
type GithubDeliveryTargetPreparation struct {
	target         GithubDeliveryTargetIntent
	appID          int64
	installationID int64
	checkAction    *github.CheckRunEvent
}

// PrepareGithubDeliveryTargetIntent consumes an already authenticated inbox
// receipt. It validates its stored identity; it does not authenticate raw input.
func PrepareGithubDeliveryTargetIntent(delivery *GithubWebhookDelivery) (*GithubDeliveryTargetPreparation, error) {
	if delivery == nil || delivery.GithubAppID <= 0 || delivery.InstallationID == nil || *delivery.InstallationID <= 0 ||
		strings.TrimSpace(delivery.DeliveryID) == "" || !utf8.Valid(delivery.Payload) || delivery.PayloadSHA256 != payloadSHA256(delivery.Payload) {
		return nil, ErrGithubDeliveryTargetIntent
	}
	expected, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
	if err != nil || delivery.OperationID != expected.String() {
		return nil, ErrGithubDeliveryTargetIntent
	}
	if delivery.EventType != "pull_request" && delivery.EventType != "issue_comment" && delivery.EventType != "check_run" {
		// Other event classes do not select a PR execution target.
		return nil, ErrGithubDeliveryTargetUnsupported
	}
	event, err := github.ParseWebHook(delivery.EventType, delivery.Payload)
	if err != nil {
		return nil, ErrGithubDeliveryTargetIntent
	}
	var repository *github.Repository
	var installation *github.Installation
	preparation := &GithubDeliveryTargetPreparation{appID: delivery.GithubAppID, installationID: *delivery.InstallationID}
	var signedPR *github.PullRequest
	var signedPRLink string
	switch event := event.(type) {
	case *github.PullRequestEvent:
		repository, installation, signedPR = event.GetRepo(), event.GetInstallation(), event.GetPullRequest()
		preparation.target.Source = GithubDeliveryTargetSignedPullRequest
		preparation.target.PullRequestNumber = signedPR.GetNumber()
		if event.Number != nil && event.GetNumber() != preparation.target.PullRequestNumber {
			return nil, ErrGithubDeliveryTargetIntent
		}
	case *github.IssueCommentEvent:
		repository, installation = event.GetRepo(), event.GetInstallation()
		if event.Issue == nil || !event.Issue.IsPullRequest() {
			return nil, ErrGithubDeliveryTargetIntent
		}
		preparation.target.Source = GithubDeliveryTargetIssueCommentLookup
		preparation.target.PullRequestNumber = event.Issue.GetNumber()
		signedPRLink = event.Issue.PullRequestLinks.GetURL()
	case *github.CheckRunEvent:
		if event.GetAction() != "requested_action" || event.GetRequestedAction() == nil ||
			!strings.HasPrefix(event.GetRequestedAction().Identifier, "abatch:") {
			return nil, ErrGithubDeliveryTargetUnsupported
		}
		if event.GetCheckRun().GetID() <= 0 || event.GetCheckRun().GetApp().GetID() != delivery.GithubAppID || !validGithubReportPathSegment(event.GetCheckRun().GetHeadSHA()) {
			return nil, ErrGithubDeliveryTargetIntent
		}
		repository, installation = event.GetRepo(), event.GetInstallation()
		preparation.target.Source = GithubDeliveryTargetLegacyCheckAction
		preparation.target.HeadSHA = event.GetCheckRun().GetHeadSHA()
		preparation.checkAction = event
	default:
		return nil, ErrGithubDeliveryTargetUnsupported
	}
	if repository == nil || repository.GetID() <= 0 || installation.GetID() != preparation.installationID ||
		(preparation.target.Source != GithubDeliveryTargetLegacyCheckAction && preparation.target.PullRequestNumber <= 0) ||
		!validGithubReportPathSegment(repository.GetOwner().GetLogin()) || !validGithubReportPathSegment(repository.GetName()) ||
		repository.GetFullName() != repository.GetOwner().GetLogin()+"/"+repository.GetName() || repository.GetFullName() != delivery.RepositoryFullName {
		return nil, ErrGithubDeliveryTargetIntent
	}
	preparation.target.RepositoryID, preparation.target.RepoOwner, preparation.target.RepoName = repository.GetID(), repository.GetOwner().GetLogin(), repository.GetName()
	if preparation.target.Source == GithubDeliveryTargetIssueCommentLookup && !validGithubDeliveryPRLink(signedPRLink, preparation.target) {
		return nil, ErrGithubDeliveryTargetIntent
	}
	if preparation.target.Source == GithubDeliveryTargetSignedPullRequest {
		resolved, err := preparation.resolvePR(signedPR)
		if err != nil {
			return nil, err
		}
		preparation.target = resolved
	}
	return preparation, nil
}

func validGithubDeliveryPRLink(link string, target GithubDeliveryTargetIntent) bool {
	parsed, err := url.Parse(link)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" {
		return false
	}
	expectedPath := fmt.Sprintf("/repos/%s/%s/pulls/%d", target.RepoOwner, target.RepoName, target.PullRequestNumber)
	// The link is a signed PR marker, never a request destination. The
	// configured installation client owns the API origin and prefix.
	return parsed.Path == expectedPath || parsed.Path == "/api/v3"+expectedPath
}

func (preparation *GithubDeliveryTargetPreparation) PullRequestLookup() *GithubDeliveryTargetLookup {
	if preparation == nil || preparation.target.Source != GithubDeliveryTargetIssueCommentLookup {
		return nil
	}
	return &GithubDeliveryTargetLookup{GithubAppID: preparation.appID, GithubInstallationID: preparation.installationID,
		RepositoryID: preparation.target.RepositoryID, RepoOwner: preparation.target.RepoOwner, RepoName: preparation.target.RepoName, PullRequestNumber: preparation.target.PullRequestNumber}
}

// Resolve validates a provider observation for an issue comment, or returns the
// signed PR target without consulting the provider's potentially newer head.
// Persistence must make the first selection immutable before preparing work.
func (preparation *GithubDeliveryTargetPreparation) Resolve(pullRequest *github.PullRequest) (GithubDeliveryTargetIntent, error) {
	if preparation == nil {
		return GithubDeliveryTargetIntent{}, ErrGithubDeliveryTargetIntent
	}
	switch preparation.target.Source {
	case GithubDeliveryTargetSignedPullRequest:
		if pullRequest != nil {
			return GithubDeliveryTargetIntent{}, ErrGithubDeliveryTargetIntent
		}
		return preparation.target, nil
	case GithubDeliveryTargetIssueCommentLookup:
		return preparation.resolvePR(pullRequest)
	default:
		return GithubDeliveryTargetIntent{}, ErrGithubDeliveryTargetUnsupported
	}
}

// ValidateIntent rechecks the selected value against the accepted delivery.
// An issue comment has no signed head: its head/ref are a trusted controller
// observation, validated by Resolve before the first immutable write.
// A legacy check action binds its PR/ref through the saved batch on first write.
func (preparation *GithubDeliveryTargetPreparation) ValidateIntent(intent GithubDeliveryTargetIntent) error {
	if preparation == nil {
		return ErrGithubDeliveryTargetIntent
	}
	expected := preparation.target
	switch expected.Source {
	case GithubDeliveryTargetSignedPullRequest:
	case GithubDeliveryTargetIssueCommentLookup:
		if !utf8.ValidString(intent.HeadSHA) || !validGithubReportPathSegment(intent.HeadSHA) || !validGithubDeliveryHeadRef(intent.HeadRef) {
			return ErrGithubDeliveryTargetIntent
		}
		expected.HeadSHA, expected.HeadRef = intent.HeadSHA, intent.HeadRef
	case GithubDeliveryTargetLegacyCheckAction:
		if intent.PullRequestNumber <= 0 || !validGithubDeliveryHeadRef(intent.HeadRef) {
			return ErrGithubDeliveryTargetIntent
		}
		expected.PullRequestNumber, expected.HeadRef = intent.PullRequestNumber, intent.HeadRef
	default:
		return ErrGithubDeliveryTargetUnsupported
	}
	if expected != intent {
		return ErrGithubDeliveryTargetIntent
	}
	return nil
}

func (preparation *GithubDeliveryTargetPreparation) resolvePR(pullRequest *github.PullRequest) (GithubDeliveryTargetIntent, error) {
	target := preparation.target
	if pullRequest == nil || pullRequest.GetNumber() != target.PullRequestNumber || pullRequest.GetBase().GetRepo().GetID() != target.RepositoryID ||
		pullRequest.GetBase().GetRepo().GetFullName() != target.RepoOwner+"/"+target.RepoName ||
		!validGithubReportPathSegment(pullRequest.GetHead().GetSHA()) || !validGithubDeliveryHeadRef(pullRequest.GetHead().GetRef()) {
		return GithubDeliveryTargetIntent{}, ErrGithubDeliveryTargetIntent
	}
	// The head repository may be a fork; only the base identifies the PR tenant.
	target.HeadSHA, target.HeadRef = pullRequest.GetHead().GetSHA(), pullRequest.GetHead().GetRef()
	return target, nil
}

func validGithubDeliveryHeadRef(ref string) bool {
	if ref == "" || ref != strings.TrimSpace(ref) || !utf8.ValidString(ref) {
		return false
	}
	for _, character := range ref {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
