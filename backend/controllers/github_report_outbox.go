package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
)

type githubReportCreateStore interface {
	PrepareGithubReportCreate(context.Context, uuid.UUID, string, string, int64) (*models.GithubReportCreatePreparation, error)
}

func dispatchGithubReportCreate(ctx context.Context, request OutboxDispatchRequest, store githubReportCreateStore, provider utils.ContextGithubClientProvider) (OutboxDispatchResult, error) {
	payload, err := models.DecodeGithubReportCreatePayload(request.Payload)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	canonical, err := models.CanonicalGithubReportCreatePayload(payload)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	// Authentication must succeed before the database consumes the only POST.
	client, _, err := provider.GetContext(ctx, payload.GithubAppID, payload.GithubInstallationID)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	if client == nil {
		return OutboxDispatchResult{}, ErrOutboxDispatcherMisconfigured
	}
	client = githubClientWithoutRedirects(client)
	preparation, err := store.PrepareGithubReportCreate(ctx, request.EffectID, request.LeaseID, request.DatabaseIdentity, request.WriterEpoch)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	if preparation == nil || preparation.AttemptedAt.IsZero() {
		return OutboxDispatchResult{}, models.ErrGithubReportCreateConflict
	}
	prepared, err := models.CanonicalGithubReportCreatePayload(preparation.Payload)
	if err != nil || !bytes.Equal(prepared, canonical) {
		return OutboxDispatchResult{}, models.ErrGithubReportCreateConflict
	}
	digestBytes := sha256.Sum256(canonical)
	digest := hex.EncodeToString(digestBytes[:])
	correlation, err := models.GithubReportCreateCorrelation(request.EffectID, digest)
	if err != nil || preparation.Correlation != correlation {
		return OutboxDispatchResult{}, models.ErrGithubReportCreateConflict
	}
	if preparation.Receipt != nil {
		receipt := preparation.Receipt
		expectedURL, err := models.GithubReportProviderURL(payload, receipt.ProviderID)
		if err != nil || preparation.MayCreate || receipt.EffectID != request.EffectID || receipt.PayloadSHA256 != digest ||
			receipt.ResourceKind != payload.ResourceKind || receipt.ProviderURL != expectedURL {
			return OutboxDispatchResult{}, models.ErrGithubReportCreateConflict
		}
		return githubReportDispatchReceipt(request.EffectID, digest, payload, receipt.ProviderID)
	}
	if preparation.MayCreate {
		if payload.ResourceKind == models.GithubReportResourceComment {
			comment, _, err := client.Issues.CreateComment(ctx, payload.RepoOwner, payload.RepoName, payload.PullRequestNumber,
				&github.IssueComment{Body: github.String(githubReportCommentBody(payload.Body, correlation))})
			if err != nil || comment == nil || comment.GetID() <= 0 || comment.GetBody() != githubReportCommentBody(payload.Body, correlation) ||
				!githubReportCommentAuthor(ctx, client, comment, payload.GithubAppID) {
				return githubReportRetry(), nil
			}
			return githubReportDispatchReceipt(request.EffectID, digest, payload, comment.GetID())
		}
		run, _, err := client.Checks.CreateCheckRun(ctx, payload.RepoOwner, payload.RepoName, githubReportCheckOptions(payload, correlation, preparation.AttemptedAt))
		if err != nil || !githubReportCheckMatches(run, payload, correlation) {
			return githubReportRetry(), nil
		}
		return githubReportDispatchReceipt(request.EffectID, digest, payload, run.GetID())
	}
	var providerID int64
	if payload.ResourceKind == models.GithubReportResourceComment {
		providerID = reconcileGithubReportComment(ctx, client, payload, correlation)
	} else {
		providerID = reconcileGithubReportCheck(ctx, client, payload, correlation)
	}
	if providerID <= 0 {
		return githubReportRetry(), nil
	}
	return githubReportDispatchReceipt(request.EffectID, digest, payload, providerID)
}

func githubReportRetry() OutboxDispatchResult { return OutboxDispatchResult{RetryAfter: time.Minute} }

func githubReportCommentBody(body, correlation string) string {
	return body + "\n\n<!-- " + correlation + " -->"
}

func githubReportCheckOptions(payload models.GithubReportCreatePayload, correlation string, attemptedAt time.Time) github.CreateCheckRunOptions {
	check := payload.Check
	options := github.CreateCheckRunOptions{Name: check.Name, HeadSHA: payload.HeadSHA, ExternalID: github.String(correlation),
		Status: github.String(check.Status), StartedAt: &github.Timestamp{Time: attemptedAt.UTC()},
		Output: &github.CheckRunOutput{Title: github.String(check.Title), Summary: github.String(check.Summary), Text: github.String(check.Text)}}
	if check.Status == "completed" {
		options.Conclusion = github.String(check.Conclusion)
		options.CompletedAt = &github.Timestamp{Time: attemptedAt.UTC()}
	}
	for _, action := range check.Actions {
		options.Actions = append(options.Actions, &github.CheckRunAction{Label: action.Label, Description: action.Description, Identifier: action.Identifier})
	}
	return options
}

func githubReportCheckMatches(run *github.CheckRun, payload models.GithubReportCreatePayload, correlation string) bool {
	// GitHub's read response omits actions. Their original intent is bound by
	// the digest in ExternalID; mutable provider actions cannot be reverified.
	return run != nil && run.GetID() > 0 && payload.Check != nil && run.GetApp().GetID() == payload.GithubAppID &&
		run.GetExternalID() == correlation && run.GetHeadSHA() == payload.HeadSHA && run.GetName() == payload.Check.Name &&
		run.GetStatus() == payload.Check.Status && run.GetConclusion() == payload.Check.Conclusion && run.Output != nil &&
		run.Output.GetTitle() == payload.Check.Title && run.Output.GetSummary() == payload.Check.Summary && run.Output.GetText() == payload.Check.Text
}

func reconcileGithubReportComment(ctx context.Context, client *github.Client, payload models.GithubReportCreatePayload, correlation string) int64 {
	marker := "<!-- " + correlation + " -->"
	expectedBody := githubReportCommentBody(payload.Body, correlation)
	var selected int64
	page := 1
	for pages := 0; pages < 100; pages++ {
		comments, response, err := client.Issues.ListComments(ctx, payload.RepoOwner, payload.RepoName, payload.PullRequestNumber,
			&github.IssueListCommentsOptions{ListOptions: github.ListOptions{Page: page, PerPage: 100}})
		if err != nil || response == nil {
			return 0
		}
		for _, comment := range comments {
			if comment == nil || !strings.Contains(comment.GetBody(), marker) {
				continue
			}
			if selected != 0 || comment.GetID() <= 0 || comment.GetBody() != expectedBody || !githubReportCommentAuthor(ctx, client, comment, payload.GithubAppID) {
				return 0
			}
			selected = comment.GetID()
		}
		if response.NextPage == 0 {
			return selected
		}
		if response.NextPage <= page {
			return 0
		}
		page = response.NextPage
	}
	return 0
}

func githubReportCommentAuthor(ctx context.Context, client *github.Client, comment *github.IssueComment, appID int64) bool {
	user := comment.GetUser()
	if user.GetType() != "Bot" || !strings.HasSuffix(user.GetLogin(), "[bot]") {
		return false
	}
	slug := strings.TrimSuffix(user.GetLogin(), "[bot]")
	if slug == "" || strings.ContainsAny(slug, "/\\?#%") {
		return false
	}
	app, _, err := client.Apps.Get(ctx, slug)
	return err == nil && app != nil && app.GetID() == appID && app.GetSlug() == slug
}

func reconcileGithubReportCheck(ctx context.Context, client *github.Client, payload models.GithubReportCreatePayload, correlation string) int64 {
	var selected int64
	page := 1
	for pages := 0; pages < 100; pages++ {
		runs, response, err := client.Checks.ListCheckRunsForRef(ctx, payload.RepoOwner, payload.RepoName, url.PathEscape(payload.HeadSHA),
			&github.ListCheckRunsOptions{Filter: github.String("all"), ListOptions: github.ListOptions{Page: page, PerPage: 100}})
		if err != nil || response == nil || runs == nil {
			return 0
		}
		for _, run := range runs.CheckRuns {
			if run == nil || run.GetExternalID() != correlation {
				continue
			}
			if selected != 0 || !githubReportCheckMatches(run, payload, correlation) {
				return 0
			}
			selected = run.GetID()
		}
		if response.NextPage == 0 {
			return selected
		}
		if response.NextPage <= page {
			return 0
		}
		page = response.NextPage
	}
	return 0
}

func githubReportDispatchReceipt(effectID uuid.UUID, digest string, payload models.GithubReportCreatePayload, providerID int64) (OutboxDispatchResult, error) {
	providerURL, err := models.GithubReportProviderURL(payload, providerID)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	receipt, err := json.Marshal(models.GithubReportCreateReceipt{EffectID: effectID, PayloadSHA256: digest,
		ResourceKind: payload.ResourceKind, ProviderID: providerID, ProviderURL: providerURL})
	return OutboxDispatchResult{ProviderReceipt: receipt}, err
}
