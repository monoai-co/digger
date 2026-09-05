package controllers

import (
	"context"
	"errors"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
)

// resolveGithubDeliveryTarget reads only the PR identified by the accepted
// delivery. Its result must be persisted before preparing reports or jobs.
func resolveGithubDeliveryTarget(ctx context.Context, delivery *models.GithubWebhookDelivery, provider utils.ContextGithubClientProvider) (models.GithubDeliveryTargetIntent, error) {
	preparation, err := models.PrepareGithubDeliveryTargetIntent(delivery)
	if err != nil {
		return models.GithubDeliveryTargetIntent{}, err
	}
	lookup := preparation.PullRequestLookup()
	if lookup == nil {
		return preparation.Resolve(nil)
	}
	if provider == nil {
		return models.GithubDeliveryTargetIntent{}, errors.New("GitHub target resolution requires an installation client")
	}
	client, _, err := provider.GetContext(ctx, lookup.GithubAppID, lookup.GithubInstallationID)
	if err != nil {
		return models.GithubDeliveryTargetIntent{}, err
	}
	if client == nil {
		return models.GithubDeliveryTargetIntent{}, errors.New("GitHub target resolution returned no installation client")
	}
	client = githubClientWithoutRedirects(client)
	pullRequest, _, err := client.PullRequests.Get(ctx, lookup.RepoOwner, lookup.RepoName, lookup.PullRequestNumber)
	if err != nil {
		return models.GithubDeliveryTargetIntent{}, err
	}
	return preparation.Resolve(pullRequest)
}
