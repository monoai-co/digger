package controllers

import (
	"context"
	"errors"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/go-github/v61/github"
)

// observeClosedGithubLockOwners performs GET-only checks outside database locks.
// The submission transaction subsequently compares the exact observed lock rows.
func observeClosedGithubLockOwners(ctx context.Context, client *github.Client, target models.GithubDeliveryTargetIntent, owners []models.GithubSubmissionLockOwner) ([]models.GithubSubmissionLockOwner, error) {
	if len(owners) == 0 {
		return nil, nil
	}
	if client == nil || target.RepositoryID <= 0 {
		return nil, errors.New("closed lock recovery requires a selected repository and installation client")
	}
	client = githubClientWithoutRedirects(client)
	closed := make(map[int]bool)
	for _, owner := range owners {
		if owner.ID == 0 || owner.PullRequestNumber <= 0 || owner.PullRequestNumber == target.PullRequestNumber {
			return nil, models.ErrGithubSubmissionLockConflict
		}
		if closed[owner.PullRequestNumber] {
			continue
		}
		pr, _, err := client.PullRequests.Get(ctx, target.RepoOwner, target.RepoName, owner.PullRequestNumber)
		if err != nil {
			return nil, err
		}
		if pr == nil || pr.GetNumber() != owner.PullRequestNumber || pr.GetBase().GetRepo().GetID() != target.RepositoryID ||
			pr.GetBase().GetRepo().GetFullName() != target.RepoOwner+"/"+target.RepoName || pr.GetState() != "closed" {
			return nil, models.ErrGithubSubmissionLockConflict
		}
		closed[owner.PullRequestNumber] = true
	}
	return append([]models.GithubSubmissionLockOwner(nil), owners...), ctx.Err()
}
