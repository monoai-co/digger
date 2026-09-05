package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type durableRunReconciliationStore interface {
	PrepareDurableRunReconciliation(context.Context, uuid.UUID, string, string, int64) (*models.DurableRunReconciliationPreparation, error)
	ReconcileDurableWorkflowRun(context.Context, uuid.UUID, string, models.DurableRunObservation, string, int64) (bool, error)
}

func reconcileGithubWorkflowRun(ctx context.Context, request OutboxDispatchRequest, store durableRunReconciliationStore, provider utils.ContextGithubClientProvider) (OutboxDispatchResult, error) {
	prepared, err := store.PrepareDurableRunReconciliation(ctx, request.EffectID, request.LeaseID, request.DatabaseIdentity, request.WriterEpoch)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	if prepared == nil {
		return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
	}
	if prepared.Terminal {
		receipt := prepared.PreviousReceipt
		if len(receipt) == 0 {
			receipt = []byte(`{"terminal_noop":true}`)
		}
		return OutboxDispatchResult{ProviderReceipt: receipt}, nil
	}
	// Provider loss is uncertainty, not evidence of an executor's termination.
	// Keep its canonical identity and let the model preserve valid callbacks or
	// create explicit recovery once their acceptance window expires.
	observation := prepared.CanonicalObservation
	observation.Status = "unavailable"
	client, _, providerErr := provider.GetContext(ctx, prepared.GithubAppID, prepared.InstallationID)
	if providerErr != nil && !githubRunAuthoritativelyUnavailable(providerErr) {
		return durableRunReconciliationRetry(), nil
	}
	if providerErr == nil {
		if client == nil {
			return durableRunReconciliationRetry(), nil
		}
		run, _, lookupErr := client.Actions.GetWorkflowRunByID(ctx, prepared.RepoOwner, prepared.RepoName, prepared.RunID)
		if lookupErr != nil && !githubRunAuthoritativelyUnavailable(lookupErr) {
			return durableRunReconciliationRetry(), nil
		}
		if lookupErr == nil {
			if run == nil {
				return durableRunReconciliationRetry(), nil
			}
			observation = models.DurableRunObservation{RepositoryID: run.GetRepository().GetID(), WorkflowID: run.GetWorkflowID(), RunID: run.GetID(), RunAttempt: run.GetRunAttempt(), HeadSHA: run.GetHeadSHA(), Status: run.GetStatus(), Conclusion: run.GetConclusion()}
		}
	}
	terminal, err := store.ReconcileDurableWorkflowRun(ctx, request.EffectID, request.LeaseID, observation, request.DatabaseIdentity, request.WriterEpoch)
	if errors.Is(err, models.ErrDurableRunQuarantined) {
		return durableRunReconciliationRetry(), nil
	}
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	if !terminal {
		return durableRunReconciliationRetry(), nil
	}
	receipt, err := json.Marshal(observation)
	return OutboxDispatchResult{ProviderReceipt: receipt}, err
}

func durableRunReconciliationRetry() OutboxDispatchResult {
	return OutboxDispatchResult{RetryAfter: time.Minute}
}

func githubRunAuthoritativelyUnavailable(err error) bool {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}
	var responseError *github.ErrorResponse
	return errors.As(err, &responseError) && responseError.Response != nil && responseError.Response.StatusCode == http.StatusNotFound
}
