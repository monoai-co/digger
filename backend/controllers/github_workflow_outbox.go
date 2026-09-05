package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
)

const DefaultDurableJobTokenValidity = 31 * 24 * time.Hour

var ErrGithubWorkflowOutboxDispatch = errors.New("GitHub workflow outbox dispatch is misconfigured")

type durableJobDispatchStore interface {
	PrepareDurableJobDispatch(context.Context, uuid.UUID, string, time.Duration, time.Duration, string, int64) (*models.DurableJobDispatchPreparation, error)
}

func NewGithubWorkflowOutboxDispatch(
	store durableJobDispatchStore,
	githubClientProvider utils.ContextGithubClientProvider,
	tokenValidity time.Duration,
) (OutboxDispatchFunc, error) {
	if store == nil || githubClientProvider == nil || tokenValidity <= 0 {
		return nil, ErrGithubWorkflowOutboxDispatch
	}
	return func(ctx context.Context, request OutboxDispatchRequest) (OutboxDispatchResult, error) {
		if request.EffectKind != models.GithubWorkflowDispatchEffectKind {
			return OutboxDispatchResult{}, fmt.Errorf("unsupported outbox effect kind %q", request.EffectKind)
		}
		if request.LeaseDuration <= 0 {
			return OutboxDispatchResult{}, ErrGithubWorkflowOutboxDispatch
		}
		preparation, err := store.PrepareDurableJobDispatch(
			ctx,
			request.EffectID,
			request.LeaseID,
			tokenValidity,
			request.LeaseDuration,
			request.DatabaseIdentity,
			request.WriterEpoch,
		)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		if preparation == nil || preparation.Job == nil || preparation.Job.Batch == nil {
			return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
		}
		if preparation.Job.OperationID == nil || preparation.Job.WriterEpoch == nil || request.OperationID != *preparation.Job.OperationID {
			return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
		}
		job := preparation.Job
		batch := job.Batch
		if preparation.SkipProvider {
			return durableWorkflowDispatchReceipt(*preparation.Job.OperationID, true, "", nil, time.Time{})
		}
		if preparation.ClaimExpiresAt.IsZero() {
			return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
		}
		client, _, err := githubClientProvider.GetContext(ctx, preparation.GithubAppID, batch.GithubInstallationId)
		if err != nil {
			return OutboxDispatchResult{}, fmt.Errorf("create GitHub workflow client: %w", err)
		}
		backend := ci_backends.GithubActionCi{Client: client}
		workflowSpec, err := services.GetSpecFromJob(*job)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		workflowSpec.OperationID = *preparation.Job.OperationID
		workflowSpec.ProtocolVersion = job.ProtocolVersion
		workflowSpec.WriterEpoch = *job.WriterEpoch
		workflowSpec.ClaimExpiresAt = &preparation.ClaimExpiresAt
		runName, err := services.GetRunNameFromJob(*job)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		durableRunName := durableWorkflowRunName(*runName, *preparation.Job.OperationID)
		target, err := backend.ResolveDurableWorkflowTarget(ctx, *workflowSpec)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		controlRef := target.ControlRef
		details, err := backend.TriggerWorkflowContextAtRefWithRunDetails(ctx, *workflowSpec, durableRunName, "", controlRef, target.WorkflowID)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		run, _, err := client.Actions.GetWorkflowRunByID(ctx, workflowSpec.VCS.RepoOwner, workflowSpec.VCS.RepoName, details.RunID)
		if err != nil {
			return OutboxDispatchResult{}, fmt.Errorf("%w: load dispatched workflow run: %v", ci_backends.ErrWorkflowDispatchAcceptanceAmbiguous, err)
		}
		if !sameDurableWorkflowRun(run, details.RunID, target) {
			return OutboxDispatchResult{}, fmt.Errorf("%w: returned workflow run does not match dispatch", ErrOutboxDispatchPermanent)
		}
		run.HTMLURL = github.String(fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d", workflowSpec.VCS.RepoOwner, workflowSpec.VCS.RepoName, details.RunID))
		return durableWorkflowDispatchReceipt(*preparation.Job.OperationID, false, controlRef, run, preparation.ClaimExpiresAt)
	}, nil
}

func durableWorkflowRunName(runName string, operationID string) string {
	return fmt.Sprintf("%s [digger-operation:%s]", strings.TrimSpace(runName), operationID)
}

func sameDurableWorkflowRun(run *github.WorkflowRun, runID int64, target *ci_backends.DurableWorkflowTarget) bool {
	return run != nil && target != nil && runID > 0 && run.GetID() == runID && run.GetRunAttempt() == 1 &&
		run.GetRepository().GetID() == target.RepositoryID && target.RepositoryID > 0 && run.GetWorkflowID() == target.WorkflowID && target.WorkflowID > 0 &&
		run.GetEvent() == "workflow_dispatch" && run.GetHeadBranch() == target.ControlRef && workflowDigest.MatchString(run.GetHeadSHA())
}

func durableWorkflowDispatchReceipt(operationID string, terminalNoop bool, controlRef string, run *github.WorkflowRun, claimExpiresAt time.Time) (OutboxDispatchResult, error) {
	if !terminalNoop && (run == nil || run.GetID() <= 0 || controlRef == "" || claimExpiresAt.IsZero()) {
		return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
	}
	receipt := struct {
		ClaimExpiresAt *time.Time `json:"claim_expires_at,omitempty"`
		Accepted       bool       `json:"accepted"`
		OperationID    string     `json:"operation_id"`
		TerminalNoop   bool       `json:"terminal_noop,omitempty"`
		ControlRef     string     `json:"control_ref,omitempty"`
		RunID          int64      `json:"run_id,omitempty"`
		RunAttempt     int        `json:"run_attempt,omitempty"`
		RunURL         string     `json:"run_url,omitempty"`
		HeadSHA        string     `json:"head_sha,omitempty"`
		RepositoryID   int64      `json:"repository_id,omitempty"`
		WorkflowID     int64      `json:"workflow_id,omitempty"`
	}{
		Accepted:     !terminalNoop,
		OperationID:  operationID,
		TerminalNoop: terminalNoop,
		ControlRef:   controlRef,
	}
	if run != nil {
		receipt.ClaimExpiresAt = &claimExpiresAt
		receipt.RunID = run.GetID()
		receipt.RunAttempt = run.GetRunAttempt()
		receipt.RunURL = run.GetHTMLURL()
		receipt.HeadSHA = run.GetHeadSHA()
		receipt.RepositoryID = run.GetRepository().GetID()
		receipt.WorkflowID = run.GetWorkflowID()
	}
	serializedReceipt, err := json.Marshal(receipt)
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	return OutboxDispatchResult{ProviderReceipt: serializedReceipt}, nil
}
