package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
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
		if preparation.Job.OperationID == nil || request.OperationID != *preparation.Job.OperationID {
			return OutboxDispatchResult{}, models.ErrDurableJobDispatchConflict
		}
		if preparation.SkipProvider {
			return durableWorkflowDispatchReceipt(*preparation.Job.OperationID, true)
		}

		job := preparation.Job
		batch := job.Batch
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
		workflowSpec.WriterEpoch = request.WriterEpoch
		runName, err := services.GetRunNameFromJob(*job)
		if err != nil {
			return OutboxDispatchResult{}, err
		}
		if err := backend.TriggerWorkflowContext(ctx, *workflowSpec, *runName, ""); err != nil {
			return OutboxDispatchResult{}, err
		}
		return durableWorkflowDispatchReceipt(*preparation.Job.OperationID, false)
	}, nil
}

func durableWorkflowDispatchReceipt(operationID string, terminalNoop bool) (OutboxDispatchResult, error) {
	receipt, err := json.Marshal(struct {
		Accepted     bool   `json:"accepted"`
		OperationID  string `json:"operation_id"`
		TerminalNoop bool   `json:"terminal_noop,omitempty"`
	}{
		Accepted:     !terminalNoop,
		OperationID:  operationID,
		TerminalNoop: terminalNoop,
	})
	if err != nil {
		return OutboxDispatchResult{}, err
	}
	return OutboxDispatchResult{ProviderReceipt: receipt}, nil
}
