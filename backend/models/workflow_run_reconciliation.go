package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const GithubWorkflowReconcileEffectKind = "github_workflow_reconcile"

var ErrDurableRunQuarantined = errors.New("workflow completion requires reconciliation with job state")

type GithubRunReconciliationPayload struct {
	OperationID      string    `json:"operation_id"`
	DispatchEffectID uuid.UUID `json:"dispatch_effect_id"`
}

type DurableRunObservation struct {
	RepositoryID int64  `json:"repository_id"`
	WorkflowID   int64  `json:"workflow_id"`
	RunID        int64  `json:"run_id"`
	RunAttempt   int    `json:"run_attempt"`
	HeadSHA      string `json:"head_sha"`
	Status       string `json:"status"`
	Conclusion   string `json:"conclusion"`
}

type DurableRunReconciliationPreparation struct {
	CanonicalObservation DurableRunObservation
	GithubAppID          int64
	InstallationID       int64
	RepoOwner, RepoName  string
	RunID                int64
	Terminal             bool
	PreviousReceipt      []byte
}

func enqueueDurableRunReconciliationTx(tx *gorm.DB, effect *OutboxEffect, receiptBytes []byte, now time.Time) error {
	if effect.EffectKind != GithubWorkflowDispatchEffectKind {
		return nil
	}
	var terminal struct {
		TerminalNoop bool `json:"terminal_noop"`
	}
	if err := json.Unmarshal(receiptBytes, &terminal); err != nil {
		return err
	}
	if terminal.TerminalNoop {
		return nil
	}
	receipt, err := decodeDurableWorkflowDispatchReceipt(receiptBytes)
	if err != nil || !receipt.Accepted || receipt.OperationID != effect.ControlOperationID {
		return ErrDurableJobDispatchConflict
	}
	payload, err := json.Marshal(GithubRunReconciliationPayload{OperationID: effect.ControlOperationID, DispatchEffectID: effect.ID})
	if err != nil {
		return err
	}
	watch := NewOutboxEffect(effect.ControlOperationID, GithubWorkflowReconcileEffectKind, durableRunReconciliationKey(receipt.RepositoryID, receipt.RunID), payload, effect.WriterEpoch, now)
	_, _, err = EnqueueOutboxEffectTx(tx, watch)
	return err
}

func durableRunReconciliationKey(repositoryID, runID int64) string {
	return fmt.Sprintf("run:%d:%d", repositoryID, runID)
}

func loadDurableRunWatchTx(tx *gorm.DB, effectID uuid.UUID, leaseID string, epoch int64, now time.Time) (*OutboxEffect, *OutboxEffect, *durableWorkflowDispatchProviderReceipt, error) {
	var watch OutboxEffect
	query := tx
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.First(&watch, "id = ?", effectID).Error; err != nil {
		return nil, nil, nil, err
	}
	if watch.EffectKind != GithubWorkflowReconcileEffectKind || !watch.ValidPayloadDigest() || watch.Status != OutboxEffectProcessing || watch.LeaseID != leaseID || watch.WriterEpoch != epoch || watch.LeaseExpiresAt == nil || !watch.LeaseExpiresAt.After(now) {
		return nil, nil, nil, ErrDurableJobDispatchClaim
	}
	var payload GithubRunReconciliationPayload
	if err := json.Unmarshal(watch.Payload, &payload); err != nil || payload.OperationID != watch.ControlOperationID {
		return nil, nil, nil, ErrDurableJobDispatchConflict
	}
	var dispatch OutboxEffect
	dispatchQuery := tx
	if tx.Dialector.Name() == "postgres" {
		dispatchQuery = dispatchQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := dispatchQuery.First(&dispatch, "id = ?", payload.DispatchEffectID).Error; err != nil {
		return nil, nil, nil, err
	}
	if dispatch.ControlOperationID != watch.ControlOperationID || dispatch.EffectKind != GithubWorkflowDispatchEffectKind || dispatch.Status != OutboxEffectSucceeded {
		return nil, nil, nil, ErrDurableJobDispatchConflict
	}
	receipt, err := decodeDurableWorkflowDispatchReceipt(dispatch.ProviderReceipt)
	if err != nil || !receipt.Accepted || receipt.TerminalNoop || receipt.OperationID != watch.ControlOperationID || watch.EffectKey != durableRunReconciliationKey(receipt.RepositoryID, receipt.RunID) {
		return nil, nil, nil, ErrDurableJobDispatchConflict
	}
	return &watch, &dispatch, receipt, nil
}

func (db *Database) PrepareDurableRunReconciliation(ctx context.Context, effectID uuid.UUID, leaseID string, databaseIdentity string, epoch int64) (*DurableRunReconciliationPreparation, error) {
	var preparation *DurableRunReconciliationPreparation
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, epoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		if err := lockExecutionAdmissionTx(tx); err != nil {
			return err
		}
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		watch, dispatch, receipt, err := loadDurableRunWatchTx(tx, effectID, leaseID, epoch, now)
		if err != nil {
			return err
		}
		state, err := loadDurableWorkflowStateTx(tx, dispatch, false)
		if err != nil {
			return err
		}
		now, err = databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if watch.LeaseExpiresAt == nil || !watch.LeaseExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		if state.Job.Status == scheduler.DiggerJobStarted {
			unknown := DurableRunObservation{RepositoryID: receipt.RepositoryID, WorkflowID: receipt.WorkflowID, RunID: receipt.RunID, RunAttempt: receipt.RunAttempt, HeadSHA: receipt.HeadSHA, Status: "unobserved"}
			if err := recordUnknownApplyTx(tx, state, unknown, now, epoch); err != nil {
				return err
			}
		}
		preparation = &DurableRunReconciliationPreparation{GithubAppID: state.Delivery.GithubAppID, InstallationID: state.Batch.GithubInstallationId, RepoOwner: state.Batch.RepoOwner, RepoName: state.Batch.RepoName, RunID: receipt.RunID,
			CanonicalObservation: DurableRunObservation{RepositoryID: receipt.RepositoryID, WorkflowID: receipt.WorkflowID, RunID: receipt.RunID, RunAttempt: receipt.RunAttempt, HeadSHA: receipt.HeadSHA},
			Terminal:             state.Job.Status == scheduler.DiggerJobSucceeded || state.Job.Status == scheduler.DiggerJobFailed, PreviousReceipt: append([]byte(nil), watch.ProviderReceipt...)}
		return nil
	})
	return preparation, err
}

// A provider observation may fail a nonterminal job, but cannot infer a successful
// apply or authorize another execution. Job success still requires its callback.
func (db *Database) ReconcileDurableWorkflowRun(ctx context.Context, effectID uuid.UUID, leaseID string, observation DurableRunObservation, databaseIdentity string, epoch int64) (bool, error) {
	terminal := false
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, epoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		if err := lockExecutionAdmissionTx(tx); err != nil {
			return err
		}
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		watch, dispatch, receipt, err := loadDurableRunWatchTx(tx, effectID, leaseID, epoch, now)
		if err != nil {
			return err
		}
		if observation.RepositoryID != receipt.RepositoryID || observation.WorkflowID != receipt.WorkflowID || observation.RunID != receipt.RunID || observation.RunAttempt != receipt.RunAttempt || observation.HeadSHA != receipt.HeadSHA {
			return ErrDurableRunQuarantined
		}
		switch observation.Status {
		case "completed":
			switch observation.Conclusion {
			case "success", "failure", "cancelled", "timed_out", "startup_failure", "action_required", "stale", "skipped", "neutral":
			default:
				return ErrDurableRunQuarantined
			}
		case "queued", "in_progress", "waiting", "pending", "requested", "unavailable":
			if observation.Conclusion != "" {
				return ErrDurableRunQuarantined
			}
		default:
			return ErrDurableRunQuarantined
		}
		state, err := loadDurableWorkflowStateTx(tx, dispatch, false)
		if err != nil {
			return err
		}
		now, err = databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if watch.LeaseExpiresAt == nil || !watch.LeaseExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		if state.Job.Status == scheduler.DiggerJobSucceeded || state.Job.Status == scheduler.DiggerJobFailed {
			terminal = true
			return nil
		}
		// A provider completion cannot revoke a still-valid callback. The job
		// callback owns the apply result; the runner may be delivering it when
		// GitHub transitions the surrounding workflow to a terminal state.
		if state.Job.Status == scheduler.DiggerJobStarted && state.Token.RevokedAt == nil && state.Token.Expiry.After(now) {
			var claim ExecutionClaimAttempt
			if err := tx.Where("operation_id = ? AND state = ?", state.JobOperation.OperationID, ExecutionClaimGranted).First(&claim).Error; err != nil {
				return err
			}
			if claim.GrantExpiresAt.After(now) {
				return nil
			}
		}
		if state.Job.Status == scheduler.DiggerJobStarted {
			if err := recordUnknownApplyTx(tx, state, observation, now, epoch); err != nil {
				return err
			}
			// The reconciliation effect is complete, not the apply. Recovery now
			// owns the unknown outcome; no success/failure transition is inferred.
			terminal = observation.Status == "completed" || observation.Status == "unavailable"
			return nil
		}
		failed := false
		switch observation.Status {
		case "completed":
			switch observation.Conclusion {
			case "failure", "cancelled", "timed_out", "startup_failure", "action_required", "stale", "skipped", "neutral":
				failed = true
			default:
				return ErrDurableRunQuarantined
			}
		case "queued", "in_progress", "waiting", "pending", "requested", "unavailable":
			failed = state.Job.Status == scheduler.DiggerJobTriggered && !receipt.ClaimExpiresAt.After(now)
		default:
			return ErrDurableRunQuarantined
		}
		if !failed {
			return nil
		}
		jobs, operations, tokens, links, err := lockDurableBatchGraphTx(tx, &state.Batch)
		if err != nil {
			return err
		}
		batchOperation, err := validateDurableCallbackGraph(&state.Batch, jobs, operations, tokens, links)
		if err != nil {
			return err
		}
		job := durableJobByID(jobs, state.Job.ID)
		jobOperation := durableOperationByID(operations, receipt.OperationID)
		token := durableTokenByJobID(tokens, state.Job.ID)
		if job == nil || jobOperation == nil || token == nil || jobOperation.Status != ControlOperationPending {
			return ErrDurableJobDispatchConflict
		}
		if job.Status != scheduler.DiggerJobStarted && job.Status != scheduler.DiggerJobTriggered {
			return ErrDurableJobDispatchConflict
		}
		result := tx.Model(&DiggerJob{}).Where("id = ? AND status = ? AND status_version = ?", job.ID, job.Status, job.StatusVersion).Updates(map[string]any{"status": scheduler.DiggerJobFailed, "status_version": int64(3), "status_updated_at": now, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobDispatchConflict
		}
		job.Status = scheduler.DiggerJobFailed
		job.StatusVersion = 3
		job.StatusUpdatedAt = now
		if err := reconcileDurableTerminalJobTx(tx, job, jobOperation, token, now); err != nil {
			return err
		}
		if err := failUnstartedDurableJobsTx(tx, jobs, operations, tokens, now); err != nil {
			return err
		}
		if err := updateDurableBatchStateTx(tx, &state.Batch, batchOperation, jobs, now); err != nil {
			return err
		}
		encoded, err := json.Marshal(observation)
		if err != nil {
			return err
		}
		if err := tx.Model(&OutboxEffect{}).Where("id = ?", watch.ID).Update("provider_receipt", encoded).Error; err != nil {
			return err
		}
		terminal = true
		return nil
	})
	return terminal, err
}

func (db *Database) WakeDurableRunReconciliation(ctx context.Context, repositoryID, runID int64, databaseIdentity string, epoch int64) error {
	if repositoryID <= 0 || runID <= 0 {
		return ErrDurableJobDispatchClaim
	}
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, epoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		return tx.Model(&OutboxEffect{}).Where("effect_kind = ? AND effect_key = ? AND status IN ?", GithubWorkflowReconcileEffectKind, durableRunReconciliationKey(repositoryID, runID), []OutboxEffectStatus{OutboxEffectPending, OutboxEffectRetrying}).Updates(map[string]any{"next_attempt_at": now, "updated_at": now}).Error
	})
}
