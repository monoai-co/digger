package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrApplyRecoveryRequired = errors.New("an execution outcome requires recovery before another execution can start")
var ErrApplyRecoveryConflict = errors.New("apply recovery revision or evidence conflicts with persisted state")

// ApplyRecovery records uncertainty separately from the job's terminal result.
// An unresolved record never authorizes another execution or releases children.
type ApplyRecovery struct {
	OperationID               string                 `gorm:"type:text;primaryKey" json:"operation_id"`
	Operation                 *ControlOperation      `gorm:"foreignKey:OperationID;references:OperationID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT" json:"-"`
	OrganisationID            uint                   `gorm:"not null;index:idx_apply_recovery_org" json:"organisation_id"`
	Organisation              *Organisation          `gorm:"foreignKey:OrganisationID;references:ID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT" json:"-"`
	ExecutionClaimID          uuid.UUID              `gorm:"type:uuid;not null;uniqueIndex:idx_apply_recovery_claim" json:"execution_claim_id"`
	Claim                     *ExecutionClaimAttempt `gorm:"foreignKey:ExecutionClaimID;references:ID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT" json:"-"`
	WriterEpoch               int64                  `gorm:"not null" json:"writer_epoch"`
	Revision                  int64                  `gorm:"not null;default:1" json:"revision"`
	Outcome                   string                 `gorm:"type:text;not null;default:unknown;check:apply_recovery_outcome_check,outcome IN ('unknown','verified_succeeded','aborted')" json:"outcome"`
	Observation               json.RawMessage        `gorm:"type:jsonb;not null" json:"observation"`
	ObservationSHA256         string                 `gorm:"type:text;not null" json:"observation_sha256"`
	TerminalObservation       json.RawMessage        `gorm:"type:jsonb" json:"terminal_observation,omitempty"`
	TerminalObservationSHA256 string                 `gorm:"type:text;not null;default:''" json:"terminal_observation_sha256,omitempty"`
	CreatedAt                 time.Time              `gorm:"not null" json:"created_at"`
	ResolutionID              *uuid.UUID             `gorm:"type:uuid;uniqueIndex:idx_apply_recovery_resolution" json:"resolution_id,omitempty"`
	ResolutionSHA256          string                 `gorm:"type:text;not null;default:''" json:"resolution_sha256,omitempty"`
	Resolution                json.RawMessage        `gorm:"type:jsonb" json:"resolution,omitempty"`
	ResolvedAt                *time.Time             `json:"resolved_at,omitempty"`
}

// All execution admission and uncertainty decisions take this short transaction
// mutex before any graph rows. It closes the race between observing a newly
// expired grant and admitting another job. No provider call runs under this lock.
func lockExecutionAdmissionTx(tx *gorm.DB) error {
	if tx.Dialector.Name() == "postgres" {
		return tx.Exec("SELECT pg_advisory_xact_lock(68437125047000)").Error
	}
	return nil
}

func requireKnownExecutionOutcomesTx(tx *gorm.DB, organisationID uint, operationID string, now time.Time) error {
	var count int64
	// Check the grant itself as well as recorded recovery. The scheduler need not
	// have observed the expiration yet for admission to fail closed.
	err := tx.Table("execution_claim_attempts AS c").Joins("JOIN job_tokens AS t ON t.id = c.job_token_id").Joins("JOIN digger_jobs AS j ON j.id = c.digger_job_database_id").Where("t.organisation_id = ? AND c.operation_id <> ? AND c.state = ? AND (c.grant_expires_at <= ? OR t.expiry <= ? OR t.revoked_at IS NOT NULL) AND j.status = ?", organisationID, operationID, ExecutionClaimGranted, now, now, scheduler.DiggerJobStarted).Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrApplyRecoveryRequired
	}
	err = tx.Model(&ApplyRecovery{}).Where("organisation_id = ? AND outcome = ?", organisationID, "unknown").Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrApplyRecoveryRequired
	}
	return nil
}

func recordUnknownApplyTx(tx *gorm.DB, state *durableWorkflowDispatchState, observation DurableRunObservation, now time.Time, epoch int64) error {
	var claim ExecutionClaimAttempt
	if err := tx.Where("operation_id = ? AND state = ?", state.JobOperation.OperationID, ExecutionClaimGranted).First(&claim).Error; err != nil {
		return err
	}
	if claim.GrantExpiresAt.After(now) && state.Token.Expiry.After(now) && state.Token.RevokedAt == nil {
		return nil
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	recovery := ApplyRecovery{OperationID: state.JobOperation.OperationID, OrganisationID: state.Token.OrganisationID, ExecutionClaimID: claim.ID, WriterEpoch: epoch, Revision: 1, Outcome: "unknown", Observation: encoded, ObservationSHA256: payloadSHA256(encoded), CreatedAt: now}
	if observation.Status == "completed" || observation.Status == "unavailable" {
		recovery.TerminalObservation = encoded
		recovery.TerminalObservationSHA256 = payloadSHA256(encoded)
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&recovery).Error; err != nil {
		return err
	}
	var stored ApplyRecovery
	if err := tx.First(&stored, "operation_id = ?", recovery.OperationID).Error; err != nil {
		return err
	}
	first, err := decodeRecoveryObservation(stored.Observation, stored.ObservationSHA256)
	if err != nil || stored.OrganisationID != recovery.OrganisationID || stored.ExecutionClaimID != claim.ID || first.RepositoryID != observation.RepositoryID || first.WorkflowID != observation.WorkflowID || first.RunID != observation.RunID || first.RunAttempt != observation.RunAttempt || first.HeadSHA != observation.HeadSHA {
		return ErrApplyRecoveryConflict
	}
	if len(stored.TerminalObservation) == 0 && (observation.Status == "completed" || observation.Status == "unavailable") && stored.Outcome == "unknown" {
		return tx.Model(&ApplyRecovery{}).Where("operation_id = ? AND outcome = ?", recovery.OperationID, "unknown").Updates(map[string]any{"terminal_observation": encoded, "terminal_observation_sha256": payloadSHA256(encoded)}).Error
	}
	if len(stored.TerminalObservation) > 0 {
		terminal, err := decodeRecoveryObservation(stored.TerminalObservation, stored.TerminalObservationSHA256)
		if err != nil || (terminal.Status != "completed" && terminal.Status != "unavailable") || terminal.RepositoryID != first.RepositoryID || terminal.WorkflowID != first.WorkflowID || terminal.RunID != first.RunID || terminal.RunAttempt != first.RunAttempt || terminal.HeadSHA != first.HeadSHA {
			return ErrApplyRecoveryConflict
		}
		if observation.Status == terminal.Status && stored.TerminalObservationSHA256 != payloadSHA256(encoded) {
			return ErrApplyRecoveryConflict
		}
	}
	return nil
}

func decodeRecoveryObservation(raw []byte, digest string) (*DurableRunObservation, error) {
	var observation DurableRunObservation
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&observation) != nil {
		return nil, ErrApplyRecoveryConflict
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, ErrApplyRecoveryConflict
	}
	encoded, err := json.Marshal(observation)
	if err != nil || payloadSHA256(encoded) != digest {
		return nil, ErrApplyRecoveryConflict
	}
	return &observation, nil
}

func (db *Database) GetApplyRecovery(ctx context.Context, operationID string, organisationID uint) (*ApplyRecovery, error) {
	var recovery ApplyRecovery
	err := db.GormDB.WithContext(ctx).Where("operation_id = ? AND organisation_id = ?", operationID, organisationID).First(&recovery).Error
	return &recovery, err
}

type ResolveApplyRecoveryRequest struct {
	ProviderUnavailable    bool      `json:"provider_unavailable"`
	Reason                 string    `json:"reason"`
	ResolutionID           uuid.UUID `json:"resolution_id"`
	ExpectedRevision       int64     `json:"expected_revision"`
	Outcome                string    `json:"outcome"`
	ExecutorStopped        bool      `json:"executor_stopped"`
	EvidenceURI            string    `json:"evidence_uri"`
	ExecutorEvidenceSHA256 string    `json:"executor_evidence_sha256"`
	StateEvidenceSHA256    string    `json:"state_evidence_sha256"`
	ResourceEvidenceSHA256 string    `json:"resource_evidence_sha256"`
	ResultEvidenceSHA256   string    `json:"result_evidence_sha256"`
}

// ResolveApplyRecovery is an operator decision, never a CLI callback or retry.
// Evidence digests identify the operator's immutable recovery package. The
// operator must verify that package; hashes alone cannot establish AWS reality.
func (db *Database) ResolveApplyRecovery(ctx context.Context, operationID string, organisationID uint, actor string, request ResolveApplyRecoveryRequest, databaseIdentity string, epoch int64) (*ApplyRecovery, error) {
	evidenceURL, err := url.Parse(request.EvidenceURI)
	if err != nil || evidenceURL == nil || (evidenceURL.Scheme != "https" && evidenceURL.Scheme != "s3") || evidenceURL.Host == "" || evidenceURL.User != nil || evidenceURL.RawQuery != "" || evidenceURL.Fragment != "" || len(request.EvidenceURI) > 2048 ||
		strings.TrimSpace(actor) == "" || len(actor) > 1024 || organisationID == 0 || request.ResolutionID == uuid.Nil || request.ExpectedRevision != 1 || !request.ExecutorStopped ||
		strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 2048 ||
		(request.Outcome != "verified_succeeded" && request.Outcome != "aborted") ||
		!validLowerHexDigest(request.ExecutorEvidenceSHA256, 64) || !validLowerHexDigest(request.StateEvidenceSHA256, 64) || !validLowerHexDigest(request.ResourceEvidenceSHA256, 64) || !validLowerHexDigest(request.ResultEvidenceSHA256, 64) {
		return nil, ErrApplyRecoveryConflict
	}
	encoded, err := json.Marshal(struct {
		Actor   string                      `json:"actor"`
		Request ResolveApplyRecoveryRequest `json:"request"`
	}{actor, request})
	if err != nil {
		return nil, err
	}
	digest := payloadSHA256(encoded)
	var result *ApplyRecovery
	err = db.WithAuthoritativeWriteTx(ctx, databaseIdentity, epoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		if err := lockExecutionAdmissionTx(tx); err != nil {
			return err
		}
		var recovery ApplyRecovery
		if err := tx.Where("operation_id = ? AND organisation_id = ?", operationID, organisationID).First(&recovery).Error; err != nil {
			return err
		}
		if recovery.Outcome != "unknown" {
			if recovery.ResolutionID == nil || *recovery.ResolutionID != request.ResolutionID || recovery.ResolutionSHA256 != digest {
				return ErrApplyRecoveryConflict
			}
			result = &recovery
			return nil
		}
		if recovery.Revision != request.ExpectedRevision {
			return ErrApplyRecoveryConflict
		}
		terminal, err := decodeRecoveryObservation(recovery.TerminalObservation, recovery.TerminalObservationSHA256)
		if len(recovery.TerminalObservation) == 0 && request.ProviderUnavailable {
			// An administrator may reconcile using independent executor evidence
			// when the provider cannot supply a usable terminal receipt. The first
			// observation binds this decision to the original canonical execution.
			terminal, err = decodeRecoveryObservation(recovery.Observation, recovery.ObservationSHA256)
		}
		if err != nil || (terminal.Status != "completed") != request.ProviderUnavailable {
			return ErrApplyRecoveryConflict
		}
		var route DiggerJob
		if err := tx.Where("operation_id = ?", operationID).First(&route).Error; err != nil {
			return err
		}
		if route.BatchID == nil {
			return ErrApplyRecoveryConflict
		}
		var batch DiggerBatch
		batchQuery := tx
		if tx.Dialector.Name() == "postgres" {
			batchQuery = batchQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := batchQuery.First(&batch, "id = ?", *route.BatchID).Error; err != nil {
			return err
		}
		jobs, operations, tokens, links, err := lockDurableBatchGraphTx(tx, &batch)
		if err != nil {
			return err
		}
		batchOperation, err := validateDurableCallbackGraph(&batch, jobs, operations, tokens, links)
		if err != nil {
			return err
		}
		job := durableJobByID(jobs, route.ID)
		jobOperation := durableOperationByID(operations, operationID)
		token := durableTokenByJobID(tokens, route.ID)
		if job == nil || jobOperation == nil || token == nil || token.OrganisationID != organisationID {
			return ErrApplyRecoveryConflict
		}
		var claim ExecutionClaimAttempt
		if err := tx.First(&claim, "id = ? AND operation_id = ? AND job_token_id = ? AND digger_job_database_id = ? AND state = ?", recovery.ExecutionClaimID, operationID, token.ID, job.ID, ExecutionClaimGranted).Error; err != nil {
			return ErrApplyRecoveryConflict
		}
		if terminal.RepositoryID != claim.RepositoryID || terminal.RunID != claim.RunID || int64(terminal.RunAttempt) != claim.RunAttempt || terminal.HeadSHA != claim.WorkflowSHA {
			return ErrApplyRecoveryConflict
		}
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if claim.GrantExpiresAt.After(now) && token.Expiry.After(now) && token.RevokedAt == nil {
			return ErrApplyRecoveryConflict
		}
		targetStatus := "failed"
		if request.Outcome == "verified_succeeded" {
			targetStatus = "succeeded"
		}
		transition := DurableJobStatusCallbackRequest{TargetStatus: targetStatus, ExpectedStatusVersion: 2, PRCommentURL: job.PRCommentUrl, TerraformOutput: job.TerraformOutput}
		if _, _, err := applyDurableStatusTransitionTx(tx, transition, job, jobOperation, token, jobs, operations, tokens, now, epoch); err != nil {
			return err
		}
		if err := updateDurableBatchStateTx(tx, &batch, batchOperation, jobs, now); err != nil {
			return err
		}
		update := tx.Model(&ApplyRecovery{}).Where("operation_id = ? AND revision = ? AND outcome = ?", operationID, request.ExpectedRevision, "unknown").Updates(map[string]any{"outcome": request.Outcome, "revision": int64(2), "resolution_id": request.ResolutionID, "resolution_sha256": digest, "resolution": encoded, "resolved_at": now})
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return ErrApplyRecoveryConflict
		}
		if err := tx.First(&recovery, "operation_id = ?", operationID).Error; err != nil {
			return err
		}
		result = &recovery
		return nil
	})
	return result, err
}
