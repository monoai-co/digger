package models

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrOutboxEffectConflict = errors.New("outbox effect identity was already recorded with a different payload")
var ErrOutboxEffectPayload = errors.New("outbox effect payload is invalid")

func NewOutboxEffect(operationID string, effectKind string, effectKey string, payload []byte, writerEpoch int64, now time.Time) *OutboxEffect {
	normalizedPayload, err := normalizeOutboxEffectPayload(effectKind, payload)
	if err == nil {
		payload = normalizedPayload
	}
	return &OutboxEffect{
		ID:                 uuid.New(),
		ControlOperationID: operationID,
		EffectKind:         effectKind,
		EffectKey:          effectKey,
		Payload:            payload,
		PayloadSHA256:      payloadSHA256(payload),
		WriterEpoch:        writerEpoch,
		Status:             OutboxEffectPending,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
}

func (effect *OutboxEffect) HasSameIdentity(other *OutboxEffect) bool {
	if effect == nil || other == nil {
		return false
	}
	return effect.ControlOperationID == other.ControlOperationID &&
		effect.EffectKind == other.EffectKind &&
		effect.EffectKey == other.EffectKey &&
		effect.PayloadSHA256 == other.PayloadSHA256
}

func (effect *OutboxEffect) ValidPayloadDigest() bool {
	if effect == nil {
		return false
	}
	normalizedPayload, err := normalizeOutboxEffectPayload(effect.EffectKind, effect.Payload)
	return err == nil && effect.PayloadSHA256 == payloadSHA256(normalizedPayload)
}

func normalizeOutboxEffectPayload(effectKind string, payload []byte) ([]byte, error) {
	var typedPayload any
	switch effectKind {
	case GithubReportCreateEffectKind:
		report, err := DecodeGithubReportCreatePayload(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrOutboxEffectPayload, err)
		}
		canonical, err := CanonicalGithubReportCreatePayload(report)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrOutboxEffectPayload, err)
		}
		return canonical, nil
	case GithubWorkflowDispatchEffectKind:
		typedPayload = &GithubWorkflowDispatchPayload{}
	case GithubWorkflowReconcileEffectKind:
		typedPayload = &GithubRunReconciliationPayload{}
	default:
		return payload, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(typedPayload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOutboxEffectPayload, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, ErrOutboxEffectPayload
	}
	canonical, err := json.Marshal(typedPayload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOutboxEffectPayload, err)
	}
	return canonical, nil
}

// EnqueueOutboxEffectTx records provider intent in the caller's authoritative
// transaction. The caller must hold the control-plane fence for this epoch.
func EnqueueOutboxEffectTx(tx *gorm.DB, effect *OutboxEffect) (*OutboxEffect, bool, error) {
	normalizedPayload, err := normalizeOutboxEffectPayload(effect.EffectKind, effect.Payload)
	if err != nil {
		return nil, false, err
	}
	effect.Payload = normalizedPayload
	effect.PayloadSHA256 = payloadSHA256(normalizedPayload)
	now, err := databaseTransactionNow(tx, time.Now().UTC())
	if err != nil {
		return nil, false, err
	}
	effect.CreatedAt = now
	effect.UpdatedAt = now
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(effect)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 1 {
		return effect, true, nil
	}

	var existing OutboxEffect
	if err = tx.Where("operation_id = ? AND effect_kind = ? AND effect_key = ?", effect.ControlOperationID, effect.EffectKind, effect.EffectKey).First(&existing).Error; err != nil {
		return nil, false, err
	}
	if !existing.HasSameIdentity(effect) {
		return &existing, false, ErrOutboxEffectConflict
	}
	return &existing, false, nil
}

func payloadSHA256(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func (db *Database) EnqueueOutboxEffect(ctx context.Context, effect *OutboxEffect, databaseIdentity string, writerEpoch int64) (*OutboxEffect, bool, error) {
	var receipt *OutboxEffect
	created := false
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		var err error
		receipt, created, err = EnqueueOutboxEffectTx(tx, effect)
		return err
	})
	return receipt, created, err
}

func (db *Database) ClaimNextOutboxEffect(ctx context.Context, now time.Time, leaseID string, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) (*OutboxEffect, error) {
	var claimed *OutboxEffect
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, true, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		if tx.Dialector.Name() == "postgres" {
			var effect OutboxEffect
			result := tx.Raw(`
UPDATE outbox_effects
SET attempt_count = attempt_count + 1,
    last_error = '',
    lease_expires_at = ?,
    lease_id = ?,
    writer_epoch = ?,
    status = ?,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM outbox_effects
    WHERE ((status IN (?, ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
        OR (status = ? AND lease_expires_at <= ?))
    ORDER BY created_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *`,
				effectiveNow.Add(leaseDuration), leaseID, writerEpoch, OutboxEffectProcessing, effectiveNow,
				OutboxEffectPending, OutboxEffectRetrying, effectiveNow,
				OutboxEffectProcessing, effectiveNow,
			).Scan(&effect)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 1 {
				claimed = &effect
			}
			return nil
		}

		var candidate OutboxEffect
		err = tx.Where(
			"((status IN ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (status = ? AND lease_expires_at <= ?)",
			[]OutboxEffectStatus{OutboxEffectPending, OutboxEffectRetrying}, effectiveNow,
			OutboxEffectProcessing, effectiveNow,
		).Order("created_at ASC, id ASC").First(&candidate).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		result := tx.Model(&OutboxEffect{}).
			Where("id = ?", candidate.ID).
			Where("status IN ? OR (status = ? AND lease_expires_at <= ?)", []OutboxEffectStatus{OutboxEffectPending, OutboxEffectRetrying}, OutboxEffectProcessing, effectiveNow).
			Updates(map[string]any{
				"attempt_count":    gorm.Expr("attempt_count + 1"),
				"last_error":       "",
				"lease_expires_at": effectiveNow.Add(leaseDuration),
				"lease_id":         leaseID,
				"writer_epoch":     writerEpoch,
				"status":           OutboxEffectProcessing,
				"updated_at":       effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		if err := tx.First(&candidate, "id = ?", candidate.ID).Error; err != nil {
			return err
		}
		claimed = &candidate
		return nil
	})
	return claimed, err
}

func (db *Database) RenewOutboxEffectLease(ctx context.Context, effectID uuid.UUID, leaseID string, now time.Time, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) error {
	return db.updateClaimedOutboxEffect(ctx, effectID, leaseID, databaseIdentity, writerEpoch, func(tx *gorm.DB, effectiveNow time.Time) *gorm.DB {
		return tx.Updates(map[string]any{"lease_expires_at": effectiveNow.Add(leaseDuration), "updated_at": effectiveNow})
	}, nil, now)
}

func (db *Database) CompleteOutboxEffect(ctx context.Context, effectID uuid.UUID, leaseID string, providerReceipt []byte, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.updateClaimedOutboxEffect(ctx, effectID, leaseID, databaseIdentity, writerEpoch, func(tx *gorm.DB, effectiveNow time.Time) *gorm.DB {
		return tx.Updates(map[string]any{
			"last_error":       "",
			"lease_expires_at": nil,
			"lease_id":         "",
			"next_attempt_at":  nil,
			"provider_receipt": providerReceipt,
			"status":           OutboxEffectSucceeded,
			"updated_at":       effectiveNow,
		})
	}, func(tx *gorm.DB, effect *OutboxEffect, effectiveNow time.Time) error {
		if err := completeGithubReportCreateTx(tx, effect, providerReceipt); err != nil {
			return err
		}
		if err := completeDurableWorkflowDispatchTx(tx, effect, effectiveNow); err != nil {
			return err
		}
		return enqueueDurableRunReconciliationTx(tx, effect, providerReceipt, effectiveNow)
	}, now)
}

func (db *Database) RetryOutboxEffect(ctx context.Context, effectID uuid.UUID, leaseID string, lastError string, retryDelay time.Duration, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.updateClaimedOutboxEffect(ctx, effectID, leaseID, databaseIdentity, writerEpoch, func(tx *gorm.DB, effectiveNow time.Time) *gorm.DB {
		return tx.Updates(map[string]any{
			"last_error":       lastError,
			"lease_expires_at": nil,
			"lease_id":         "",
			"next_attempt_at":  effectiveNow.Add(retryDelay),
			"status":           OutboxEffectRetrying,
			"updated_at":       effectiveNow,
		})
	}, nil, now)
}

func (db *Database) DeadLetterOutboxEffect(ctx context.Context, effectID uuid.UUID, leaseID string, lastError string, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.updateClaimedOutboxEffect(ctx, effectID, leaseID, databaseIdentity, writerEpoch, func(tx *gorm.DB, effectiveNow time.Time) *gorm.DB {
		return tx.Updates(map[string]any{
			"last_error":       lastError,
			"lease_expires_at": nil,
			"lease_id":         "",
			"next_attempt_at":  nil,
			"status":           OutboxEffectDeadLetter,
			"updated_at":       effectiveNow,
		})
	}, func(tx *gorm.DB, effect *OutboxEffect, effectiveNow time.Time) error {
		var attempts int64
		if err := tx.Model(&GithubReportCreateAttempt{}).Where("effect_id = ?", effect.ID).Count(&attempts).Error; err != nil {
			return err
		}
		if attempts != 0 {
			return ErrGithubReportCreateConflict
		}
		return deadLetterDurableWorkflowDispatchTx(tx, effect, effectiveNow)
	}, now)
}

func (db *Database) updateClaimedOutboxEffect(
	ctx context.Context,
	effectID uuid.UUID,
	leaseID string,
	databaseIdentity string,
	writerEpoch int64,
	update func(*gorm.DB, time.Time) *gorm.DB,
	afterUpdate func(*gorm.DB, *OutboxEffect, time.Time) error,
	now time.Time,
) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		var effect OutboxEffect
		effectQuery := tx
		if tx.Dialector.Name() == "postgres" {
			effectQuery = effectQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := effectQuery.First(&effect, "id = ?", effectID).Error; err != nil {
			return err
		}
		if effect.Status != OutboxEffectProcessing || effect.LeaseID != leaseID || effect.WriterEpoch != writerEpoch {
			return gorm.ErrRecordNotFound
		}
		result := update(tx.Model(&OutboxEffect{}).Where("id = ? AND status = ? AND lease_id = ? AND writer_epoch = ?", effectID, OutboxEffectProcessing, leaseID, writerEpoch), effectiveNow)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		if afterUpdate != nil {
			return afterUpdate(tx, &effect, effectiveNow)
		}
		return nil
	})
}

func completeDurableWorkflowDispatchTx(tx *gorm.DB, effect *OutboxEffect, now time.Time) error {
	if effect.EffectKind != GithubWorkflowDispatchEffectKind {
		return nil
	}
	state, err := loadDurableWorkflowDispatchStateTx(tx, effect)
	if err != nil {
		return err
	}
	switch state.Job.Status {
	case scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun:
		if state.JobOperation.Status != ControlOperationPending || state.Token.ActivatedAt == nil || state.Token.RevokedAt != nil {
			return ErrDurableJobDispatchConflict
		}
		return tx.Model(&DiggerJob{}).Where("id = ?", state.Job.ID).Updates(map[string]any{
			"status":            scheduler.DiggerJobTriggered,
			"status_version":    gorm.Expr("status_version + 1"),
			"status_updated_at": now,
		}).Error
	case scheduler.DiggerJobTriggered, scheduler.DiggerJobStarted:
		if state.JobOperation.Status != ControlOperationPending || state.Token.ActivatedAt == nil || state.Token.RevokedAt != nil {
			return ErrDurableJobDispatchConflict
		}
		return nil
	case scheduler.DiggerJobSucceeded, scheduler.DiggerJobFailed:
		return reconcileDurableTerminalJobTx(tx, &state.Job, &state.JobOperation, &state.Token, now)
	default:
		return ErrDurableJobDispatchConflict
	}
}

func deadLetterDurableWorkflowDispatchTx(tx *gorm.DB, effect *OutboxEffect, now time.Time) error {
	const savepoint = "durable_workflow_dead_letter"
	if err := tx.SavePoint(savepoint).Error; err != nil {
		return err
	}
	err := deadLetterDurableWorkflowDispatchBusinessTx(tx, effect, now)
	if errors.Is(err, ErrDurableJobDispatchConflict) || errors.Is(err, gorm.ErrRecordNotFound) {
		if rollbackErr := tx.RollbackTo(savepoint).Error; rollbackErr != nil {
			return rollbackErr
		}
		return nil
	}
	return err
}

func deadLetterDurableWorkflowDispatchBusinessTx(tx *gorm.DB, effect *OutboxEffect, now time.Time) error {
	if effect.EffectKind != GithubWorkflowDispatchEffectKind {
		return nil
	}
	state, err := loadDurableWorkflowDispatchStateTx(tx, effect)
	if err != nil {
		return err
	}
	if state.Job.Status == scheduler.DiggerJobSucceeded || state.Job.Status == scheduler.DiggerJobFailed {
		if err := reconcileDurableTerminalJobTx(tx, &state.Job, &state.JobOperation, &state.Token, now); err != nil {
			return err
		}
		if err := tx.Model(&OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
			"last_error":       "",
			"provider_receipt": []byte(`{"accepted":false,"terminal_noop":true}`),
			"status":           OutboxEffectSucceeded,
			"updated_at":       now,
		}).Error; err != nil {
			return err
		}
		return nil
	}
	if state.JobOperation.Status != ControlOperationPending {
		return ErrDurableJobDispatchConflict
	}
	if err := tx.Model(&DiggerJob{}).Where("id = ?", state.Job.ID).Updates(map[string]any{
		"status":            scheduler.DiggerJobFailed,
		"status_version":    gorm.Expr("status_version + 1"),
		"status_updated_at": now,
	}).Error; err != nil {
		return err
	}
	state.Job.Status = scheduler.DiggerJobFailed
	return reconcileDurableTerminalJobTx(tx, &state.Job, &state.JobOperation, &state.Token, now)
}

func databaseTransactionNow(tx *gorm.DB, fallback time.Time) (time.Time, error) {
	if tx.Dialector.Name() != "postgres" {
		return fallback, nil
	}
	var now time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}
	return now.UTC(), nil
}
