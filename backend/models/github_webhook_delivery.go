package models

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GithubWebhookDeliveryStatus string

const (
	GithubWebhookDeliveryPending    GithubWebhookDeliveryStatus = "pending"
	GithubWebhookDeliveryProcessing GithubWebhookDeliveryStatus = "processing"
	GithubWebhookDeliveryRetrying   GithubWebhookDeliveryStatus = "retrying"
	GithubWebhookDeliverySucceeded  GithubWebhookDeliveryStatus = "succeeded"
	GithubWebhookDeliveryIgnored    GithubWebhookDeliveryStatus = "ignored"
	GithubWebhookDeliveryDeadLetter GithubWebhookDeliveryStatus = "dead_letter"
)

var ErrGithubWebhookDeliveryConflict = errors.New("github webhook delivery ID was already recorded with a different request identity")

// GithubWebhookDelivery is the durable application inbox for GitHub App webhooks.
// Payload and request identity fields are immutable after the first insert.
type GithubWebhookDelivery struct {
	DeliveryID                 string `gorm:"primaryKey;type:text"`
	OperationID                string `gorm:"type:text;not null;uniqueIndex"`
	PayloadSHA256              string `gorm:"type:text;not null"`
	Payload                    []byte `gorm:"type:bytea;not null"`
	EventType                  string `gorm:"type:text;not null"`
	GithubAppID                int64  `gorm:"not null"`
	HookID                     string `gorm:"type:text"`
	HookInstallationTargetType string `gorm:"type:text"`
	InstallationID             *int64
	RepositoryFullName         string `gorm:"type:text"`
	OrderingDomain             string `gorm:"type:text;not null;uniqueIndex:idx_github_webhook_delivery_order,priority:1"`
	OrderingSequence           int64  `gorm:"not null;uniqueIndex:idx_github_webhook_delivery_order,priority:2"`
	WriterEpoch                *int64
	ReceivedAt                 time.Time                   `gorm:"not null"`
	ProcessingStatus           GithubWebhookDeliveryStatus `gorm:"type:text;not null;default:pending;check:github_webhook_deliveries_status_check,processing_status IN ('pending','processing','retrying','succeeded','ignored','dead_letter');index:idx_github_webhook_delivery_queue,priority:1"`
	AttemptCount               int64                       `gorm:"not null;default:0"`
	LeaseID                    string                      `gorm:"type:text"`
	LeaseExpiresAt             *time.Time                  `gorm:"index:idx_github_webhook_delivery_queue,priority:3"`
	NextAttemptAt              *time.Time                  `gorm:"index:idx_github_webhook_delivery_queue,priority:2"`
	ProcessedAt                *time.Time
	DeadLetteredAt             *time.Time
	TerminalResult             string    `gorm:"type:text"`
	LastError                  string    `gorm:"type:text"`
	UpdatedAt                  time.Time `gorm:"not null"`
}

type GithubWebhookOrderingDomain struct {
	OrderingDomain       string    `gorm:"type:text;primaryKey"`
	NextSequence         int64     `gorm:"not null;default:1;check:github_webhook_ordering_domains_sequence_check,next_sequence > last_terminal_sequence"`
	LastTerminalSequence int64     `gorm:"not null;default:0"`
	CreatedAt            time.Time `gorm:"not null"`
	UpdatedAt            time.Time `gorm:"not null"`
}

func (GithubWebhookOrderingDomain) TableName() string {
	return "github_webhook_ordering_domains"
}

// GithubWebhookDeliveryRequeue is an immutable audit record for an operator
// replaying a dead-lettered delivery.
type GithubWebhookDeliveryRequeue struct {
	ID                      uuid.UUID              `gorm:"type:uuid;primaryKey"`
	GithubWebhookDeliveryID string                 `gorm:"column:delivery_id;type:text;not null;index:idx_github_webhook_delivery_requeues_delivery_id"`
	Delivery                *GithubWebhookDelivery `gorm:"foreignKey:GithubWebhookDeliveryID;references:DeliveryID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	Actor                   string                 `gorm:"type:text;not null"`
	Reason                  string                 `gorm:"type:text;not null"`
	RequeuedAt              time.Time              `gorm:"not null"`
}

func (GithubWebhookDeliveryRequeue) TableName() string {
	return "github_webhook_delivery_requeues"
}

func (GithubWebhookDelivery) TableName() string {
	return "github_webhook_deliveries"
}

func (d *GithubWebhookDelivery) HasSameRequestIdentity(other *GithubWebhookDelivery) bool {
	if d == nil || other == nil {
		return false
	}

	return d.PayloadSHA256 == other.PayloadSHA256 &&
		bytes.Equal(d.Payload, other.Payload) &&
		d.EventType == other.EventType &&
		d.GithubAppID == other.GithubAppID &&
		d.HookID == other.HookID &&
		d.HookInstallationTargetType == other.HookInstallationTargetType &&
		d.InstallationIDValue() == other.InstallationIDValue() &&
		d.RepositoryFullName == other.RepositoryFullName &&
		d.OrderingDomain == other.OrderingDomain &&
		d.OperationID == other.OperationID
}

func (d *GithubWebhookDelivery) InstallationIDValue() int64 {
	if d == nil || d.InstallationID == nil {
		return 0
	}
	return *d.InstallationID
}

func (d *GithubWebhookDelivery) EnsureOrderingDomain() {
	if d.OrderingDomain != "" {
		return
	}
	if d.InstallationID != nil && *d.InstallationID > 0 {
		d.OrderingDomain = fmt.Sprintf("installation:%d:%d", d.GithubAppID, *d.InstallationID)
		return
	}
	d.OrderingDomain = fmt.Sprintf("app:%d", d.GithubAppID)
}

// RecordGithubWebhookDelivery preserves the first request observed for a delivery ID.
// Matching retries return that receipt; a reused ID with a different request is rejected.
func (db *Database) RecordGithubWebhookDelivery(ctx context.Context, delivery *GithubWebhookDelivery, databaseIdentity string, writerEpoch int64) (*GithubWebhookDelivery, bool, error) {
	delivery.EnsureOrderingDomain()
	if delivery.OperationID == "" {
		operationID, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
		if err != nil {
			return nil, false, err
		}
		delivery.OperationID = operationID.String()
	}
	var receipt *GithubWebhookDelivery
	created := false
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		delivery.ReceivedAt = now
		delivery.UpdatedAt = now
		domain := GithubWebhookOrderingDomain{
			OrderingDomain: delivery.OrderingDomain,
			NextSequence:   1,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&domain).Error; err != nil {
			return err
		}
		query := tx
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.First(&domain, "ordering_domain = ?", delivery.OrderingDomain).Error; err != nil {
			return err
		}

		var existing GithubWebhookDelivery
		err = tx.First(&existing, "delivery_id = ?", delivery.DeliveryID).Error
		if err == nil {
			receipt = &existing
			if !existing.HasSameRequestIdentity(delivery) {
				return ErrGithubWebhookDeliveryConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		delivery.OrderingSequence = domain.NextSequence
		result := tx.Model(&GithubWebhookOrderingDomain{}).
			Where("ordering_domain = ? AND next_sequence = ?", domain.OrderingDomain, domain.NextSequence).
			Updates(map[string]any{"next_sequence": domain.NextSequence + 1, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		insertResult := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(delivery)
		if insertResult.Error != nil {
			return insertResult.Error
		}
		if insertResult.RowsAffected != 1 {
			var conflicting GithubWebhookDelivery
			if err := tx.First(&conflicting, "delivery_id = ?", delivery.DeliveryID).Error; err != nil {
				return fmt.Errorf("webhook delivery operation identity collision: %w", err)
			}
			receipt = &conflicting
			return ErrGithubWebhookDeliveryConflict
		}
		controlOperation := ControlOperation{
			OperationID:      delivery.OperationID,
			OperationKind:    "github_webhook_delivery",
			IdentitySHA256:   delivery.PayloadSHA256,
			GithubDeliveryID: &delivery.DeliveryID,
			WriterEpoch:      writerEpoch,
			ProtocolVersion:  operation.ProtocolVersion,
			Status:           ControlOperationPending,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if err := tx.Create(&controlOperation).Error; err != nil {
			return err
		}
		receipt = delivery
		created = true
		return nil
	})
	return receipt, created, err
}

// ClaimNextGithubWebhookDelivery atomically claims one pending or expired delivery.
// The compare-and-set update prevents concurrent backend tasks from sharing a claim.
func (db *Database) ClaimNextGithubWebhookDelivery(ctx context.Context, now time.Time, leaseID string, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) (*GithubWebhookDelivery, error) {
	var claimed *GithubWebhookDelivery
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, true, func(database *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(database, now)
		if err != nil {
			return err
		}
		if database.Dialector.Name() == "postgres" {
			var row GithubWebhookDelivery
			result := database.Raw(`
UPDATE github_webhook_deliveries
SET attempt_count = attempt_count + 1,
    last_error = '',
    lease_expires_at = ?,
    lease_id = ?,
    writer_epoch = ?,
    processing_status = ?,
    updated_at = ?
WHERE delivery_id = (
    SELECT deliveries.delivery_id
    FROM github_webhook_deliveries AS deliveries
    JOIN github_webhook_ordering_domains AS domains
      ON domains.ordering_domain = deliveries.ordering_domain
     AND deliveries.ordering_sequence = domains.last_terminal_sequence + 1
    WHERE ((deliveries.processing_status IN (?, ?) AND (deliveries.next_attempt_at IS NULL OR deliveries.next_attempt_at <= ?))
        OR (deliveries.processing_status = ? AND deliveries.lease_expires_at <= ?))
    ORDER BY deliveries.received_at ASC, deliveries.delivery_id ASC
    FOR UPDATE OF deliveries SKIP LOCKED
    LIMIT 1
)
RETURNING *`,
				effectiveNow.Add(leaseDuration), leaseID, writerEpoch, GithubWebhookDeliveryProcessing, effectiveNow,
				GithubWebhookDeliveryPending, GithubWebhookDeliveryRetrying, effectiveNow,
				GithubWebhookDeliveryProcessing, effectiveNow,
			).Scan(&row)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return nil
			}
			claimed = &row
			return nil
		}

		// SQLite is used by focused unit tests. A transaction keeps this fallback
		// deterministic; production PostgreSQL always uses the SKIP LOCKED statement.
		var candidate GithubWebhookDelivery
		err = database.
			Table("github_webhook_deliveries AS deliveries").
			Select("deliveries.*").
			Joins("JOIN github_webhook_ordering_domains AS domains ON domains.ordering_domain = deliveries.ordering_domain AND deliveries.ordering_sequence = domains.last_terminal_sequence + 1").
			Where(
				"((processing_status IN ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (processing_status = ? AND lease_expires_at <= ?)",
				[]GithubWebhookDeliveryStatus{GithubWebhookDeliveryPending, GithubWebhookDeliveryRetrying}, effectiveNow,
				GithubWebhookDeliveryProcessing, effectiveNow,
			).
			Order("received_at ASC, delivery_id ASC").
			First(&candidate).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		result := database.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ?", candidate.DeliveryID).
			Where("processing_status IN ? OR (processing_status = ? AND lease_expires_at <= ?)", []GithubWebhookDeliveryStatus{GithubWebhookDeliveryPending, GithubWebhookDeliveryRetrying}, GithubWebhookDeliveryProcessing, effectiveNow).
			Updates(map[string]any{
				"attempt_count":     gorm.Expr("attempt_count + 1"),
				"last_error":        "",
				"lease_expires_at":  effectiveNow.Add(leaseDuration),
				"lease_id":          leaseID,
				"writer_epoch":      writerEpoch,
				"processing_status": GithubWebhookDeliveryProcessing,
				"updated_at":        effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return nil
		}
		if err := database.Where("delivery_id = ?", candidate.DeliveryID).First(&candidate).Error; err != nil {
			return err
		}
		claimed = &candidate
		return nil
	})
	return claimed, err
}

func (db *Database) RenewGithubWebhookDeliveryLease(ctx context.Context, deliveryID string, leaseID string, now time.Time, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		result := tx.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ? AND processing_status = ? AND lease_id = ? AND writer_epoch = ?", deliveryID, GithubWebhookDeliveryProcessing, leaseID, writerEpoch).
			Updates(map[string]any{
				"lease_expires_at": effectiveNow.Add(leaseDuration),
				"updated_at":       effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (db *Database) CompleteGithubWebhookDelivery(ctx context.Context, deliveryID string, leaseID string, status GithubWebhookDeliveryStatus, terminalResult string, now time.Time, databaseIdentity string, writerEpoch int64) error {
	if status != GithubWebhookDeliverySucceeded && status != GithubWebhookDeliveryIgnored {
		return fmt.Errorf("invalid terminal webhook status %q", status)
	}
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		var delivery GithubWebhookDelivery
		if err := tx.First(&delivery, "delivery_id = ? AND processing_status = ? AND lease_id = ? AND writer_epoch = ?", deliveryID, GithubWebhookDeliveryProcessing, leaseID, writerEpoch).Error; err != nil {
			return err
		}
		result := tx.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ? AND processing_status = ? AND lease_id = ? AND writer_epoch = ?", deliveryID, GithubWebhookDeliveryProcessing, leaseID, writerEpoch).
			Updates(map[string]any{
				"last_error":        "",
				"lease_expires_at":  nil,
				"lease_id":          "",
				"next_attempt_at":   nil,
				"processed_at":      effectiveNow,
				"processing_status": status,
				"terminal_result":   terminalResult,
				"updated_at":        effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		domainResult := tx.Model(&GithubWebhookOrderingDomain{}).
			Where("ordering_domain = ? AND last_terminal_sequence = ?", delivery.OrderingDomain, delivery.OrderingSequence-1).
			Updates(map[string]any{"last_terminal_sequence": delivery.OrderingSequence, "updated_at": effectiveNow})
		if domainResult.Error != nil {
			return domainResult.Error
		}
		if domainResult.RowsAffected != 1 {
			return fmt.Errorf("advance webhook ordering domain %q to %d: %w", delivery.OrderingDomain, delivery.OrderingSequence, gorm.ErrRecordNotFound)
		}
		operationResult := tx.Model(&ControlOperation{}).
			Where("operation_id = ? AND status = ?", delivery.OperationID, ControlOperationPending).
			Updates(map[string]any{"status": ControlOperationCompleted, "updated_at": effectiveNow})
		if operationResult.Error != nil {
			return operationResult.Error
		}
		if operationResult.RowsAffected != 1 {
			return fmt.Errorf("complete control operation %q: %w", delivery.OperationID, gorm.ErrRecordNotFound)
		}
		return nil
	})
}

func (db *Database) RetryGithubWebhookDelivery(ctx context.Context, deliveryID string, leaseID string, lastError string, nextAttemptAt time.Time, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		retryDelay := nextAttemptAt.Sub(now)
		result := tx.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ? AND processing_status = ? AND lease_id = ? AND writer_epoch = ?", deliveryID, GithubWebhookDeliveryProcessing, leaseID, writerEpoch).
			Updates(map[string]any{
				"last_error":        lastError,
				"lease_expires_at":  nil,
				"lease_id":          "",
				"next_attempt_at":   effectiveNow.Add(retryDelay),
				"processing_status": GithubWebhookDeliveryRetrying,
				"updated_at":        effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (db *Database) DeadLetterGithubWebhookDelivery(ctx context.Context, deliveryID string, leaseID string, lastError string, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		result := tx.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ? AND processing_status = ? AND lease_id = ? AND writer_epoch = ?", deliveryID, GithubWebhookDeliveryProcessing, leaseID, writerEpoch).
			Updates(map[string]any{
				"dead_lettered_at":  effectiveNow,
				"last_error":        lastError,
				"lease_expires_at":  nil,
				"lease_id":          "",
				"next_attempt_at":   nil,
				"processing_status": GithubWebhookDeliveryDeadLetter,
				"terminal_result":   "failed",
				"updated_at":        effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

func (db *Database) CheckGithubWebhookInbox(ctx context.Context) error {
	rows, err := db.GormDB.WithContext(ctx).Model(&GithubWebhookDelivery{}).Select("delivery_id").Limit(1).Rows()
	if err != nil {
		return err
	}
	return rows.Close()
}

func (db *Database) RequeueGithubWebhookDelivery(ctx context.Context, deliveryID string, actor string, reason string, now time.Time, databaseIdentity string, writerEpoch int64) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		effectiveNow, err := databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		result := tx.Model(&GithubWebhookDelivery{}).
			Where("delivery_id = ? AND processing_status = ?", deliveryID, GithubWebhookDeliveryDeadLetter).
			Updates(map[string]any{
				"attempt_count":     0,
				"dead_lettered_at":  nil,
				"last_error":        "",
				"lease_expires_at":  nil,
				"lease_id":          "",
				"next_attempt_at":   effectiveNow,
				"processed_at":      nil,
				"processing_status": GithubWebhookDeliveryPending,
				"terminal_result":   "",
				"updated_at":        effectiveNow,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return tx.Create(&GithubWebhookDeliveryRequeue{
			ID:                      uuid.New(),
			GithubWebhookDeliveryID: deliveryID,
			Actor:                   actor,
			Reason:                  reason,
			RequeuedAt:              effectiveNow,
		}).Error
	})
}
