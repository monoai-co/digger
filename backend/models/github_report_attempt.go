package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGithubReportCreateConflict = errors.New("github report create conflicts with immutable intent")
var ErrGithubReportCreateClaim = errors.New("github report create lease is not owned by this writer")

type GithubReportCreateAttempt struct {
	EffectID           uuid.UUID     `gorm:"type:uuid;primaryKey"`
	Effect             *OutboxEffect `gorm:"foreignKey:EffectID;references:ID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	ControlOperationID string        `gorm:"column:operation_id;type:text;not null"`
	EffectKey          string        `gorm:"type:text;not null"`
	PayloadSHA256      string        `gorm:"type:text;not null;check:github_report_create_attempt_digest_check,length(payload_sha256) = 64"`
	WriterEpoch        int64         `gorm:"not null"`
	LeaseID            string        `gorm:"type:text;not null"`
	CreatedAt          time.Time     `gorm:"not null"`
}

func (GithubReportCreateAttempt) TableName() string { return "github_report_create_attempts" }

type GithubReportReceipt struct {
	EffectID               uuid.UUID                  `gorm:"type:uuid;primaryKey"`
	Attempt                *GithubReportCreateAttempt `gorm:"foreignKey:EffectID;references:EffectID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	PayloadSHA256          string                     `gorm:"type:text;not null;check:github_report_receipt_digest_check,length(payload_sha256) = 64"`
	ProviderIdentitySHA256 string                     `gorm:"type:text;not null;check:github_report_receipt_provider_identity_check,length(provider_identity_sha256) = 64;uniqueIndex:idx_github_report_receipt_provider_identity"`
	ResourceKind           GithubReportResourceKind   `gorm:"type:text;not null"`
	ProviderID             int64                      `gorm:"not null"`
	ProviderURL            string                     `gorm:"type:text;not null"`
	CreatedAt              time.Time                  `gorm:"not null"`
}

func (GithubReportReceipt) TableName() string { return "github_report_receipts" }

type GithubReportCreatePreparation struct {
	Payload     GithubReportCreatePayload
	Correlation string
	MayCreate   bool
	AttemptedAt time.Time
	Receipt     *GithubReportCreateReceipt
}

// PrepareGithubReportCreate consumes the sole create permit before any provider
// request. Every retry must reconcile the provider using the returned identity.
func (db *Database) PrepareGithubReportCreate(ctx context.Context, effectID uuid.UUID, leaseID, databaseIdentity string, writerEpoch int64) (*GithubReportCreatePreparation, error) {
	if effectID == uuid.Nil || strings.TrimSpace(leaseID) == "" || writerEpoch <= 0 {
		return nil, ErrGithubReportCreateClaim
	}
	var preparation *GithubReportCreatePreparation
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		var effect OutboxEffect
		if err := githubReportLock(tx, "UPDATE").First(&effect, "id = ?", effectID).Error; err != nil {
			return err
		}
		if effect.Status != OutboxEffectProcessing || effect.LeaseID != leaseID || effect.WriterEpoch != writerEpoch {
			return ErrGithubReportCreateClaim
		}
		var attempt GithubReportCreateAttempt
		err := githubReportLock(tx, "SHARE").First(&attempt, "effect_id = ?", effect.ID).Error
		fresh := errors.Is(err, gorm.ErrRecordNotFound)
		if err != nil && !fresh {
			return err
		}
		if !fresh && !validGithubReportAttempt(&attempt, &effect) {
			return ErrGithubReportCreateConflict
		}
		payload, err := validateGithubReportEffectTx(tx, &effect, fresh)
		if err != nil {
			return err
		}
		correlation, err := GithubReportCreateCorrelation(effect.ID, effect.PayloadSHA256)
		if err != nil {
			return err
		}
		var receipt GithubReportReceipt
		receiptErr := githubReportLock(tx, "SHARE").First(&receipt, "effect_id = ?", effect.ID).Error
		if receiptErr != nil && !errors.Is(receiptErr, gorm.ErrRecordNotFound) {
			return receiptErr
		}
		now, err := githubReportLeaseNow(tx, &effect)
		if err != nil {
			return err
		}
		result := &GithubReportCreatePreparation{Payload: payload, Correlation: correlation}
		if receiptErr == nil {
			value := receipt.value()
			expectedIdentity, identityErr := githubReportProviderIdentitySHA256(payload, value.ProviderID)
			if fresh || validateGithubReportReceipt(value, &effect, payload) != nil || identityErr != nil || receipt.ProviderIdentitySHA256 != expectedIdentity {
				return ErrGithubReportCreateConflict
			}
			result.Receipt = &value
		}
		if fresh {
			attempt = GithubReportCreateAttempt{EffectID: effect.ID, PayloadSHA256: effect.PayloadSHA256,
				ControlOperationID: effect.ControlOperationID, EffectKey: effect.EffectKey,
				WriterEpoch: writerEpoch, LeaseID: leaseID, CreatedAt: now}
			if err := tx.Create(&attempt).Error; err != nil {
				return err
			}
			result.MayCreate = true
		}
		result.AttemptedAt = attempt.CreatedAt.UTC()
		preparation = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return preparation, nil
}

func githubReportLock(tx *gorm.DB, strength string) *gorm.DB {
	if tx.Dialector.Name() == "postgres" {
		return tx.Clauses(clause.Locking{Strength: strength})
	}
	return tx
}

func githubReportLeaseNow(tx *gorm.DB, effect *OutboxEffect) (time.Time, error) {
	now, err := databaseTransactionNow(tx, time.Now().UTC())
	if err != nil {
		return time.Time{}, err
	}
	if effect.Status != OutboxEffectProcessing || effect.WriterEpoch <= 0 || strings.TrimSpace(effect.LeaseID) == "" ||
		effect.LeaseExpiresAt == nil || !effect.LeaseExpiresAt.After(now) {
		return time.Time{}, ErrGithubReportCreateClaim
	}
	return now, nil
}

func validGithubReportAttempt(attempt *GithubReportCreateAttempt, effect *OutboxEffect) bool {
	return attempt.EffectID == effect.ID && attempt.PayloadSHA256 == effect.PayloadSHA256 &&
		attempt.ControlOperationID == effect.ControlOperationID && attempt.EffectKey == effect.EffectKey &&
		attempt.WriterEpoch > 0 && attempt.WriterEpoch <= effect.WriterEpoch && strings.TrimSpace(attempt.LeaseID) != "" && !attempt.CreatedAt.IsZero()
}

func validateGithubReportEffectTx(tx *gorm.DB, effect *OutboxEffect, requireActiveAuthorization bool) (GithubReportCreatePayload, error) {
	var empty GithubReportCreatePayload
	if effect.EffectKind != GithubReportCreateEffectKind || !effect.ValidPayloadDigest() {
		return empty, ErrGithubReportCreateConflict
	}
	payload, err := DecodeGithubReportCreatePayload(effect.Payload)
	if err != nil {
		return empty, err
	}
	// Resolve the batch before locking operation rows, matching execution workers.
	var route ControlOperation
	if err := tx.First(&route, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
		return empty, err
	}
	var batch DiggerBatch
	var jobRoute DiggerJob
	switch route.OperationKind {
	case "digger_job":
		if err := tx.First(&jobRoute, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
			return empty, err
		}
		if jobRoute.BatchID == nil {
			return empty, ErrGithubReportCreateConflict
		}
		if err := githubReportLock(tx, "UPDATE").First(&batch, "id = ?", *jobRoute.BatchID).Error; err != nil {
			return empty, err
		}
	case "digger_batch":
		if err := githubReportLock(tx, "UPDATE").First(&batch, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
			return empty, err
		}
	case "github_webhook_delivery":
	default:
		return empty, ErrGithubReportCreateConflict
	}
	var source ControlOperation
	if route.OperationKind == "github_webhook_delivery" {
		if err := githubReportLock(tx, "SHARE").First(&source, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
			return empty, err
		}
	} else {
		jobs, operations, tokens, links, err := lockDurableBatchGraphTx(tx, &batch)
		if err != nil {
			return empty, fmt.Errorf("%w: %w", ErrGithubReportCreateConflict, err)
		}
		if _, err := validateDurableCallbackGraph(&batch, jobs, operations, tokens, links); err != nil {
			return empty, fmt.Errorf("%w: %w", ErrGithubReportCreateConflict, err)
		}
		lockedSource := durableOperationByID(operations, effect.ControlOperationID)
		if lockedSource == nil {
			return empty, ErrGithubReportCreateConflict
		}
		source = *lockedSource
		if route.OperationKind == "digger_job" {
			job := durableJobByID(jobs, jobRoute.ID)
			if job == nil || job.OperationID == nil || *job.OperationID != effect.ControlOperationID {
				return empty, ErrGithubReportCreateConflict
			}
		}
		for _, token := range tokens {
			if token.OrganisationID != payload.OrganisationID {
				return empty, ErrGithubReportCreateConflict
			}
		}
	}
	if source.OperationKind != route.OperationKind || source.GithubDeliveryID == nil || source.ProtocolVersion != operation.ProtocolVersion ||
		source.WriterEpoch <= 0 || source.WriterEpoch > effect.WriterEpoch {
		return empty, ErrGithubReportCreateConflict
	}
	if source.OperationKind != "github_webhook_delivery" {
		if batch.OperationID == nil || batch.WriterEpoch == nil || batch.ProtocolVersion != source.ProtocolVersion ||
			batch.VCS != DiggerVCSGithub || batch.GithubInstallationId != payload.GithubInstallationID ||
			batch.RepoOwner != payload.RepoOwner || batch.RepoName != payload.RepoName || batch.RepoFullName != payload.RepoOwner+"/"+payload.RepoName ||
			batch.PrNumber != payload.PullRequestNumber || (payload.ResourceKind == GithubReportResourceCheckRun && batch.CommitSha != payload.HeadSHA) {
			return empty, ErrGithubReportCreateConflict
		}
	}
	var delivery GithubWebhookDelivery
	if err := githubReportLock(tx, "SHARE").First(&delivery, "delivery_id = ?", *source.GithubDeliveryID).Error; err != nil {
		return empty, err
	}
	if delivery.PayloadSHA256 != payloadSHA256(delivery.Payload) || delivery.GithubAppID != payload.GithubAppID ||
		delivery.InstallationID == nil || *delivery.InstallationID != payload.GithubInstallationID || delivery.RepositoryFullName != payload.RepoOwner+"/"+payload.RepoName {
		return empty, ErrGithubReportCreateConflict
	}
	deliveryID, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
	if err != nil || delivery.OperationID != deliveryID.String() {
		return empty, ErrGithubReportCreateConflict
	}
	var deliveryOperation ControlOperation
	if err := githubReportLock(tx, "SHARE").First(&deliveryOperation, "operation_id = ?", delivery.OperationID).Error; err != nil {
		return empty, err
	}
	if deliveryOperation.OperationKind != "github_webhook_delivery" || deliveryOperation.GithubDeliveryID == nil ||
		*deliveryOperation.GithubDeliveryID != delivery.DeliveryID || deliveryOperation.ProtocolVersion != operation.ProtocolVersion || deliveryOperation.IdentitySHA256 != delivery.PayloadSHA256 ||
		(source.OperationKind == "github_webhook_delivery" && source.OperationID != delivery.OperationID) {
		return empty, ErrGithubReportCreateConflict
	}
	if source.OperationKind != "github_webhook_delivery" {
		expectedBatch, err := operation.DeriveBatch(operation.ID(deliveryOperation.OperationID), string(batch.BatchType), batch.RepoFullName, batch.PrNumber, batch.CommitSha)
		if err != nil || batch.OperationID == nil || *batch.OperationID != expectedBatch.String() {
			return empty, ErrGithubReportCreateConflict
		}
	}
	selected, err := loadGithubDeliveryTargetIntentTx(tx, JobCreationIdentity{DeliveryOperationID: delivery.OperationID, WriterEpoch: effect.WriterEpoch}, &delivery, payload.OrganisationID)
	if err != nil {
		return empty, fmt.Errorf("%w: %w", ErrGithubReportCreateConflict, err)
	}
	if selected.RepoOwner != payload.RepoOwner || selected.RepoName != payload.RepoName || selected.PullRequestNumber != payload.PullRequestNumber ||
		(payload.ResourceKind == GithubReportResourceCheckRun && selected.HeadSHA != payload.HeadSHA) {
		return empty, ErrGithubReportCreateConflict
	}
	// Only a new create permit needs current authorization. A committed permit
	// must remain recoverable if installation access changes after the POST.
	if !requireActiveAuthorization {
		return payload, nil
	}
	var links []GithubAppInstallationLink
	if err := githubReportLock(tx, "SHARE").Where("github_installation_id = ? AND status = ?", payload.GithubInstallationID, GithubAppInstallationLinkActive).Find(&links).Error; err != nil {
		return empty, err
	}
	if len(links) != 1 || links[0].OrganisationId != payload.OrganisationID {
		return empty, ErrGithubReportCreateConflict
	}
	var organisation Organisation
	if err := githubReportLock(tx, "SHARE").First(&organisation, "id = ?", payload.OrganisationID).Error; err != nil {
		return empty, err
	}
	return payload, nil
}

func completeGithubReportCreateTx(tx *gorm.DB, effect *OutboxEffect, raw []byte) error {
	if effect.EffectKind != GithubReportCreateEffectKind {
		var attempts int64
		if err := tx.Model(&GithubReportCreateAttempt{}).Where("effect_id = ?", effect.ID).Count(&attempts).Error; err != nil {
			return err
		}
		if attempts != 0 {
			return ErrGithubReportCreateConflict
		}
		return nil
	}
	var attempt GithubReportCreateAttempt
	if err := githubReportLock(tx, "SHARE").First(&attempt, "effect_id = ?", effect.ID).Error; err != nil {
		return err
	}
	if !validGithubReportAttempt(&attempt, effect) {
		return ErrGithubReportCreateConflict
	}
	payload, err := validateGithubReportEffectTx(tx, effect, false)
	if err != nil {
		return err
	}
	var receipt GithubReportCreateReceipt
	if len(raw) > GithubReportCreateMaxBytes {
		return ErrGithubReportCreateConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return ErrGithubReportCreateConflict
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrGithubReportCreateConflict
	}
	if err := validateGithubReportReceipt(receipt, effect, payload); err != nil {
		return err
	}
	providerIdentity, err := githubReportProviderIdentitySHA256(payload, receipt.ProviderID)
	if err != nil {
		return err
	}
	var existing GithubReportReceipt
	err = githubReportLock(tx, "SHARE").First(&existing, "effect_id = ?", effect.ID).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	now, clockErr := githubReportLeaseNow(tx, effect)
	if clockErr != nil {
		return clockErr
	}
	if err == nil {
		if existing.value() != receipt || existing.ProviderIdentitySHA256 != providerIdentity {
			return ErrGithubReportCreateConflict
		}
		return nil
	}
	return tx.Create(&GithubReportReceipt{EffectID: receipt.EffectID, PayloadSHA256: receipt.PayloadSHA256,
		ProviderIdentitySHA256: providerIdentity,
		ResourceKind:           receipt.ResourceKind, ProviderID: receipt.ProviderID, ProviderURL: receipt.ProviderURL, CreatedAt: now}).Error
}

func githubReportProviderIdentitySHA256(payload GithubReportCreatePayload, providerID int64) (string, error) {
	if providerID <= 0 || validateGithubReportCreatePayload(payload) != nil {
		return "", ErrGithubReportCreateConflict
	}
	canonical, err := json.Marshal([]any{"github.com", strings.ToLower(payload.RepoOwner), strings.ToLower(payload.RepoName), payload.ResourceKind, providerID})
	if err != nil {
		return "", ErrGithubReportCreateConflict
	}
	return payloadSHA256(canonical), nil
}

func (receipt GithubReportReceipt) value() GithubReportCreateReceipt {
	return GithubReportCreateReceipt{EffectID: receipt.EffectID, PayloadSHA256: receipt.PayloadSHA256,
		ResourceKind: receipt.ResourceKind, ProviderID: receipt.ProviderID, ProviderURL: receipt.ProviderURL}
}

func validateGithubReportReceipt(receipt GithubReportCreateReceipt, effect *OutboxEffect, payload GithubReportCreatePayload) error {
	if receipt.EffectID != effect.ID || receipt.PayloadSHA256 != effect.PayloadSHA256 || receipt.ResourceKind != payload.ResourceKind || receipt.ProviderID <= 0 {
		return ErrGithubReportCreateConflict
	}
	expected, err := GithubReportProviderURL(payload, receipt.ProviderID)
	if err != nil || receipt.ProviderURL != expected {
		return ErrGithubReportCreateConflict
	}
	return nil
}

// GithubReportProviderURL binds a verified provider ID to its repository and PR.
func GithubReportProviderURL(payload GithubReportCreatePayload, providerID int64) (string, error) {
	if providerID <= 0 || validateGithubReportCreatePayload(payload) != nil {
		return "", ErrGithubReportCreateConflict
	}
	base := fmt.Sprintf("https://github.com/%s/%s/pull/%d", payload.RepoOwner, payload.RepoName, payload.PullRequestNumber)
	if payload.ResourceKind == GithubReportResourceComment {
		return fmt.Sprintf("%s#issuecomment-%d", base, providerID), nil
	}
	return fmt.Sprintf("%s/checks?check_run_id=%d", base, providerID), nil
}
