package models

import (
	"context"
	"errors"
	"sort"

	"gorm.io/gorm"
)

var ErrGithubSubmissionReportFailed = errors.New("required github submission report failed")

type GithubSubmissionReportReceipt struct {
	Report  GithubSubmissionReport
	Receipt GithubReportCreateReceipt
}

// ReadGithubSubmissionReportReceipts resolves saved report bindings without
// changing graph identity. Ready means every required report has a receipt;
// optional reports are included only after their successful creation.
func (db *Database) ReadGithubSubmissionReportReceipts(ctx context.Context, identity JobCreationIdentity) ([]GithubSubmissionReportReceipt, bool, error) {
	results := make([]GithubSubmissionReportReceipt, 0)
	ready := true
	err := db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, _, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		var submission GithubSubmission
		if err := tx.First(&submission, "delivery_operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
			return err
		}
		if err := validateStoredGithubSubmission(tx, identity, &submission, delivery, orgID); err != nil {
			return err
		}
		intent, err := DecodeGithubSubmissionIntent(submission.Intent)
		if err != nil {
			return err
		}
		for _, report := range intent.Reports {
			// Do not lock outbox rows while holding the delivery lock: report
			// workers acquire these in the opposite order. Immutable receipts
			// and atomic completion allow a conservative MVCC read instead.
			var effect OutboxEffect
			err := tx.Where("operation_id = ? AND effect_kind = ? AND effect_key = ?", identity.DeliveryOperationID, GithubReportCreateEffectKind, report.Key).First(&effect).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				ready = ready && report.Optional
				continue
			}
			if err != nil {
				return err
			}
			if !effect.ValidPayloadDigest() || effect.PayloadSHA256 != payloadSHA256(report.Payload) || effect.WriterEpoch <= 0 || effect.WriterEpoch > identity.WriterEpoch {
				return ErrGithubReportCreateConflict
			}
			switch effect.Status {
			case OutboxEffectPending, OutboxEffectProcessing, OutboxEffectRetrying:
				ready = ready && report.Optional
				continue
			case OutboxEffectDeadLetter:
				if !report.Optional {
					return ErrGithubSubmissionReportFailed
				}
				continue
			case OutboxEffectSucceeded:
			default:
				return ErrGithubReportCreateConflict
			}
			var attempt GithubReportCreateAttempt
			var receipt GithubReportReceipt
			if err := tx.First(&attempt, "effect_id = ?", effect.ID).Error; err != nil {
				return err
			}
			if err := tx.First(&receipt, "effect_id = ?", effect.ID).Error; err != nil {
				return err
			}
			payload, err := DecodeGithubReportCreatePayload(report.Payload)
			if err != nil {
				return err
			}
			value := receipt.value()
			providerIdentity, err := githubReportProviderIdentitySHA256(payload, value.ProviderID)
			var completed GithubReportCreateReceipt
			if err != nil || !validGithubReportAttempt(&attempt, &effect) || receipt.CreatedAt.IsZero() ||
				validateGithubReportReceipt(value, &effect, payload) != nil || receipt.ProviderIdentitySHA256 != providerIdentity ||
				len(effect.ProviderReceipt) > GithubReportCreateMaxBytes || decodeGithubSubmissionJSON(effect.ProviderReceipt, &completed) != nil || completed != value {
				return ErrGithubReportCreateConflict
			}
			results = append(results, GithubSubmissionReportReceipt{Report: report, Receipt: value})
		}
		return githubDeliveryTargetLeaseNow(tx, delivery)
	})
	if err != nil {
		return nil, false, err
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Report.Order < results[j].Report.Order })
	return results, ready, nil
}
