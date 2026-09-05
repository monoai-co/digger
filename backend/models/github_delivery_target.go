package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

var ErrGithubDeliveryTargetConflict = errors.New("github delivery target conflicts with immutable selection")
var ErrGithubDeliveryTargetNotFound = errors.New("github delivery target has not been selected")

type GithubDeliveryTarget struct {
	DeliveryOperationID   string            `gorm:"type:text;primaryKey"`
	DeliveryOperation     *ControlOperation `gorm:"foreignKey:DeliveryOperationID;references:OperationID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	OrganisationID        uint              `gorm:"not null"`
	Organisation          *Organisation     `gorm:"foreignKey:OrganisationID;references:ID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	Target                json.RawMessage   `gorm:"type:jsonb;not null"`
	TargetSHA256          string            `gorm:"type:text;not null;check:github_delivery_target_digest_check,length(target_sha256) = 64"`
	DeliveryPayloadSHA256 string            `gorm:"type:text;not null"`
	WriterEpoch           int64             `gorm:"not null"`
	CreatedAt             time.Time         `gorm:"not null"`
}

func (GithubDeliveryTarget) TableName() string { return "github_delivery_targets" }

func DecodeGithubDeliveryTarget(raw []byte) (GithubDeliveryTargetIntent, error) {
	var target GithubDeliveryTargetIntent
	if !utf8.Valid(raw) {
		return target, ErrGithubDeliveryTargetIntent
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return target, ErrGithubDeliveryTargetIntent
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return target, ErrGithubDeliveryTargetIntent
	}
	if target.RepositoryID <= 0 || !validGithubReportPathSegment(target.RepoOwner) || !validGithubReportPathSegment(target.RepoName) ||
		target.PullRequestNumber <= 0 || !validGithubReportPathSegment(target.HeadSHA) || !validGithubDeliveryHeadRef(target.HeadRef) || !validGithubDeliveryBase(target) {
		return target, ErrGithubDeliveryTargetIntent
	}
	if target.Source != GithubDeliveryTargetSignedPullRequest && target.Source != GithubDeliveryTargetIssueCommentLookup && target.Source != GithubDeliveryTargetLegacyCheckAction {
		return target, ErrGithubDeliveryTargetUnsupported
	}
	return target, nil
}

func (db *Database) RecordGithubDeliveryTarget(ctx context.Context, identity JobCreationIdentity, intent GithubDeliveryTargetIntent) (*GithubDeliveryTarget, bool, error) {
	var result *GithubDeliveryTarget
	created := false
	err := db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, _, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		preparation, err := PrepareGithubDeliveryTargetIntent(delivery)
		if err != nil {
			return err
		}
		if err := preparation.ValidateIntent(intent); err != nil {
			return err
		}
		canonical, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		var existing GithubDeliveryTarget
		err = tx.First(&existing, "delivery_operation_id = ?", identity.DeliveryOperationID).Error
		if err == nil {
			if err := validateStoredGithubDeliveryTarget(identity, &existing, delivery, orgID, preparation); err != nil {
				return err
			}
			if existing.TargetSHA256 != payloadSHA256(canonical) {
				return ErrGithubDeliveryTargetConflict
			}
			if err := githubDeliveryTargetLeaseNow(tx, delivery); err != nil {
				return err
			}
			result = &existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if intent.Source == GithubDeliveryTargetLegacyCheckAction {
			resolved, err := (&Database{GormDB: tx}).ResolveGithubCheckDeliveryTarget(ctx, delivery)
			if err != nil {
				return err
			}
			if resolved != intent {
				return ErrGithubDeliveryTargetConflict
			}
		}
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if !delivery.LeaseExpiresAt.After(now) {
			return ErrGithubSubmissionClaim
		}
		target := &GithubDeliveryTarget{DeliveryOperationID: identity.DeliveryOperationID, OrganisationID: orgID,
			Target: canonical, TargetSHA256: payloadSHA256(canonical), DeliveryPayloadSHA256: delivery.PayloadSHA256,
			WriterEpoch: identity.WriterEpoch, CreatedAt: now}
		if err := tx.Omit("DeliveryOperation", "Organisation").Create(target).Error; err != nil {
			return err
		}
		result, created = target, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, created, nil
}

func (db *Database) GetGithubDeliveryTarget(ctx context.Context, identity JobCreationIdentity) (*GithubDeliveryTarget, error) {
	var target GithubDeliveryTarget
	err := db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, _, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		preparation, err := PrepareGithubDeliveryTargetIntent(delivery)
		if err != nil {
			return err
		}
		if err := tx.First(&target, "delivery_operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrGithubDeliveryTargetNotFound
			}
			return err
		}
		if err := validateStoredGithubDeliveryTarget(identity, &target, delivery, orgID, preparation); err != nil {
			return err
		}
		return githubDeliveryTargetLeaseNow(tx, delivery)
	})
	if err != nil {
		return nil, err
	}
	return &target, nil
}

func validateStoredGithubDeliveryTarget(identity JobCreationIdentity, stored *GithubDeliveryTarget, delivery *GithubWebhookDelivery, orgID uint, preparation *GithubDeliveryTargetPreparation) error {
	intent, err := DecodeGithubDeliveryTarget(stored.Target)
	if err != nil {
		return ErrGithubDeliveryTargetConflict
	}
	if err := preparation.ValidateIntent(intent); err != nil {
		return ErrGithubDeliveryTargetConflict
	}
	canonical, err := json.Marshal(intent)
	if err != nil || stored.DeliveryOperationID != identity.DeliveryOperationID || stored.OrganisationID != orgID ||
		stored.TargetSHA256 != payloadSHA256(canonical) || stored.DeliveryPayloadSHA256 != delivery.PayloadSHA256 ||
		stored.WriterEpoch <= 0 || stored.WriterEpoch > identity.WriterEpoch || stored.CreatedAt.IsZero() {
		return ErrGithubDeliveryTargetConflict
	}
	stored.Target, stored.CreatedAt = canonical, stored.CreatedAt.UTC()
	return nil
}

// Read immutable selection without requiring a live delivery lease. Outbox
// recovery can outlive both the delivery lease and installation authorization.
func loadGithubDeliveryTargetIntentTx(tx *gorm.DB, identity JobCreationIdentity, delivery *GithubWebhookDelivery, orgID uint) (GithubDeliveryTargetIntent, error) {
	var empty GithubDeliveryTargetIntent
	preparation, err := PrepareGithubDeliveryTargetIntent(delivery)
	if err != nil {
		return empty, err
	}
	var target GithubDeliveryTarget
	if err := tx.First(&target, "delivery_operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return empty, ErrGithubDeliveryTargetNotFound
		}
		return empty, err
	}
	if err := validateStoredGithubDeliveryTarget(identity, &target, delivery, orgID, preparation); err != nil {
		return empty, err
	}
	return DecodeGithubDeliveryTarget(target.Target)
}

// ValidateDurableGraphTargetTx binds graph creation to the first selected target.
// The caller must hold the authoritative writer transaction and delivery lease.
func ValidateDurableGraphTargetTx(tx *gorm.DB, identity JobCreationIdentity, delivery *GithubWebhookDelivery, graph DurableGraphIntent) error {
	selected, err := loadGithubDeliveryTargetIntentTx(tx, identity, delivery, graph.OrganisationID)
	if err != nil {
		return err
	}
	if graph.PullRequestNumber != selected.PullRequestNumber || graph.CommitSHA != selected.HeadSHA || graph.Branch != selected.HeadRef ||
		graph.RepoOwner != selected.RepoOwner || graph.RepoName != selected.RepoName {
		return ErrGithubDeliveryTargetConflict
	}
	return nil
}

func githubDeliveryTargetLeaseNow(tx *gorm.DB, delivery *GithubWebhookDelivery) error {
	now, err := databaseTransactionNow(tx, time.Now().UTC())
	if err != nil {
		return err
	}
	if delivery.LeaseExpiresAt == nil || !delivery.LeaseExpiresAt.After(now) {
		return ErrGithubSubmissionClaim
	}
	return nil
}
