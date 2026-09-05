package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGithubSubmissionIntent = errors.New("github submission intent is invalid")
var ErrGithubSubmissionConflict = errors.New("github submission conflicts with immutable preparation")
var ErrGithubSubmissionClaim = errors.New("github submission delivery lease is not owned by this writer")
var ErrGithubSubmissionTenant = errors.New("github submission tenant does not match its delivery")

type GithubSubmissionSource struct {
	Location string   `json:"location"`
	Projects []string `json:"projects"`
}

type GithubSubmissionIntent struct {
	Graph   DurableGraphIntent       `json:"graph"`
	Sources []GithubSubmissionSource `json:"sources"`
}

// GithubSubmission preserves the first selected execution and reporting inputs.
// Report receipts and current delivery leases are not part of this immutable row.
type GithubSubmission struct {
	DeliveryOperationID   string            `gorm:"type:text;primaryKey"`
	DeliveryOperation     *ControlOperation `gorm:"foreignKey:DeliveryOperationID;references:OperationID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	OrganisationID        uint              `gorm:"not null"`
	Organisation          *Organisation     `gorm:"foreignKey:OrganisationID;references:ID;belongsTo;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	Intent                json.RawMessage   `gorm:"type:jsonb;not null"`
	IntentSHA256          string            `gorm:"type:text;not null;check:github_submission_intent_digest_check,length(intent_sha256) = 64"`
	DeliveryPayloadSHA256 string            `gorm:"type:text;not null"`
	WriterEpoch           int64             `gorm:"not null"`
	CreatedAt             time.Time         `gorm:"not null"`
}

func (GithubSubmission) TableName() string { return "github_submissions" }

// DecodeGithubSubmissionIntent normalizes JSONB representation while rejecting
// unknown fields and invalid execution DAGs using the graph consumer's validator.
func DecodeGithubSubmissionIntent(raw []byte) (GithubSubmissionIntent, error) {
	var intent GithubSubmissionIntent
	if err := decodeGithubSubmissionJSON(raw, &intent); err != nil {
		return intent, err
	}
	normalized, _, err := normalizeFrozenGraphShape(intent.Graph)
	if err != nil {
		return intent, ErrGithubSubmissionIntent
	}
	intent.Graph = *normalized
	projects := make(map[string]bool, len(intent.Graph.Jobs))
	for _, job := range intent.Graph.Jobs {
		projects[job.ProjectName] = true
	}
	if intent.Sources == nil {
		intent.Sources = []GithubSubmissionSource{}
	}
	locations := make(map[string]bool, len(intent.Sources))
	for index := range intent.Sources {
		source := &intent.Sources[index]
		if strings.TrimSpace(source.Location) == "" || locations[source.Location] || len(source.Projects) == 0 {
			return intent, ErrGithubSubmissionIntent
		}
		locations[source.Location] = true
		if err := sortGithubSubmissionNames(&source.Projects); err != nil {
			return intent, err
		}
		for _, project := range source.Projects {
			if !projects[project] {
				return intent, ErrGithubSubmissionIntent
			}
		}
	}
	sort.Slice(intent.Sources, func(i, j int) bool { return intent.Sources[i].Location < intent.Sources[j].Location })
	return intent, nil
}

func decodeGithubSubmissionJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrGithubSubmissionIntent
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrGithubSubmissionIntent
	}
	return nil
}

func sortGithubSubmissionNames(names *[]string) error {
	if *names == nil {
		*names = []string{}
	}
	sort.Strings(*names)
	for index, name := range *names {
		if strings.TrimSpace(name) == "" || (index > 0 && (*names)[index-1] == name) {
			return ErrGithubSubmissionIntent
		}
	}
	return nil
}

func canonicalGithubSubmissionIntent(intent GithubSubmissionIntent) ([]byte, GithubSubmissionIntent, error) {
	encoded, err := json.Marshal(intent)
	if err != nil {
		return nil, GithubSubmissionIntent{}, ErrGithubSubmissionIntent
	}
	normalized, err := DecodeGithubSubmissionIntent(encoded)
	if err != nil {
		return nil, GithubSubmissionIntent{}, err
	}
	encoded, err = json.Marshal(normalized)
	return encoded, normalized, err
}

func (db *Database) RecordGithubSubmission(ctx context.Context, identity JobCreationIdentity, intent GithubSubmissionIntent) (*GithubSubmission, bool, error) {
	encoded, normalized, err := canonicalGithubSubmissionIntent(intent)
	if err != nil {
		return nil, false, err
	}
	var result *GithubSubmission
	created := false
	err = db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, now, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		if err := validateGithubSubmissionEnvelope(tx, identity, normalized, delivery, orgID); err != nil {
			return err
		}
		var existing GithubSubmission
		err = tx.First(&existing, "delivery_operation_id = ?", identity.DeliveryOperationID).Error
		if err == nil {
			if err := validateStoredGithubSubmission(tx, identity, &existing, delivery, orgID); err != nil {
				return err
			}
			if existing.IntentSHA256 != payloadSHA256(encoded) {
				return ErrGithubSubmissionConflict
			}
			result = &existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// VCS/organisation locks in envelope validation can wait past the lease.
		now, err = databaseTransactionNow(tx, now)
		if err != nil {
			return err
		}
		if !delivery.LeaseExpiresAt.After(now) {
			return ErrGithubSubmissionClaim
		}
		submission := &GithubSubmission{DeliveryOperationID: identity.DeliveryOperationID, OrganisationID: orgID,
			Intent: encoded, IntentSHA256: payloadSHA256(encoded), DeliveryPayloadSHA256: delivery.PayloadSHA256,
			WriterEpoch: identity.WriterEpoch, CreatedAt: now}
		if err := tx.Omit("DeliveryOperation", "Organisation").Create(submission).Error; err != nil {
			return err
		}
		result, created = submission, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, created, nil
}

func (db *Database) GetGithubSubmission(ctx context.Context, identity JobCreationIdentity) (*GithubSubmission, error) {
	var result GithubSubmission
	err := db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, _, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		if err := tx.First(&result, "delivery_operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
			return err
		}
		return validateStoredGithubSubmission(tx, identity, &result, delivery, orgID)
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func lockGithubPreparationDelivery(tx *gorm.DB, identity JobCreationIdentity) (*GithubWebhookDelivery, uint, time.Time, error) {
	if identity.ProtocolVersion != operation.ProtocolVersion {
		return nil, 0, time.Time{}, ErrControlPlaneProtocol
	}
	if !operation.ID(identity.DeliveryOperationID).Valid() || strings.TrimSpace(identity.DeliveryLeaseID) == "" {
		return nil, 0, time.Time{}, ErrGithubSubmissionClaim
	}
	var delivery GithubWebhookDelivery
	query := tx
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.First(&delivery, "operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
		return nil, 0, time.Time{}, err
	}
	if delivery.ProcessingStatus != GithubWebhookDeliveryProcessing || delivery.LeaseID != identity.DeliveryLeaseID ||
		delivery.WriterEpoch == nil || *delivery.WriterEpoch != identity.WriterEpoch || delivery.LeaseExpiresAt == nil {
		return nil, 0, time.Time{}, ErrGithubSubmissionClaim
	}
	expectedID, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
	if err != nil || expectedID.String() != identity.DeliveryOperationID || delivery.PayloadSHA256 != payloadSHA256(delivery.Payload) {
		return nil, 0, time.Time{}, ErrGithubSubmissionConflict
	}
	var deliveryOperation ControlOperation
	operationQuery := tx
	if tx.Dialector.Name() == "postgres" {
		operationQuery = operationQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := operationQuery.First(&deliveryOperation, "operation_id = ?", identity.DeliveryOperationID).Error; err != nil {
		return nil, 0, time.Time{}, err
	}
	if deliveryOperation.ProtocolVersion != identity.ProtocolVersion {
		return nil, 0, time.Time{}, ErrControlPlaneProtocol
	}
	if deliveryOperation.OperationKind != "github_webhook_delivery" || deliveryOperation.GithubDeliveryID == nil ||
		*deliveryOperation.GithubDeliveryID != delivery.DeliveryID || deliveryOperation.IdentitySHA256 != delivery.PayloadSHA256 {
		return nil, 0, time.Time{}, ErrGithubSubmissionConflict
	}
	if delivery.InstallationID == nil || *delivery.InstallationID <= 0 || delivery.GithubAppID <= 0 || delivery.RepositoryFullName == "" {
		return nil, 0, time.Time{}, ErrGithubSubmissionTenant
	}
	var links []GithubAppInstallationLink
	linkQuery := tx.Where("github_installation_id = ? AND status = ?", *delivery.InstallationID, GithubAppInstallationLinkActive)
	if tx.Dialector.Name() == "postgres" {
		linkQuery = linkQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := linkQuery.Find(&links).Error; err != nil {
		return nil, 0, time.Time{}, err
	}
	if len(links) != 1 || links[0].OrganisationId == 0 {
		return nil, 0, time.Time{}, ErrGithubSubmissionTenant
	}
	var org Organisation
	orgQuery := tx
	if tx.Dialector.Name() == "postgres" {
		orgQuery = orgQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := orgQuery.First(&org, "id = ?", links[0].OrganisationId).Error; err != nil {
		return nil, 0, time.Time{}, ErrGithubSubmissionTenant
	}
	now, err := databaseTransactionNow(tx, time.Now().UTC())
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	if !delivery.LeaseExpiresAt.After(now) {
		return nil, 0, time.Time{}, ErrGithubSubmissionClaim
	}
	return &delivery, org.ID, now, nil
}

func validateGithubSubmissionEnvelope(tx *gorm.DB, identity JobCreationIdentity, intent GithubSubmissionIntent, delivery *GithubWebhookDelivery, orgID uint) error {
	graph := intent.Graph
	if graph.ProtocolVersion != identity.ProtocolVersion {
		return ErrControlPlaneProtocol
	}
	if _, _, err := NormalizeDurableGraphIntent(identity.DeliveryOperationID, graph); err != nil {
		return ErrGithubSubmissionIntent
	}
	if graph.OrganisationID != orgID || graph.GithubInstallationID != *delivery.InstallationID ||
		graph.RepoFullName != delivery.RepositoryFullName || graph.RepoOwner == "" || graph.RepoName == "" ||
		graph.RepoOwner+"/"+graph.RepoName != graph.RepoFullName {
		return ErrGithubSubmissionTenant
	}
	if graph.VCSConnectionID != nil {
		var connection VCSConnection
		query := tx
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if err := query.First(&connection, "id = ?", *graph.VCSConnectionID).Error; err != nil {
			return ErrGithubSubmissionTenant
		}
		if connection.OrganisationID != orgID || connection.GithubId != delivery.GithubAppID || connection.VCSType != DiggerVCSGithub {
			return ErrGithubSubmissionTenant
		}
	}
	return nil
}

func validateStoredGithubSubmission(tx *gorm.DB, identity JobCreationIdentity, stored *GithubSubmission, delivery *GithubWebhookDelivery, orgID uint) error {
	intent, err := DecodeGithubSubmissionIntent(stored.Intent)
	if err != nil {
		return ErrGithubSubmissionConflict
	}
	canonical, err := json.Marshal(intent)
	if err != nil || stored.DeliveryOperationID != identity.DeliveryOperationID || stored.OrganisationID != orgID ||
		stored.DeliveryPayloadSHA256 != delivery.PayloadSHA256 || stored.IntentSHA256 != payloadSHA256(canonical) ||
		stored.WriterEpoch <= 0 || stored.WriterEpoch > identity.WriterEpoch {
		return ErrGithubSubmissionConflict
	}
	if err := validateGithubSubmissionEnvelope(tx, identity, intent, delivery, orgID); err != nil {
		return err
	}
	now, err := databaseTransactionNow(tx, time.Now().UTC())
	if err != nil {
		return err
	}
	if !delivery.LeaseExpiresAt.After(now) {
		return ErrGithubSubmissionClaim
	}
	stored.Intent = canonical
	stored.CreatedAt = stored.CreatedAt.UTC()
	return nil
}
