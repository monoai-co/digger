package models

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const GithubWorkflowDispatchEffectKind = "github_workflow_dispatch"

// Leave room within the hosted-runner job limit for terminal reporting. This
// deadline starts at first dispatch preparation and is never extended by retry.
const DurableExecutionClaimWindow = 5 * time.Hour

var ErrDurableJobDispatchClaim = errors.New("durable job dispatch is not owned by this outbox claim")
var ErrDurableJobDispatchConflict = errors.New("durable job dispatch state does not match its bound operation")
var ErrDurableJobDispatchNotReady = errors.New("durable job dispatch is not yet committed")

type DurableJobDispatchPreparation struct {
	Job            *DiggerJob
	GithubAppID    int64
	SkipProvider   bool
	ClaimExpiresAt time.Time
}

type DurableExecutionClaimRequest struct {
	OperationID         string
	DiggerJobID         string
	RepositoryFullName  string
	ProjectName         string
	RunID               int64
	RunAttempt          int64
	RepositoryID        int64
	OIDCIssuer          string
	OIDCAudience        string
	OIDCSubject         string
	WorkflowRef         string
	WorkflowSHA         string
	ActionRef           string
	CLISHA256           string
	ProtocolVersion     int
	DispatchWriterEpoch int64
}

type DurableExecutionClaimReceipt struct {
	Granted        bool
	AlreadyGranted bool
	ExecutionGrant string
	SigningKeyID   string
	GrantExpiresAt time.Time
}

type GithubWorkflowDispatchPayload struct {
	OperationID string `json:"operation_id"`
	DiggerJobID string `json:"digger_job_id"`
}

type durablePersistedJobIntent struct {
	DiggerJobID            string          `json:"digger_job_id"`
	OperationID            string          `json:"operation_id"`
	ProtocolVersion        int             `json:"protocol_version"`
	WriterEpoch            int64           `json:"writer_epoch"`
	ProjectName            string          `json:"project_name"`
	BatchID                string          `json:"batch_id"`
	CheckRunID             *string         `json:"check_run_id"`
	CheckRunURL            *string         `json:"check_run_url"`
	SerializedJobSpec      json.RawMessage `json:"serialized_job_spec"`
	DependencyOperationIDs json.RawMessage `json:"dependency_operation_ids"`
	WorkflowFile           string          `json:"workflow_file"`
	ReporterType           string          `json:"reporter_type"`
}

type durableWorkflowDispatchState struct {
	Job            DiggerJob
	JobOperation   ControlOperation
	Batch          DiggerBatch
	BatchOperation ControlOperation
	Delivery       GithubWebhookDelivery
	Organisation   Organisation
	Token          JobToken
}

func DurableJobIntentSHA256(job *DiggerJob) (string, error) {
	if job == nil || job.OperationID == nil || job.WriterEpoch == nil || job.BatchID == nil {
		return "", ErrDurableJobDispatchConflict
	}
	var jobSpec scheduler.JobJson
	if err := json.Unmarshal(job.SerializedJobSpec, &jobSpec); err != nil {
		return "", err
	}
	jobSpec.BackendJobToken = ""
	normalizedSpec, err := json.Marshal(jobSpec)
	if err != nil {
		return "", err
	}
	dependencyOperationIDs, err := CanonicalDependencyOperationIDs(job.DependencyOperationIDs)
	if err != nil {
		return "", err
	}
	intent := durablePersistedJobIntent{
		DiggerJobID:            job.DiggerJobID,
		OperationID:            *job.OperationID,
		ProtocolVersion:        job.ProtocolVersion,
		WriterEpoch:            *job.WriterEpoch,
		ProjectName:            job.ProjectName,
		BatchID:                *job.BatchID,
		CheckRunID:             job.CheckRunId,
		CheckRunURL:            job.CheckRunUrl,
		SerializedJobSpec:      normalizedSpec,
		DependencyOperationIDs: dependencyOperationIDs,
		WorkflowFile:           job.WorkflowFile,
		ReporterType:           job.ReporterType,
	}
	serializedIntent, err := json.Marshal(intent)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(serializedIntent)
	return hex.EncodeToString(digest[:]), nil
}

func CanonicalDependencyOperationIDs(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var operationIDs []string
	if err := decoder.Decode(&operationIDs); err != nil {
		return nil, ErrDurableJobDispatchConflict
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, ErrDurableJobDispatchConflict
	}
	for index, operationID := range operationIDs {
		if !operation.ID(operationID).Valid() || index > 0 && operationIDs[index-1] >= operationID {
			return nil, ErrDurableJobDispatchConflict
		}
	}
	return json.Marshal(operationIDs)
}

// loadDurableWorkflowDispatchStateTx validates the complete immutable route
// from an outbox effect through its job and batch operations to the signed
// GitHub delivery and active tenant binding. Callers must already hold the
// effect row lock in the same transaction. The batch is the graph mutex and is
// acquired before any operation, job, or token row.
func loadDurableWorkflowDispatchStateTx(tx *gorm.DB, effect *OutboxEffect) (*durableWorkflowDispatchState, error) {
	if effect == nil || effect.EffectKind != GithubWorkflowDispatchEffectKind || effect.ControlOperationID == "" ||
		effect.EffectKey != "job:"+effect.ControlOperationID || !effect.ValidPayloadDigest() {
		return nil, ErrDurableJobDispatchConflict
	}

	var route DiggerJob
	if err := tx.Select("id", "batch_id").First(&route, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch route: %w", err)
	}
	if route.BatchID == nil {
		return nil, ErrDurableJobDispatchConflict
	}

	var batch DiggerBatch
	batchQuery := tx
	if tx.Dialector.Name() == "postgres" {
		batchQuery = batchQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := batchQuery.First(&batch, "id = ?", *route.BatchID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch batch: %w", err)
	}

	var jobOperation ControlOperation
	operationQuery := tx
	if tx.Dialector.Name() == "postgres" {
		operationQuery = operationQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := operationQuery.First(&jobOperation, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch operation: %w", err)
	}
	if jobOperation.OperationKind != "digger_job" || jobOperation.GithubDeliveryID == nil {
		return nil, ErrDurableJobDispatchConflict
	}

	var job DiggerJob
	jobQuery := tx
	if tx.Dialector.Name() == "postgres" {
		jobQuery = jobQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := jobQuery.First(&job, "operation_id = ?", effect.ControlOperationID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch job: %w", err)
	}
	if job.ID != route.ID || job.OperationID == nil || *job.OperationID != effect.ControlOperationID || job.BatchID == nil || *job.BatchID != *route.BatchID ||
		job.ProtocolVersion != jobOperation.ProtocolVersion || job.WriterEpoch == nil || *job.WriterEpoch != jobOperation.WriterEpoch {
		return nil, ErrDurableJobDispatchConflict
	}
	jobIntentSHA256, err := DurableJobIntentSHA256(&job)
	if err != nil || jobOperation.IdentitySHA256 != jobIntentSHA256 {
		return nil, ErrDurableJobDispatchConflict
	}

	if batch.OperationID == nil || batch.WriterEpoch == nil || batch.ID.String() != *job.BatchID ||
		batch.ProtocolVersion != job.ProtocolVersion || *batch.WriterEpoch != *job.WriterEpoch {
		return nil, ErrDurableJobDispatchConflict
	}

	var batchOperation ControlOperation
	batchOperationQuery := tx
	if tx.Dialector.Name() == "postgres" {
		batchOperationQuery = batchOperationQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := batchOperationQuery.First(&batchOperation, "operation_id = ?", *batch.OperationID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch batch operation: %w", err)
	}
	if batchOperation.OperationKind != "digger_batch" || batchOperation.GithubDeliveryID == nil ||
		*batchOperation.GithubDeliveryID != *jobOperation.GithubDeliveryID ||
		batchOperation.ProtocolVersion != batch.ProtocolVersion || batchOperation.WriterEpoch != *batch.WriterEpoch {
		return nil, ErrDurableJobDispatchConflict
	}

	var delivery GithubWebhookDelivery
	deliveryQuery := tx
	if tx.Dialector.Name() == "postgres" {
		deliveryQuery = deliveryQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := deliveryQuery.First(&delivery, "delivery_id = ?", *jobOperation.GithubDeliveryID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch delivery: %w", err)
	}
	expectedDeliveryOperationID, err := operation.Derive("github-webhook-delivery", fmt.Sprintf("github-app:%d", delivery.GithubAppID), "delivery:"+delivery.DeliveryID)
	if err != nil || delivery.OperationID != expectedDeliveryOperationID.String() {
		return nil, ErrDurableJobDispatchConflict
	}
	expectedBatchOperationID, err := operation.DeriveBatch(expectedDeliveryOperationID, string(batch.BatchType), batch.RepoFullName, batch.PrNumber, batch.CommitSha)
	if err != nil || batchOperation.OperationID != expectedBatchOperationID.String() {
		return nil, ErrDurableJobDispatchConflict
	}
	expectedJobOperationID, err := operation.DeriveJob(expectedBatchOperationID, job.ProjectName, job.WorkflowFile)
	if err != nil || jobOperation.OperationID != expectedJobOperationID.String() {
		return nil, ErrDurableJobDispatchConflict
	}
	if delivery.InstallationID == nil || *delivery.InstallationID != batch.GithubInstallationId ||
		delivery.RepositoryFullName != batch.RepoFullName || delivery.GithubAppID <= 0 ||
		batch.VCS != DiggerVCSGithub || batch.RepoOwner == "" || batch.RepoName == "" ||
		batch.RepoOwner+"/"+batch.RepoName != batch.RepoFullName {
		return nil, ErrDurableJobDispatchConflict
	}

	var dispatchPayload GithubWorkflowDispatchPayload
	if err := json.Unmarshal(effect.Payload, &dispatchPayload); err != nil ||
		dispatchPayload.OperationID != effect.ControlOperationID || dispatchPayload.DiggerJobID != job.DiggerJobID {
		return nil, ErrDurableJobDispatchConflict
	}

	var installationLinks []GithubAppInstallationLink
	installationLinkQuery := tx.Where("github_installation_id = ? AND status = ?", *delivery.InstallationID, GithubAppInstallationLinkActive)
	if tx.Dialector.Name() == "postgres" {
		installationLinkQuery = installationLinkQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := installationLinkQuery.Find(&installationLinks).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch installation tenant link: %w", err)
	}
	if len(installationLinks) != 1 {
		return nil, ErrDurableJobDispatchConflict
	}
	var organisation Organisation
	organisationQuery := tx
	if tx.Dialector.Name() == "postgres" {
		organisationQuery = organisationQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := organisationQuery.First(&organisation, "id = ?", installationLinks[0].OrganisationId).Error; err != nil {
		return nil, ErrDurableJobDispatchConflict
	}
	if batch.VCSConnectionId != nil {
		var connection VCSConnection
		connectionQuery := tx
		if tx.Dialector.Name() == "postgres" {
			connectionQuery = connectionQuery.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if err := connectionQuery.First(&connection, "id = ?", *batch.VCSConnectionId).Error; err != nil ||
			connection.OrganisationID != organisation.ID || connection.GithubId != delivery.GithubAppID || connection.VCSType != DiggerVCSGithub {
			return nil, ErrDurableJobDispatchConflict
		}
	}

	var token JobToken
	tokenQuery := tx
	if tx.Dialector.Name() == "postgres" {
		tokenQuery = tokenQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := tokenQuery.First(&token, "digger_job_database_id = ?", job.ID).Error; err != nil {
		return nil, fmt.Errorf("load durable dispatch token: %w", err)
	}
	var jobSpec scheduler.JobJson
	if err := json.Unmarshal(job.SerializedJobSpec, &jobSpec); err != nil {
		return nil, ErrDurableJobDispatchConflict
	}
	if token.DiggerJobDatabaseID == nil || *token.DiggerJobDatabaseID != job.ID || token.OrganisationID != organisation.ID ||
		token.Type != CliJobAccessType || token.Value == "" || token.Value != jobSpec.BackendJobToken {
		return nil, ErrDurableJobDispatchConflict
	}
	if err := validateDurableJobDependenciesTx(tx, &job, &batch, &batchOperation, &organisation); err != nil {
		return nil, err
	}

	job.Batch = &batch
	return &durableWorkflowDispatchState{
		Job:            job,
		JobOperation:   jobOperation,
		Batch:          batch,
		BatchOperation: batchOperation,
		Delivery:       delivery,
		Organisation:   organisation,
		Token:          token,
	}, nil
}

func validateDurableJobDependenciesTx(tx *gorm.DB, job *DiggerJob, batch *DiggerBatch, batchOperation *ControlOperation, organisation *Organisation) error {
	canonicalOperationIDs, err := CanonicalDependencyOperationIDs(job.DependencyOperationIDs)
	if err != nil {
		return err
	}
	var expectedOperationIDs []string
	if err := json.Unmarshal(canonicalOperationIDs, &expectedOperationIDs); err != nil {
		return ErrDurableJobDispatchConflict
	}
	var links []DiggerJobParentLink
	if err := tx.Unscoped().Where("digger_job_id = ?", job.DiggerJobID).Find(&links).Error; err != nil {
		return fmt.Errorf("load durable dispatch parent links: %w", err)
	}
	if len(links) != len(expectedOperationIDs) {
		return ErrDurableJobDispatchConflict
	}
	if len(links) == 0 {
		return nil
	}
	parentPublicIDs := make([]string, 0, len(links))
	seenPublicIDs := make(map[string]struct{}, len(links))
	for _, link := range links {
		if link.DeletedAt.Valid || link.DiggerJobId != job.DiggerJobID || link.ParentDiggerJobId == "" {
			return ErrDurableJobDispatchConflict
		}
		if _, duplicate := seenPublicIDs[link.ParentDiggerJobId]; duplicate {
			return ErrDurableJobDispatchConflict
		}
		seenPublicIDs[link.ParentDiggerJobId] = struct{}{}
		parentPublicIDs = append(parentPublicIDs, link.ParentDiggerJobId)
	}
	sort.Strings(parentPublicIDs)
	var parents []DiggerJob
	parentQuery := tx.Where("digger_job_id IN ?", parentPublicIDs).Order("digger_job_id")
	if tx.Dialector.Name() == "postgres" {
		parentQuery = parentQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := parentQuery.Find(&parents).Error; err != nil {
		return fmt.Errorf("load durable dispatch parent jobs: %w", err)
	}
	if len(parents) != len(parentPublicIDs) {
		return ErrDurableJobDispatchConflict
	}
	parentOperationIDs := make([]string, 0, len(parents))
	parentByOperationID := make(map[string]*DiggerJob, len(parents))
	for index := range parents {
		parent := &parents[index]
		if parent.OperationID == nil || parent.BatchID == nil || *parent.BatchID != batch.ID.String() {
			return ErrDurableJobDispatchConflict
		}
		parentOperationIDs = append(parentOperationIDs, *parent.OperationID)
		parentByOperationID[*parent.OperationID] = parent
	}
	sort.Strings(parentOperationIDs)
	if len(parentOperationIDs) != len(expectedOperationIDs) {
		return ErrDurableJobDispatchConflict
	}
	for index := range parentOperationIDs {
		if parentOperationIDs[index] != expectedOperationIDs[index] {
			return ErrDurableJobDispatchConflict
		}
	}
	var parentOperations []ControlOperation
	parentOperationQuery := tx.Where("operation_id IN ?", parentOperationIDs).Order("operation_id")
	if tx.Dialector.Name() == "postgres" {
		parentOperationQuery = parentOperationQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := parentOperationQuery.Find(&parentOperations).Error; err != nil {
		return fmt.Errorf("load durable dispatch parent operations: %w", err)
	}
	if len(parentOperations) != len(parentOperationIDs) {
		return ErrDurableJobDispatchConflict
	}
	parentDatabaseIDs := make([]uint, 0, len(parents))
	for _, parent := range parents {
		parentDatabaseIDs = append(parentDatabaseIDs, parent.ID)
	}
	var parentTokens []JobToken
	parentTokenQuery := tx.Where("digger_job_database_id IN ?", parentDatabaseIDs).Order("digger_job_database_id")
	if tx.Dialector.Name() == "postgres" {
		parentTokenQuery = parentTokenQuery.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := parentTokenQuery.Find(&parentTokens).Error; err != nil {
		return fmt.Errorf("load durable dispatch parent tokens: %w", err)
	}
	if len(parentTokens) != len(parentDatabaseIDs) {
		return ErrDurableJobDispatchConflict
	}
	parentTokenByJobID := make(map[uint]*JobToken, len(parentTokens))
	for index := range parentTokens {
		parentToken := &parentTokens[index]
		if parentToken.DiggerJobDatabaseID == nil {
			return ErrDurableJobDispatchConflict
		}
		parentTokenByJobID[*parentToken.DiggerJobDatabaseID] = parentToken
	}
	for index := range parentOperations {
		parentOperation := &parentOperations[index]
		parent, exists := parentByOperationID[parentOperation.OperationID]
		if !exists || parent.WriterEpoch == nil || parent.ProtocolVersion != batch.ProtocolVersion || *parent.WriterEpoch != *batch.WriterEpoch ||
			parentOperation.OperationKind != "digger_job" || parentOperation.GithubDeliveryID == nil || *parentOperation.GithubDeliveryID != *batchOperation.GithubDeliveryID ||
			parentOperation.ProtocolVersion != batchOperation.ProtocolVersion || parentOperation.WriterEpoch != batchOperation.WriterEpoch ||
			parent.Status != scheduler.DiggerJobSucceeded || parentOperation.Status != ControlOperationCompleted {
			return ErrDurableJobDispatchConflict
		}
		expectedParentOperationID, deriveErr := operation.DeriveJob(operation.ID(batchOperation.OperationID), parent.ProjectName, parent.WorkflowFile)
		parentIntentSHA256, digestErr := DurableJobIntentSHA256(parent)
		parentToken := parentTokenByJobID[parent.ID]
		var parentSpec scheduler.JobJson
		parentSpecErr := json.Unmarshal(parent.SerializedJobSpec, &parentSpec)
		if deriveErr != nil || expectedParentOperationID.String() != parentOperation.OperationID || digestErr != nil || parentIntentSHA256 != parentOperation.IdentitySHA256 ||
			parentSpecErr != nil || parentToken == nil || parentToken.OrganisationID != organisation.ID || parentToken.Type != CliJobAccessType ||
			parentToken.Value == "" || parentToken.Value != parentSpec.BackendJobToken || parentToken.ActivatedAt == nil ||
			parentToken.RevokedAt == nil || parentToken.Expiry.After(*parentToken.RevokedAt) {
			return ErrDurableJobDispatchConflict
		}
	}
	return nil
}

func reconcileDurableTerminalJobTx(tx *gorm.DB, job *DiggerJob, jobOperation *ControlOperation, token *JobToken, now time.Time) error {
	if job == nil || jobOperation == nil || token == nil || job.OperationID == nil || *job.OperationID != jobOperation.OperationID ||
		token.DiggerJobDatabaseID == nil || *token.DiggerJobDatabaseID != job.ID || token.ActivatedAt == nil {
		return ErrDurableJobDispatchConflict
	}
	var terminalStatus ControlOperationStatus
	switch job.Status {
	case scheduler.DiggerJobSucceeded:
		if jobOperation.Status == ControlOperationFailed {
			return ErrDurableJobDispatchConflict
		}
		terminalStatus = ControlOperationCompleted
	case scheduler.DiggerJobFailed:
		if jobOperation.Status == ControlOperationCompleted {
			return ErrDurableJobDispatchConflict
		}
		terminalStatus = ControlOperationFailed
	default:
		return ErrDurableJobDispatchConflict
	}
	if jobOperation.Status != terminalStatus {
		result := tx.Model(&ControlOperation{}).
			Where("operation_id = ? AND status = ?", jobOperation.OperationID, jobOperation.Status).
			Updates(map[string]any{"status": terminalStatus, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobDispatchConflict
		}
		jobOperation.Status = terminalStatus
	}
	if token.RevokedAt == nil || token.Expiry.After(now) {
		result := tx.Model(&JobToken{}).
			Where("id = ? AND digger_job_database_id = ?", token.ID, job.ID).
			Updates(map[string]any{"expiry": now, "revoked_at": now, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobDispatchConflict
		}
		token.Expiry = now
		token.RevokedAt = &now
	}
	return nil
}

// PrepareDurableJobDispatch activates or extends the exact-bound job token just
// before a provider attempt. It uses the database clock and the outbox lease so
// a stale worker cannot make a token usable.
func (db *Database) PrepareDurableJobDispatch(
	ctx context.Context,
	effectID uuid.UUID,
	leaseID string,
	validity time.Duration,
	leaseDuration time.Duration,
	databaseIdentity string,
	writerEpoch int64,
) (*DurableJobDispatchPreparation, error) {
	if effectID == uuid.Nil || leaseID == "" || validity <= 0 || leaseDuration <= 0 {
		return nil, ErrDurableJobDispatchClaim
	}

	var preparation *DurableJobDispatchPreparation
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, true, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}

		var effect OutboxEffect
		effectQuery := tx
		if tx.Dialector.Name() == "postgres" {
			effectQuery = effectQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := effectQuery.First(&effect, "id = ?", effectID).Error; err != nil {
			return fmt.Errorf("load durable dispatch effect: %w", err)
		}
		if effect.Status != OutboxEffectProcessing || effect.LeaseID != leaseID || effect.WriterEpoch != writerEpoch ||
			effect.LeaseExpiresAt == nil || !effect.LeaseExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		state, err := loadDurableWorkflowDispatchStateTx(tx, &effect)
		if err != nil {
			return err
		}
		now, err = databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if effect.LeaseExpiresAt == nil || !effect.LeaseExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		leaseResult := tx.Model(&OutboxEffect{}).
			Where("id = ? AND status = ? AND lease_id = ? AND writer_epoch = ?", effect.ID, OutboxEffectProcessing, leaseID, writerEpoch).
			Updates(map[string]any{"lease_expires_at": now.Add(leaseDuration), "updated_at": now})
		if leaseResult.Error != nil {
			return leaseResult.Error
		}
		if leaseResult.RowsAffected != 1 {
			return ErrDurableJobDispatchClaim
		}
		if state.Job.Status == scheduler.DiggerJobSucceeded || state.Job.Status == scheduler.DiggerJobFailed {
			if err := reconcileDurableTerminalJobTx(tx, &state.Job, &state.JobOperation, &state.Token, now); err != nil {
				return err
			}
			preparation = &DurableJobDispatchPreparation{Job: &state.Job, GithubAppID: state.Delivery.GithubAppID, SkipProvider: true}
			return nil
		}
		if state.JobOperation.Status != ControlOperationPending || state.Token.RevokedAt != nil {
			return ErrDurableJobDispatchConflict
		}

		activatedAt := now
		if state.Token.ActivatedAt != nil {
			activatedAt = *state.Token.ActivatedAt
		}
		claimExpiresAt := activatedAt.Add(min(validity, DurableExecutionClaimWindow))
		if !claimExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		expiresAt := now.Add(validity)
		if state.Token.Expiry.After(expiresAt) {
			expiresAt = state.Token.Expiry
		}
		if claimExpiresAt.After(expiresAt) {
			claimExpiresAt = expiresAt
		}
		updates := map[string]any{"expiry": expiresAt, "updated_at": now}
		if state.Token.ActivatedAt == nil {
			updates["activated_at"] = now
		}
		result := tx.Model(&JobToken{}).
			Where("id = ? AND digger_job_database_id = ? AND revoked_at IS NULL", state.Token.ID, state.Job.ID).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobDispatchConflict
		}
		if state.Job.Status == scheduler.DiggerJobCreated || state.Job.Status == scheduler.DiggerJobQueuedForRun {
			result = tx.Model(&DiggerJob{}).
				Where("id = ? AND status IN ?", state.Job.ID, []scheduler.DiggerJobStatus{scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun}).
				Updates(map[string]any{"status": scheduler.DiggerJobTriggered, "status_version": gorm.Expr("CASE WHEN status_version < ? THEN ? ELSE status_version END", 1, 1), "status_updated_at": now, "updated_at": now})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrDurableJobDispatchConflict
			}
			state.Job.Status = scheduler.DiggerJobTriggered
			state.Job.StatusVersion = 1
		}
		preparation = &DurableJobDispatchPreparation{Job: &state.Job, GithubAppID: state.Delivery.GithubAppID, ClaimExpiresAt: claimExpiresAt}
		return nil
	})
	return preparation, err
}

func (db *Database) ClaimDurableJobExecution(
	ctx context.Context,
	request DurableExecutionClaimRequest,
	jobTokenValue string,
	grantSecrets map[string][]byte,
	activeGrantSigningKeyID string,
	databaseIdentity string,
	writerEpoch int64,
) (*DurableExecutionClaimReceipt, error) {
	activeGrantSecret := grantSecrets[activeGrantSigningKeyID]
	if !validDurableExecutionClaimRequest(request) || jobTokenValue == "" || len(activeGrantSecret) < 32 || !validGrantSigningKeyID(activeGrantSigningKeyID) {
		return nil, ErrDurableJobDispatchClaim
	}
	var receipt *DurableExecutionClaimReceipt
	err := db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, true, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if request.ProtocolVersion != operation.ProtocolVersion {
			return ErrDurableJobDispatchClaim
		}

		var effect OutboxEffect
		effectQuery := tx
		if tx.Dialector.Name() == "postgres" {
			effectQuery = effectQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := effectQuery.First(&effect, "operation_id = ? AND effect_kind = ?", request.OperationID, GithubWorkflowDispatchEffectKind).Error; err != nil {
			return fmt.Errorf("load execution claim dispatch effect: %w", err)
		}
		if effect.Status != OutboxEffectSucceeded {
			if effect.Status == OutboxEffectProcessing {
				return ErrDurableJobDispatchNotReady
			}
			return ErrDurableJobDispatchClaim
		}
		dispatchReceipt, err := decodeDurableWorkflowDispatchReceipt(effect.ProviderReceipt)
		if err != nil || dispatchReceipt.OperationID != request.OperationID || !dispatchReceipt.Accepted || dispatchReceipt.TerminalNoop ||
			dispatchReceipt.RunID != request.RunID || dispatchReceipt.RunAttempt != 1 || int64(dispatchReceipt.RunAttempt) != request.RunAttempt || dispatchReceipt.ControlRef == "" ||
			dispatchReceipt.HeadSHA != request.WorkflowSHA || dispatchReceipt.RepositoryID != request.RepositoryID {
			return ErrDurableJobDispatchClaim
		}
		state, err := loadDurableWorkflowDispatchStateTx(tx, &effect)
		if err != nil {
			return err
		}
		if state.Job.DiggerJobID != request.DiggerJobID || state.Job.ProjectName != request.ProjectName ||
			state.Batch.RepoFullName != request.RepositoryFullName || state.Job.ProtocolVersion != request.ProtocolVersion ||
			state.Job.WriterEpoch == nil || *state.Job.WriterEpoch != request.DispatchWriterEpoch ||
			state.JobOperation.Status != ControlOperationPending || state.Token.ActivatedAt == nil || state.Token.RevokedAt != nil ||
			!state.Token.Expiry.After(now) || !hmac.Equal([]byte(state.Token.Value), []byte(jobTokenValue)) {
			return ErrDurableJobDispatchClaim
		}
		expectedWorkflowRef := fmt.Sprintf("%s/.github/workflows/%s@refs/heads/%s", state.Batch.RepoFullName, strings.TrimPrefix(state.Job.WorkflowFile, ".github/workflows/"), dispatchReceipt.ControlRef)
		if request.WorkflowRef != expectedWorkflowRef {
			return ErrDurableJobDispatchClaim
		}
		if state.Job.Status != scheduler.DiggerJobTriggered && state.Job.Status != scheduler.DiggerJobStarted {
			return ErrDurableJobDispatchClaim
		}

		var existing ExecutionClaimAttempt
		err = tx.First(&existing, "operation_id = ? AND run_id = ? AND run_attempt = ?", request.OperationID, request.RunID, request.RunAttempt).Error
		if err == nil {
			claimSHA256, err := durableExecutionClaimSHA256(request, state.Job.ID, state.Token.ID, existing.ExpectedWriterEpoch, existing.SigningKeyID, existing.GrantExpiresAt)
			if err != nil {
				return err
			}
			if !sameExecutionClaimIdentity(&existing, request, state.Job.ID, state.Token.ID, claimSHA256) {
				return ErrDurableJobDispatchConflict
			}
			replayGrantSecret := grantSecrets[existing.SigningKeyID]
			if len(replayGrantSecret) < 32 {
				return ErrDurableJobDispatchConflict
			}
			replayGrant := durableExecutionGrant(jobTokenValue, replayGrantSecret, existing.ClaimSHA256)
			replayGrantDigest := sha256.Sum256([]byte(replayGrant))
			if existing.State == ExecutionClaimGranted && existing.GrantTokenSHA256 == hex.EncodeToString(replayGrantDigest[:]) {
				receipt = &DurableExecutionClaimReceipt{Granted: true, AlreadyGranted: true, ExecutionGrant: replayGrant, SigningKeyID: existing.SigningKeyID, GrantExpiresAt: existing.GrantExpiresAt}
				return nil
			}
			if existing.State == ExecutionClaimGranted {
				return ErrDurableJobDispatchConflict
			}
			receipt = &DurableExecutionClaimReceipt{Granted: false}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if !dispatchReceipt.ClaimExpiresAt.After(now) {
			return ErrDurableJobDispatchClaim
		}
		claimSHA256, err := durableExecutionClaimSHA256(request, state.Job.ID, state.Token.ID, writerEpoch, activeGrantSigningKeyID, state.Token.Expiry)
		if err != nil {
			return err
		}
		grant := durableExecutionGrant(jobTokenValue, activeGrantSecret, claimSHA256)
		grantDigest := sha256.Sum256([]byte(grant))
		grantSHA256 := hex.EncodeToString(grantDigest[:])

		var grantedCount int64
		if err := tx.Model(&ExecutionClaimAttempt{}).Where("operation_id = ? AND state = ?", request.OperationID, ExecutionClaimGranted).Count(&grantedCount).Error; err != nil {
			return err
		}
		claim := executionClaimAttempt(request, state.Job.ID, state.Token.ID, writerEpoch, activeGrantSigningKeyID, state.Token.Expiry, claimSHA256, now)
		if grantedCount > 0 {
			claim.State = ExecutionClaimRejected
			claim.RejectedAt = &now
			if err := tx.Create(&claim).Error; err != nil {
				return err
			}
			receipt = &DurableExecutionClaimReceipt{Granted: false}
			return nil
		}

		claim.State = ExecutionClaimGranted
		claim.GrantTokenSHA256 = grantSHA256
		claim.GrantedAt = &now
		if err := tx.Create(&claim).Error; err != nil {
			return err
		}
		result := tx.Model(&DiggerJob{}).
			Where("id = ? AND status = ? AND status_version <= ?", state.Job.ID, scheduler.DiggerJobTriggered, 2).
			Updates(map[string]any{"status": scheduler.DiggerJobStarted, "status_version": int64(2), "status_updated_at": now, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobDispatchConflict
		}
		receipt = &DurableExecutionClaimReceipt{Granted: true, ExecutionGrant: grant, SigningKeyID: activeGrantSigningKeyID, GrantExpiresAt: state.Token.Expiry}
		return nil
	})
	return receipt, err
}

type durableWorkflowDispatchProviderReceipt struct {
	ClaimExpiresAt time.Time `json:"claim_expires_at"`
	RepositoryID   int64     `json:"repository_id"`
	WorkflowID     int64     `json:"workflow_id"`
	Accepted       bool      `json:"accepted"`
	OperationID    string    `json:"operation_id"`
	TerminalNoop   bool      `json:"terminal_noop"`
	ControlRef     string    `json:"control_ref"`
	RunID          int64     `json:"run_id"`
	RunAttempt     int       `json:"run_attempt"`
	RunURL         string    `json:"run_url"`
	HeadSHA        string    `json:"head_sha"`
}

func decodeDurableWorkflowDispatchReceipt(payload []byte) (*durableWorkflowDispatchProviderReceipt, error) {
	if len(payload) == 0 {
		return nil, ErrDurableJobDispatchClaim
	}
	var receipt durableWorkflowDispatchProviderReceipt
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil, fmt.Errorf("decode durable workflow dispatch receipt: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode durable workflow dispatch receipt: trailing JSON")
	}
	if !operation.ID(receipt.OperationID).Valid() || receipt.ClaimExpiresAt.IsZero() || receipt.RunID <= 0 || receipt.RunAttempt != 1 || receipt.RepositoryID <= 0 || receipt.WorkflowID <= 0 ||
		strings.TrimSpace(receipt.ControlRef) == "" || strings.TrimSpace(receipt.ControlRef) != receipt.ControlRef ||
		!validLowerHexDigest(receipt.HeadSHA, 40) || strings.TrimSpace(receipt.RunURL) == "" {
		return nil, ErrDurableJobDispatchClaim
	}
	return &receipt, nil
}

func validDurableExecutionClaimRequest(request DurableExecutionClaimRequest) bool {
	if !operation.ID(request.OperationID).Valid() || request.DiggerJobID == "" || request.RepositoryFullName == "" || request.ProjectName == "" ||
		request.RunID <= 0 || request.RunAttempt <= 0 || request.ProtocolVersion <= 0 || request.DispatchWriterEpoch <= 0 ||
		request.RepositoryID <= 0 || strings.TrimSpace(request.OIDCIssuer) == "" || strings.TrimSpace(request.OIDCAudience) == "" || strings.TrimSpace(request.OIDCSubject) == "" ||
		strings.TrimSpace(request.WorkflowRef) == "" || strings.TrimSpace(request.ActionRef) == "" {
		return false
	}
	audience, err := operation.ExecutionClaimAudience(request.OperationID, request.DiggerJobID)
	if err != nil || request.OIDCAudience != audience || request.OIDCIssuer != "https://token.actions.githubusercontent.com" || len(request.OIDCAudience) > 1024 || len(request.OIDCSubject) > 1024 || request.ProtocolVersion != operation.OIDCProtocolVersion {
		return false
	}
	if len(request.WorkflowRef) > 1024 || len(request.ActionRef) > 1024 {
		return false
	}
	return validLowerHexDigest(request.CLISHA256, sha256.Size*2) &&
		(validLowerHexDigest(request.WorkflowSHA, 40) || validLowerHexDigest(request.WorkflowSHA, sha256.Size*2))
}

func validGrantSigningKeyID(value string) bool {
	return value != "" && len(value) <= 128 && strings.TrimSpace(value) == value
}

func validLowerHexDigest(value string, length int) bool {
	if len(value) != length || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func executionClaimAttempt(request DurableExecutionClaimRequest, jobDatabaseID uint, jobTokenID uint, writerEpoch int64, signingKeyID string, grantExpiresAt time.Time, claimSHA256 string, now time.Time) ExecutionClaimAttempt {
	return ExecutionClaimAttempt{
		ID:                  uuid.New(),
		ControlOperationID:  request.OperationID,
		DiggerJobID:         request.DiggerJobID,
		DiggerJobDatabaseID: jobDatabaseID,
		JobTokenID:          jobTokenID,
		RunID:               request.RunID,
		RunAttempt:          request.RunAttempt,
		RepositoryID:        request.RepositoryID,
		OIDCIssuer:          request.OIDCIssuer,
		OIDCAudience:        request.OIDCAudience,
		OIDCSubject:         request.OIDCSubject,
		WorkflowRef:         request.WorkflowRef,
		WorkflowSHA:         request.WorkflowSHA,
		ActionRef:           request.ActionRef,
		CLISHA256:           request.CLISHA256,
		ProtocolVersion:     request.ProtocolVersion,
		ExpectedWriterEpoch: writerEpoch,
		DispatchWriterEpoch: request.DispatchWriterEpoch,
		State:               ExecutionClaimPending,
		ClaimSHA256:         claimSHA256,
		SigningKeyID:        signingKeyID,
		GrantExpiresAt:      grantExpiresAt,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func sameExecutionClaimIdentity(existing *ExecutionClaimAttempt, request DurableExecutionClaimRequest, jobDatabaseID uint, jobTokenID uint, claimSHA256 string) bool {
	return existing != nil && existing.ControlOperationID == request.OperationID && existing.RunID == request.RunID &&
		existing.RunAttempt == request.RunAttempt && existing.WorkflowRef == request.WorkflowRef && existing.WorkflowSHA == request.WorkflowSHA &&
		existing.RepositoryID == request.RepositoryID && existing.OIDCIssuer == request.OIDCIssuer && existing.OIDCAudience == request.OIDCAudience && existing.OIDCSubject == request.OIDCSubject &&
		existing.ActionRef == request.ActionRef && existing.CLISHA256 == request.CLISHA256 &&
		existing.ProtocolVersion == request.ProtocolVersion && existing.DispatchWriterEpoch == request.DispatchWriterEpoch &&
		existing.DiggerJobID == request.DiggerJobID && existing.DiggerJobDatabaseID == jobDatabaseID && existing.JobTokenID == jobTokenID &&
		hmac.Equal([]byte(existing.ClaimSHA256), []byte(claimSHA256))
}

type durableExecutionClaimIdentity struct {
	OperationID         string `json:"operation_id"`
	DiggerJobID         string `json:"digger_job_id"`
	DiggerJobDatabaseID uint   `json:"digger_job_database_id"`
	JobTokenID          uint   `json:"job_token_id"`
	RepositoryFullName  string `json:"repository_full_name"`
	ProjectName         string `json:"project_name"`
	RunID               int64  `json:"run_id"`
	RunAttempt          int64  `json:"run_attempt"`
	RepositoryID        int64  `json:"repository_id"`
	OIDCIssuer          string `json:"oidc_issuer"`
	OIDCAudience        string `json:"oidc_audience"`
	OIDCSubject         string `json:"oidc_subject"`
	WorkflowRef         string `json:"workflow_ref"`
	WorkflowSHA         string `json:"workflow_sha"`
	ActionRef           string `json:"action_ref"`
	CLISHA256           string `json:"cli_sha256"`
	ProtocolVersion     int    `json:"protocol_version"`
	DispatchWriterEpoch int64  `json:"dispatch_writer_epoch"`
	GrantingWriterEpoch int64  `json:"granting_writer_epoch"`
	SigningKeyID        string `json:"signing_key_id"`
	GrantExpiresAt      string `json:"grant_expires_at"`
}

func durableExecutionClaimSHA256(request DurableExecutionClaimRequest, jobDatabaseID uint, jobTokenID uint, grantingWriterEpoch int64, signingKeyID string, grantExpiresAt time.Time) (string, error) {
	identity := durableExecutionClaimIdentity{
		OperationID:         request.OperationID,
		DiggerJobID:         request.DiggerJobID,
		DiggerJobDatabaseID: jobDatabaseID,
		JobTokenID:          jobTokenID,
		RepositoryFullName:  request.RepositoryFullName,
		ProjectName:         request.ProjectName,
		RunID:               request.RunID,
		RunAttempt:          request.RunAttempt,
		RepositoryID:        request.RepositoryID,
		OIDCIssuer:          request.OIDCIssuer,
		OIDCAudience:        request.OIDCAudience,
		OIDCSubject:         request.OIDCSubject,
		WorkflowRef:         request.WorkflowRef,
		WorkflowSHA:         request.WorkflowSHA,
		ActionRef:           request.ActionRef,
		CLISHA256:           request.CLISHA256,
		ProtocolVersion:     request.ProtocolVersion,
		DispatchWriterEpoch: request.DispatchWriterEpoch,
		GrantingWriterEpoch: grantingWriterEpoch,
		SigningKeyID:        signingKeyID,
		GrantExpiresAt:      grantExpiresAt.UTC().Format(time.RFC3339Nano),
	}
	serializedIdentity, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("marshal durable execution claim identity: %w", err)
	}
	digest := sha256.Sum256(serializedIdentity)
	return hex.EncodeToString(digest[:]), nil
}

func durableExecutionGrant(jobTokenValue string, grantSecret []byte, claimSHA256 string) string {
	mac := hmac.New(sha256.New, grantSecret)
	mac.Write([]byte("digger-execution-grant\x00"))
	mac.Write([]byte(jobTokenValue))
	mac.Write([]byte("\x00"))
	mac.Write([]byte(claimSHA256))
	return hex.EncodeToString(mac.Sum(nil))
}
