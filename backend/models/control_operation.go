package models

import (
	"time"

	"github.com/google/uuid"
)

type ControlOperationStatus string

const (
	ControlOperationPending   ControlOperationStatus = "pending"
	ControlOperationCompleted ControlOperationStatus = "completed"
	ControlOperationFailed    ControlOperationStatus = "failed"
)

type ControlOperation struct {
	OperationID      string                 `gorm:"type:text;primaryKey"`
	OperationKind    string                 `gorm:"type:text;not null"`
	IdentitySHA256   string                 `gorm:"type:text;not null"`
	GithubDeliveryID *string                `gorm:"column:delivery_id;type:text;index:idx_control_operations_delivery_lookup,where:delivery_id IS NOT NULL;uniqueIndex:idx_control_operations_delivery_batch,where:delivery_id IS NOT NULL AND operation_kind = 'digger_batch'"`
	Delivery         *GithubWebhookDelivery `gorm:"foreignKey:GithubDeliveryID;references:DeliveryID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	WriterEpoch      int64                  `gorm:"not null"`
	ProtocolVersion  int                    `gorm:"type:integer;not null;default:1"`
	Status           ControlOperationStatus `gorm:"type:text;not null;default:pending;check:control_operations_status_check,status IN ('pending','completed','failed')"`
	CreatedAt        time.Time              `gorm:"not null"`
	UpdatedAt        time.Time              `gorm:"not null"`
}

func (ControlOperation) TableName() string {
	return "control_operations"
}

type OutboxEffectStatus string

const (
	OutboxEffectPending    OutboxEffectStatus = "pending"
	OutboxEffectProcessing OutboxEffectStatus = "processing"
	OutboxEffectSucceeded  OutboxEffectStatus = "succeeded"
	OutboxEffectRetrying   OutboxEffectStatus = "retrying"
	OutboxEffectDeadLetter OutboxEffectStatus = "dead_letter"
)

type OutboxEffect struct {
	ID                 uuid.UUID          `gorm:"type:uuid;primaryKey"`
	ControlOperationID string             `gorm:"column:operation_id;type:text;not null;uniqueIndex:idx_outbox_effect_identity,priority:1;uniqueIndex:idx_outbox_workflow_dispatch_operation,where:effect_kind = 'github_workflow_dispatch'"`
	Operation          *ControlOperation  `gorm:"foreignKey:ControlOperationID;references:OperationID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	EffectKind         string             `gorm:"type:text;not null;uniqueIndex:idx_outbox_effect_identity,priority:2"`
	EffectKey          string             `gorm:"type:text;not null;uniqueIndex:idx_outbox_effect_identity,priority:3"`
	Payload            []byte             `gorm:"type:jsonb;not null"`
	PayloadSHA256      string             `gorm:"type:text;not null"`
	WriterEpoch        int64              `gorm:"not null"`
	Status             OutboxEffectStatus `gorm:"type:text;not null;default:pending;check:outbox_effects_status_check,status IN ('pending','processing','succeeded','retrying','dead_letter');index:idx_outbox_effect_queue,priority:1"`
	AttemptCount       int64              `gorm:"not null;default:0"`
	LeaseID            string             `gorm:"type:text"`
	LeaseExpiresAt     *time.Time         `gorm:"index:idx_outbox_effect_queue,priority:3"`
	NextAttemptAt      *time.Time         `gorm:"index:idx_outbox_effect_queue,priority:2"`
	ProviderReceipt    []byte             `gorm:"type:jsonb"`
	LastError          string             `gorm:"type:text"`
	CreatedAt          time.Time          `gorm:"not null"`
	UpdatedAt          time.Time          `gorm:"not null"`
}

func (OutboxEffect) TableName() string {
	return "outbox_effects"
}

type ExecutionClaimState string

const (
	ExecutionClaimPending  ExecutionClaimState = "pending"
	ExecutionClaimGranted  ExecutionClaimState = "granted"
	ExecutionClaimRejected ExecutionClaimState = "rejected"
)

type ExecutionClaimAttempt struct {
	ID                  uuid.UUID           `gorm:"type:uuid;primaryKey"`
	ControlOperationID  string              `gorm:"column:operation_id;type:text;not null;uniqueIndex:idx_execution_claimant,priority:1;uniqueIndex:idx_execution_claim_granted_operation,where:state = 'granted'"`
	Operation           *ControlOperation   `gorm:"foreignKey:ControlOperationID;references:OperationID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	DiggerJobID         string              `gorm:"type:text;not null"`
	DiggerJobDatabaseID uint                `gorm:"not null;check:execution_claim_attempts_positive_ids_check,digger_job_database_id > 0 AND job_token_id > 0 AND run_id > 0 AND run_attempt > 0 AND protocol_version > 0"`
	ExactJob            *DiggerJob          `gorm:"foreignKey:DiggerJobDatabaseID,ControlOperationID,DiggerJobID;references:ID,OperationID,DiggerJobID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	JobTokenID          uint                `gorm:"not null"`
	ExactJobToken       *JobToken           `gorm:"foreignKey:DiggerJobDatabaseID,JobTokenID;references:DiggerJobDatabaseID,ID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	RunID               int64               `gorm:"not null;uniqueIndex:idx_execution_claimant,priority:2"`
	RunAttempt          int64               `gorm:"not null;uniqueIndex:idx_execution_claimant,priority:3"`
	WorkflowRef         string              `gorm:"type:text;not null"`
	WorkflowSHA         string              `gorm:"type:text;not null"`
	ActionRef           string              `gorm:"type:text;not null"`
	CLISHA256           string              `gorm:"column:cli_sha256;type:text;not null"`
	ProtocolVersion     int                 `gorm:"type:integer;not null"`
	ExpectedWriterEpoch int64               `gorm:"not null"`
	DispatchWriterEpoch int64               `gorm:"not null;check:execution_claim_attempts_epochs_check,dispatch_writer_epoch > 0 AND expected_writer_epoch > 0"`
	State               ExecutionClaimState `gorm:"type:text;not null;default:pending;check:execution_claim_attempts_state_check,state IN ('pending','granted','rejected')"`
	ClaimSHA256         string              `gorm:"type:text;not null;check:execution_claim_attempts_claim_digest_check,length(claim_sha256) = 64 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_sha256,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'a',''),'b',''),'c',''),'d',''),'e',''),'f','')) = 0"`
	SigningKeyID        string              `gorm:"type:text;not null;check:execution_claim_attempts_signing_key_check,length(signing_key_id) BETWEEN 1 AND 128 AND trim(signing_key_id) = signing_key_id"`
	GrantTokenSHA256    string              `gorm:"type:text;not null;uniqueIndex:idx_execution_claim_grant_digest,where:state = 'granted';check:execution_claim_attempts_grant_digest_check,grant_token_sha256 = '' OR (length(grant_token_sha256) = 64 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(grant_token_sha256,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'a',''),'b',''),'c',''),'d',''),'e',''),'f','')) = 0)"`
	GrantExpiresAt      time.Time           `gorm:"not null;check:execution_claim_attempts_grant_expiry_check,grant_expires_at > created_at AND (granted_at IS NULL OR grant_expires_at > granted_at)"`
	GrantedAt           *time.Time          `gorm:"check:execution_claim_attempts_lifecycle_check,(state = 'granted' AND grant_token_sha256 <> '' AND granted_at IS NOT NULL AND rejected_at IS NULL) OR (state = 'rejected' AND grant_token_sha256 = '' AND granted_at IS NULL AND rejected_at IS NOT NULL) OR (state = 'pending' AND grant_token_sha256 = '' AND granted_at IS NULL AND rejected_at IS NULL)"`
	RejectedAt          *time.Time
	CreatedAt           time.Time `gorm:"not null"`
	UpdatedAt           time.Time `gorm:"not null"`
}

func (ExecutionClaimAttempt) TableName() string {
	return "execution_claim_attempts"
}

type JobStatusCallback struct {
	CallbackID         uuid.UUID         `gorm:"type:uuid;primaryKey"`
	ControlOperationID string            `gorm:"column:operation_id;type:text;not null;index:idx_job_status_callbacks_operation_id"`
	Operation          *ControlOperation `gorm:"foreignKey:ControlOperationID;references:OperationID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
	DiggerJobID        string            `gorm:"type:text;not null;index"`
	PayloadSHA256      string            `gorm:"type:text;not null"`
	StatusVersion      int64             `gorm:"not null"`
	ResponseStatus     int               `gorm:"type:integer;not null"`
	ResponseBody       []byte            `gorm:"type:jsonb;not null"`
	CreatedAt          time.Time         `gorm:"not null"`
}

func (JobStatusCallback) TableName() string {
	return "job_status_callbacks"
}
