package backendapi

import (
	"context"
	"github.com/diggerhq/digger/libs/iac_utils"
	"github.com/diggerhq/digger/libs/scheduler"
	"time"
)

type ContextExecutionClaimer interface {
	ClaimProjectJobExecutionContext(context.Context, string, string, string, ExecutionClaimRequest) (*ExecutionClaimResponse, error)
}

type Api interface {
	ReportProject(repo string, projectName string, configuration string) error
	ClaimProjectJobExecution(repo string, projectName string, jobID string, request ExecutionClaimRequest) (*ExecutionClaimResponse, error)
	ReportProjectJobStatus(repo string, projectName string, jobId string, status string, timestamp time.Time, summary *iac_utils.IacSummary, planJson string, PrCommentUrl string, PrCommentId string, terraformOutput string, iacUtils iac_utils.IacUtils) (*scheduler.SerializedBatch, error)
	UploadJobArtefact(zipLocation string) (*int, *string, error)
	DownloadJobArtefact(downloadTo string) (*string, error)
}

type ExecutionClaimRequest struct {
	RepositoryFullName  string `json:"repository_full_name"`
	ProjectName         string `json:"project_name"`
	OperationID         string `json:"operation_id"`
	RunID               int64  `json:"run_id"`
	RunAttempt          int64  `json:"run_attempt"`
	WorkflowRef         string `json:"workflow_ref"`
	WorkflowSHA         string `json:"workflow_sha"`
	ActionRef           string `json:"action_ref"`
	CLISHA256           string `json:"cli_sha256"`
	ProtocolVersion     int    `json:"protocol_version"`
	DispatchWriterEpoch int64  `json:"dispatch_writer_epoch"`
	OIDCToken           string `json:"oidc_token,omitempty"`
}

type ExecutionClaimResponse struct {
	Granted        bool      `json:"granted"`
	AlreadyGranted bool      `json:"already_granted"`
	ExecutionGrant string    `json:"execution_grant"`
	SigningKeyID   string    `json:"signing_key_id"`
	GrantExpiresAt time.Time `json:"grant_expires_at"`
}
