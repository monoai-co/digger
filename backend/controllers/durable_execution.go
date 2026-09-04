package controllers

import (
	"errors"
	"net/http"
	"strings"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/gin-gonic/gin"
)

type claimJobExecutionRequest struct {
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
}

func (d DiggerController) ClaimJobExecution(c *gin.Context) {
	activeGrantSecret := d.ExecutionGrantSecrets[d.ExecutionGrantSigningKeyID]
	if strings.TrimSpace(d.ControlPlaneDatabaseIdentity) == "" || d.ControlPlaneWriterEpoch <= 0 || len(activeGrantSecret) < 32 || strings.TrimSpace(d.ExecutionGrantSigningKeyID) == "" {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "durable execution claims are not configured"})
		return
	}
	jobTokenValue := c.GetString(middleware.JOB_TOKEN_KEY)
	if jobTokenValue == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "an exact job token is required"})
		return
	}
	var request claimJobExecutionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid execution claim"})
		return
	}
	if request.ProjectName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "project identity is required"})
		return
	}
	receipt, err := models.DB.ClaimDurableJobExecution(c.Request.Context(), models.DurableExecutionClaimRequest{
		OperationID:         request.OperationID,
		DiggerJobID:         c.Param("jobId"),
		RepositoryFullName:  request.RepositoryFullName,
		ProjectName:         request.ProjectName,
		RunID:               request.RunID,
		RunAttempt:          request.RunAttempt,
		WorkflowRef:         request.WorkflowRef,
		WorkflowSHA:         request.WorkflowSHA,
		ActionRef:           request.ActionRef,
		CLISHA256:           request.CLISHA256,
		ProtocolVersion:     request.ProtocolVersion,
		DispatchWriterEpoch: request.DispatchWriterEpoch,
	}, jobTokenValue, d.ExecutionGrantSecrets, d.ExecutionGrantSigningKeyID, d.ControlPlaneDatabaseIdentity, d.ControlPlaneWriterEpoch)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrControlPlaneHold), errors.Is(err, models.ErrControlPlaneDrain), errors.Is(err, models.ErrControlPlaneFenced):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "durable execution claims are paused"})
		case errors.Is(err, models.ErrDurableJobDispatchNotReady):
			c.JSON(http.StatusTooEarly, gin.H{"error": "workflow dispatch is not committed"})
		case errors.Is(err, models.ErrControlPlaneProtocol):
			c.JSON(http.StatusConflict, gin.H{"error": "execution claim is fenced"})
		case errors.Is(err, models.ErrDurableJobDispatchClaim), errors.Is(err, models.ErrDurableJobDispatchConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "execution claim rejected"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "execution claim failed"})
		}
		return
	}
	if receipt == nil || !receipt.Granted {
		c.JSON(http.StatusConflict, gin.H{"granted": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"granted":          true,
		"already_granted":  receipt.AlreadyGranted,
		"execution_grant":  receipt.ExecutionGrant,
		"signing_key_id":   receipt.SigningKeyID,
		"grant_expires_at": receipt.GrantExpiresAt,
	})
}
