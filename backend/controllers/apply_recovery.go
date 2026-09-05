package controllers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const maxApplyRecoveryResolutionBodyBytes int64 = 16 * 1024

type applyRecoveryResponse struct {
	OperationID               string          `json:"operation_id"`
	ExecutionClaimID          uuid.UUID       `json:"execution_claim_id"`
	WriterEpoch               int64           `json:"writer_epoch"`
	Revision                  int64           `json:"revision"`
	Outcome                   string          `json:"outcome"`
	Observation               json.RawMessage `json:"observation"`
	ObservationSHA256         string          `json:"observation_sha256"`
	TerminalObservation       json.RawMessage `json:"terminal_observation,omitempty"`
	TerminalObservationSHA256 string          `json:"terminal_observation_sha256,omitempty"`
	CreatedAt                 time.Time       `json:"created_at"`
	ResolutionID              *uuid.UUID      `json:"resolution_id,omitempty"`
	Resolution                json.RawMessage `json:"resolution,omitempty"`
	ResolvedAt                *time.Time      `json:"resolved_at,omitempty"`
}

func (d DiggerController) GetApplyRecovery(c *gin.Context) {
	if !d.applyRecoveryEnabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "apply recovery was not found"})
		return
	}
	organisationID, _, ok := applyRecoveryOperator(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "an authenticated administrator is required"})
		return
	}
	operationID := c.Param("operationID")
	if !operation.ID(operationID).Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid apply recovery identity"})
		return
	}
	recovery, err := models.DB.GetApplyRecovery(c.Request.Context(), operationID, organisationID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "apply recovery was not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "apply recovery could not be read"})
		return
	}
	response, err := publicApplyRecovery(recovery)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "apply recovery is invalid"})
		return
	}
	c.JSON(http.StatusOK, response)
}

func (d DiggerController) ResolveApplyRecovery(c *gin.Context) {
	if !d.applyRecoveryEnabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "apply recovery was not found"})
		return
	}
	organisationID, actor, ok := applyRecoveryOperator(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "an authenticated administrator is required"})
		return
	}
	operationID := c.Param("operationID")
	if !operation.ID(operationID).Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid apply recovery identity"})
		return
	}
	resolutionID, err := uuid.Parse(c.Param("resolutionID"))
	if err != nil || resolutionID == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid apply recovery resolution identity"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxApplyRecoveryResolutionBodyBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var request models.ResolveApplyRecoveryRequest
	if err := decoder.Decode(&request); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "apply recovery resolution is too large"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid apply recovery resolution"})
		}
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid apply recovery resolution"})
		return
	}
	if request.ResolutionID != resolutionID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "apply recovery resolution identity does not match its route"})
		return
	}
	recovery, err := models.DB.ResolveApplyRecovery(c.Request.Context(), operationID, organisationID, actor, request, d.ControlPlaneDatabaseIdentity, d.ControlPlaneWriterEpoch)
	if err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "apply recovery was not found"})
		case errors.Is(err, models.ErrControlPlaneFenced), errors.Is(err, models.ErrControlPlaneHold), errors.Is(err, models.ErrControlPlaneDrain), errors.Is(err, models.ErrControlPlaneUnconfigured):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "apply recovery is paused"})
		case errors.Is(err, models.ErrControlPlaneProtocol), errors.Is(err, models.ErrApplyRecoveryConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "apply recovery resolution conflicts with persisted state"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "apply recovery could not be resolved"})
		}
		return
	}
	response, err := publicApplyRecovery(recovery)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "apply recovery is invalid"})
		return
	}
	if d.OutboxDispatcher != nil {
		d.OutboxDispatcher.Wake()
	}
	c.JSON(http.StatusOK, response)
}

func (d DiggerController) applyRecoveryEnabled() bool {
	return d.GithubWebhookProcessor != nil && d.GithubWebhookProcessor.Enabled() && strings.TrimSpace(d.ControlPlaneDatabaseIdentity) != "" && d.ControlPlaneWriterEpoch > 0
}

func applyRecoveryOperator(c *gin.Context) (uint, string, bool) {
	if c.GetString(middleware.ACCESS_LEVEL_KEY) != models.AdminPolicyType {
		return 0, "", false
	}
	organisationID := c.GetUint(middleware.ORGANISATION_ID_KEY)
	if organisationID == 0 {
		return 0, "", false
	}
	actor := c.GetString(middleware.AUTHENTICATED_ACTOR_KEY)
	if actor == "" || actor != strings.TrimSpace(actor) || len(actor) > 1024 {
		return 0, "", false
	}
	return organisationID, actor, true
}

func publicApplyRecovery(recovery *models.ApplyRecovery) (*applyRecoveryResponse, error) {
	if recovery == nil || !json.Valid(recovery.Observation) || len(recovery.TerminalObservation) != 0 && !json.Valid(recovery.TerminalObservation) || len(recovery.Resolution) != 0 && !json.Valid(recovery.Resolution) {
		return nil, errors.New("apply recovery contains invalid JSON")
	}
	return &applyRecoveryResponse{
		OperationID:               recovery.OperationID,
		ExecutionClaimID:          recovery.ExecutionClaimID,
		WriterEpoch:               recovery.WriterEpoch,
		Revision:                  recovery.Revision,
		Outcome:                   recovery.Outcome,
		Observation:               append(json.RawMessage(nil), recovery.Observation...),
		ObservationSHA256:         recovery.ObservationSHA256,
		TerminalObservation:       append(json.RawMessage(nil), recovery.TerminalObservation...),
		TerminalObservationSHA256: recovery.TerminalObservationSHA256,
		CreatedAt:                 recovery.CreatedAt,
		ResolutionID:              recovery.ResolutionID,
		Resolution:                append(json.RawMessage(nil), recovery.Resolution...),
		ResolvedAt:                recovery.ResolvedAt,
	}, nil
}
