package controllers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/diggerhq/digger/backend/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const maxDurableStatusCallbackBodyBytes int64 = 16 * 1024 * 1024

func (d DiggerController) DurableJobStatusCallback(c *gin.Context) {
	if strings.TrimSpace(d.ControlPlaneDatabaseIdentity) == "" || d.ControlPlaneWriterEpoch <= 0 {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "durable status callbacks are not configured"})
		return
	}

	jobTokenValue, ok := exactBearerToken(c.GetHeader("Authorization"))
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "an exact job token is required"})
		return
	}
	executionGrant := c.GetHeader("X-Digger-Execution-Grant")
	if executionGrant == "" || executionGrant != strings.TrimSpace(executionGrant) {
		c.JSON(http.StatusForbidden, gin.H{"error": "an exact execution grant is required"})
		return
	}

	callbackID, err := uuid.Parse(c.Param("callbackId"))
	if err != nil || callbackID == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid callback identity"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxDurableStatusCallbackBodyBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var request models.DurableJobStatusCallbackRequest
	if err := decoder.Decode(&request); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "status callback is too large"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status callback"})
		}
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status callback"})
		return
	}
	if request.CallbackID != callbackID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "callback identity does not match its route"})
		return
	}

	receipt, err := models.DB.ApplyDurableJobStatusCallback(
		c.Request.Context(),
		request,
		c.Param("jobId"),
		jobTokenValue,
		executionGrant,
		d.ControlPlaneDatabaseIdentity,
		d.ControlPlaneWriterEpoch,
	)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrControlPlaneFenced), errors.Is(err, models.ErrControlPlaneUnconfigured):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "durable status callbacks are paused"})
		case errors.Is(err, models.ErrControlPlaneProtocol):
			c.JSON(http.StatusConflict, gin.H{"error": "status callback is fenced"})
		case errors.Is(err, models.ErrDurableJobStatusCallback):
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status callback"})
		case errors.Is(err, models.ErrDurableJobStatusCallbackConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "status callback rejected"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "status callback failed"})
		}
		return
	}
	if receipt == nil || receipt.ResponseStatus != http.StatusOK || len(receipt.ResponseBody) == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "status callback failed"})
		return
	}
	if d.OutboxDispatcher != nil {
		d.OutboxDispatcher.Wake()
	}
	c.Data(receipt.ResponseStatus, "application/json", receipt.ResponseBody)
}

func exactBearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimPrefix(header, prefix)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	return token, true
}
