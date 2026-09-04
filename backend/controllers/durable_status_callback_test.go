package controllers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExactBearerToken(t *testing.T) {
	for header, expected := range map[string]string{
		"Bearer cli:exact": "cli:exact",
	} {
		token, ok := exactBearerToken(header)
		require.True(t, ok)
		require.Equal(t, expected, token)
	}
	for _, header := range []string{"", "cli:exact", "bearer cli:exact", "Bearer", "Bearer ", "Bearer  cli:exact", "Bearer cli:exact ", "Bearer cli:exact\n"} {
		_, ok := exactBearerToken(header)
		require.False(t, ok, header)
	}
}

func TestDurableJobStatusCallbackRejectsMalformedRequestsBeforeDatabaseAccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controller := DiggerController{ControlPlaneDatabaseIdentity: "database", ControlPlaneWriterEpoch: 1}
	callbackID := uuid.New()
	valid := models.DurableJobStatusCallbackRequest{
		CallbackID:            callbackID,
		RepositoryFullName:    "monoai-co/sre",
		ProjectName:           "root",
		OperationID:           "op1_callback",
		ProtocolVersion:       2,
		DispatchWriterEpoch:   1,
		TargetStatus:          "succeeded",
		ExpectedStatusVersion: 2,
		ClientTimestamp:       time.Now().UTC(),
	}
	validBody, err := json.Marshal(valid)
	require.NoError(t, err)
	unknownFieldBody := append(append([]byte(nil), validBody[:len(validBody)-1]...), []byte(`,"unknown":true}`)...)
	trailingBody := append(append([]byte(nil), validBody...), []byte(` {}`)...)

	testCases := []struct {
		name          string
		authorization string
		grant         string
		pathID        string
		body          []byte
		wantStatus    int
	}{
		{name: "missing bearer", grant: strings.Repeat("a", 64), pathID: callbackID.String(), body: validBody, wantStatus: http.StatusForbidden},
		{name: "missing grant", authorization: "Bearer cli:exact", pathID: callbackID.String(), body: validBody, wantStatus: http.StatusForbidden},
		{name: "invalid path", authorization: "Bearer cli:exact", grant: strings.Repeat("a", 64), pathID: "invalid", body: validBody, wantStatus: http.StatusBadRequest},
		{name: "mismatched path", authorization: "Bearer cli:exact", grant: strings.Repeat("a", 64), pathID: uuid.NewString(), body: validBody, wantStatus: http.StatusBadRequest},
		{name: "unknown field", authorization: "Bearer cli:exact", grant: strings.Repeat("a", 64), pathID: callbackID.String(), body: unknownFieldBody, wantStatus: http.StatusBadRequest},
		{name: "trailing json", authorization: "Bearer cli:exact", grant: strings.Repeat("a", 64), pathID: callbackID.String(), body: trailingBody, wantStatus: http.StatusBadRequest},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Params = gin.Params{{Key: "jobId", Value: "job-1"}, {Key: "callbackId", Value: testCase.pathID}}
			context.Request = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-1/status-callbacks/"+testCase.pathID, bytes.NewReader(testCase.body))
			if testCase.authorization != "" {
				context.Request.Header.Set("Authorization", testCase.authorization)
			}
			if testCase.grant != "" {
				context.Request.Header.Set("X-Digger-Execution-Grant", testCase.grant)
			}
			controller.DurableJobStatusCallback(context)
			require.Equal(t, testCase.wantStatus, recorder.Code)
		})
	}
}

func TestDurableJobStatusCallbackRequiresConfiguration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-1/status-callbacks/"+uuid.NewString(), http.NoBody)
	DiggerController{}.DurableJobStatusCallback(context)
	require.Equal(t, http.StatusNotImplemented, recorder.Code)
}
