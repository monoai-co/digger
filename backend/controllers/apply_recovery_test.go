package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const applyRecoveryTestGrantKeyID = "apply-recovery-test-key"

func TestApplyRecoveryRejectsUnverifiedJWTActor(t *testing.T) {
	t.Setenv("JWT_AUTH", "true")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPut, "/", nil)
	c.Request.Header.Set("Authorization", "Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJhdHRhY2tlciJ9.")
	c.Set(middleware.ACCESS_LEVEL_KEY, models.AdminPolicyType)
	c.Set(middleware.ORGANISATION_ID_KEY, uint(1))
	c.Set(middleware.USER_ID_KEY, "unverified-header-user")
	_, _, authenticated := applyRecoveryOperator(c)
	require.False(t, authenticated)
}

var applyRecoveryTestGrantSecret = []byte("apply-recovery-test-grant-secret-32")

func TestPostgresApplyRecoveryAdminRoutesAreStrictScopedAndReplayable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database, organisation, recovery, job := prepareApplyRecoveryControllerFixture(t)
	require.NoError(t, database.GormDB.Model(organisation).Update("external_id", models.DEFAULT_ORG_NAME).Error)

	config := DefaultGithubWebhookProcessorConfig()
	config.Enabled = true
	controller := DiggerController{
		GithubWebhookProcessor:       NewGithubWebhookProcessor(nil, nil, config),
		ControlPlaneDatabaseIdentity: durableExecutionIntegrationDatabaseIdentity,
		ControlPlaneWriterEpoch:      durableExecutionIntegrationWriterEpoch,
	}
	t.Setenv("JWT_AUTH", "false")
	t.Setenv("HTTP_BASIC_AUTH", "true")
	t.Setenv("NOOP_AUTH", "false")
	t.Setenv("BEARER_AUTH_TOKEN", "apply-recovery-admin")
	liveJobCredential := models.JobToken{
		Value:          "cli:apply-recovery-route-test",
		Expiry:         time.Now().UTC().Add(time.Hour),
		OrganisationID: organisation.ID,
		Type:           models.CliJobAccessType,
	}
	require.NoError(t, database.GormDB.Create(&liveJobCredential).Error)

	router := applyRecoveryTestRouter(controller)
	operationPath := "/admin/apply-recoveries/" + recovery.OperationID
	resolutionID := uuid.New()
	resolutionPath := operationPath + "/resolutions/" + resolutionID.String()
	request := applyRecoveryControllerResolution(resolutionID)

	response := performApplyRecoveryRequest(t, router, http.MethodGet, operationPath, "", nil)
	require.Equal(t, http.StatusForbidden, response.Code)
	response = performApplyRecoveryRequest(t, router, http.MethodGet, operationPath, liveJobCredential.Value, nil)
	require.Equal(t, http.StatusForbidden, response.Code, "a live CLI job credential must not reach operator recovery")

	disabledProcessor := controller
	disabledProcessor.GithubWebhookProcessor = nil
	disabledIdentity := controller
	disabledIdentity.ControlPlaneDatabaseIdentity = ""
	disabledEpoch := controller
	disabledEpoch.ControlPlaneWriterEpoch = 0
	for _, candidate := range []DiggerController{disabledProcessor, disabledIdentity, disabledEpoch} {
		response = performApplyRecoveryRequest(t, applyRecoveryTestRouter(candidate), http.MethodGet, operationPath, "apply-recovery-admin", nil)
		require.Equal(t, http.StatusNotFound, response.Code, "dormant recovery routes must not expose a legacy API surface")
	}

	response = performApplyRecoveryRequest(t, router, http.MethodGet, operationPath, "apply-recovery-admin", nil)
	require.Equal(t, http.StatusOK, response.Code)
	require.NotContains(t, response.Body.String(), "cli:")
	require.NotContains(t, response.Body.String(), string(applyRecoveryTestGrantSecret))
	var initial applyRecoveryResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &initial))
	require.Equal(t, recovery.OperationID, initial.OperationID)
	require.Equal(t, recovery.ExecutionClaimID, initial.ExecutionClaimID)
	require.Equal(t, durableExecutionIntegrationWriterEpoch, initial.WriterEpoch)
	require.Equal(t, int64(1), initial.Revision)
	require.Equal(t, "unknown", initial.Outcome)
	require.Equal(t, recovery.ObservationSHA256, initial.ObservationSHA256)
	require.Equal(t, recovery.TerminalObservationSHA256, initial.TerminalObservationSHA256)
	require.JSONEq(t, string(recovery.TerminalObservation), string(initial.TerminalObservation))

	wrongID := request
	wrongID.ResolutionID = uuid.New()
	response = performApplyRecoveryRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", wrongID)
	require.Equal(t, http.StatusBadRequest, response.Code)
	response = performApplyRecoveryRawRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", []byte(`{"unknown":true}`))
	require.Equal(t, http.StatusBadRequest, response.Code)
	validBody, err := json.Marshal(request)
	require.NoError(t, err)
	response = performApplyRecoveryRawRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", append(validBody, []byte(` {}`)...))
	require.Equal(t, http.StatusBadRequest, response.Code)
	oversized := []byte(`{"resolution_id":"` + resolutionID.String() + `","padding":"` + strings.Repeat("x", int(maxApplyRecoveryResolutionBodyBytes)) + `"}`)
	response = performApplyRecoveryRawRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", oversized)
	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)

	response = performApplyRecoveryRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "cli:")
	require.NotContains(t, response.Body.String(), string(applyRecoveryTestGrantSecret))
	firstBody := append([]byte(nil), response.Body.Bytes()...)
	response = performApplyRecoveryRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, string(firstBody), response.Body.String())

	conflict := request
	conflict.ResultEvidenceSHA256 = strings.Repeat("5", 64)
	response = performApplyRecoveryRequest(t, router, http.MethodPut, resolutionPath, "apply-recovery-admin", conflict)
	require.Equal(t, http.StatusConflict, response.Code)

	stored, err := database.GetApplyRecovery(context.Background(), recovery.OperationID, organisation.ID)
	require.NoError(t, err)
	require.Equal(t, "aborted", stored.Outcome)
	require.Equal(t, int64(2), stored.Revision)
	require.NotNil(t, stored.ResolutionID)
	require.Equal(t, resolutionID, *stored.ResolutionID)
	var audit struct {
		Actor   string                             `json:"actor"`
		Request models.ResolveApplyRecoveryRequest `json:"request"`
	}
	require.NoError(t, json.Unmarshal(stored.Resolution, &audit))
	require.Equal(t, "static-admin", audit.Actor)
	require.Equal(t, request.Reason, audit.Request.Reason)
	require.False(t, audit.Request.ProviderUnavailable)

	var terminalJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&terminalJob, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobFailed, terminalJob.Status)
	require.Equal(t, int64(3), terminalJob.StatusVersion)
	var terminalToken models.JobToken
	require.NoError(t, database.GormDB.First(&terminalToken, "digger_job_database_id = ?", job.ID).Error)
	require.NotNil(t, terminalToken.RevokedAt)

	otherOrganisation := models.Organisation{Name: "other", ExternalSource: "test", ExternalId: "other-recovery-tenant"}
	require.NoError(t, database.GormDB.Create(&otherOrganisation).Error)
	noPrincipalRouter := gin.New()
	noPrincipalRouter.Use(func(c *gin.Context) {
		c.Set(middleware.ACCESS_LEVEL_KEY, models.AdminPolicyType)
		c.Set(middleware.ORGANISATION_ID_KEY, organisation.ID)
	})
	noPrincipalRouter.PUT("/admin/apply-recoveries/:operationID/resolutions/:resolutionID", controller.ResolveApplyRecovery)
	response = performApplyRecoveryRequest(t, noPrincipalRouter, http.MethodPut, resolutionPath, "", request)
	require.Equal(t, http.StatusForbidden, response.Code, "an admin access level without an authenticated principal must fail closed")

	tenantRouter := gin.New()
	tenantRouter.Use(func(c *gin.Context) {
		c.Set(middleware.ACCESS_LEVEL_KEY, models.AdminPolicyType)
		c.Set(middleware.ORGANISATION_ID_KEY, otherOrganisation.ID)
		c.Set(middleware.AUTHENTICATED_ACTOR_KEY, "user:other-operator")
	})
	tenantRouter.GET("/admin/apply-recoveries/:operationID", controller.GetApplyRecovery)
	response = performApplyRecoveryRequest(t, tenantRouter, http.MethodGet, operationPath, "", nil)
	require.Equal(t, http.StatusNotFound, response.Code, "another tenant must not learn that the operation exists")

	t.Setenv("HTTP_BASIC_AUTH", "false")
	t.Setenv("NOOP_AUTH", "true")
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(http.MethodPut, resolutionPath, nil)
	context.Request.Header.Set("Authorization", "Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJhdHRhY2tlciJ9.")
	context.Set(middleware.ACCESS_LEVEL_KEY, models.AdminPolicyType)
	context.Set(middleware.ORGANISATION_ID_KEY, organisation.ID)
	_, _, authenticated := applyRecoveryOperator(context)
	require.False(t, authenticated, "NOOP auth must never manufacture a recovery actor from an unverified bearer")
}

func prepareApplyRecoveryControllerFixture(t *testing.T) (*models.Database, *models.Organisation, *models.ApplyRecovery, *models.DiggerJob) {
	t.Helper()
	database, organisation, delivery := newDurableExecutionIntegrationDatabase(t)
	job, effect := createDurableExecutionIntegrationGraph(t, database, organisation, delivery)
	now := time.Now().UTC()
	leaseID := "apply-recovery-dispatch-lease"
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
		"status":           models.OutboxEffectProcessing,
		"lease_id":         leaseID,
		"lease_expires_at": now.Add(time.Minute),
		"writer_epoch":     durableExecutionIntegrationWriterEpoch,
		"updated_at":       now,
	}).Error)
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), effect.ID, leaseID, time.Hour, time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, preparation)
	receipt, err := json.Marshal(map[string]any{
		"claim_expires_at": preparation.ClaimExpiresAt,
		"repository_id":    durableExecutionIntegrationRepositoryID,
		"workflow_id":      durableExecutionIntegrationWorkflowID,
		"accepted":         true,
		"operation_id":     *job.OperationID,
		"control_ref":      "main",
		"run_id":           int64(1001),
		"run_attempt":      1,
		"run_url":          "https://github.com/monoai-co/sre/actions/runs/1001",
		"head_sha":         durableExecutionIntegrationSecondSHA,
	})
	require.NoError(t, err)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
		"status":           models.OutboxEffectSucceeded,
		"lease_id":         "",
		"lease_expires_at": nil,
		"provider_receipt": receipt,
		"updated_at":       time.Now().UTC(),
	}).Error)

	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	audience, err := operation.ExecutionClaimAudience(*job.OperationID, job.DiggerJobID)
	require.NoError(t, err)
	claimRequest := models.DurableExecutionClaimRequest{
		OperationID:         *job.OperationID,
		DiggerJobID:         job.DiggerJobID,
		RepositoryFullName:  "monoai-co/sre",
		ProjectName:         job.ProjectName,
		RunID:               1001,
		RunAttempt:          1,
		RepositoryID:        durableExecutionIntegrationRepositoryID,
		OIDCIssuer:          githubOIDCIssuer,
		OIDCAudience:        audience,
		OIDCSubject:         "repo:monoai-co/sre:ref:refs/heads/main",
		WorkflowRef:         "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main",
		WorkflowSHA:         durableExecutionIntegrationSecondSHA,
		ActionRef:           "diggerhq/digger@" + strings.Repeat("c", 40),
		CLISHA256:           strings.Repeat("d", 64),
		ProtocolVersion:     operation.ProtocolVersion,
		DispatchWriterEpoch: durableExecutionIntegrationWriterEpoch,
	}
	claimReceipt, err := database.ClaimDurableJobExecution(context.Background(), claimRequest, token.Value, map[string][]byte{applyRecoveryTestGrantKeyID: applyRecoveryTestGrantSecret}, applyRecoveryTestGrantKeyID, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.True(t, claimReceipt.Granted)

	var claim models.ExecutionClaimAttempt
	require.NoError(t, database.GormDB.First(&claim, "operation_id = ? AND state = ?", *job.OperationID, models.ExecutionClaimGranted).Error)
	expiredAt := time.Now().UTC().Add(-time.Minute)
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Where("id = ?", claim.ID).Updates(map[string]any{
		"created_at":       expiredAt.Add(-2 * time.Hour),
		"granted_at":       expiredAt.Add(-time.Hour),
		"grant_expires_at": expiredAt,
	}).Error)
	require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("id = ?", token.ID).Updates(map[string]any{
		"activated_at": expiredAt.Add(-2 * time.Hour),
		"expiry":       expiredAt,
	}).Error)

	observation := models.DurableRunObservation{
		RepositoryID: durableExecutionIntegrationRepositoryID,
		WorkflowID:   durableExecutionIntegrationWorkflowID,
		RunID:        1001,
		RunAttempt:   1,
		HeadSHA:      durableExecutionIntegrationSecondSHA,
		Status:       "completed",
		Conclusion:   "cancelled",
	}
	encodedObservation, err := json.Marshal(observation)
	require.NoError(t, err)
	digest := sha256.Sum256(encodedObservation)
	recovery := &models.ApplyRecovery{
		OperationID:               *job.OperationID,
		OrganisationID:            organisation.ID,
		ExecutionClaimID:          claim.ID,
		WriterEpoch:               durableExecutionIntegrationWriterEpoch,
		Revision:                  1,
		Outcome:                   "unknown",
		Observation:               encodedObservation,
		ObservationSHA256:         hex.EncodeToString(digest[:]),
		TerminalObservation:       encodedObservation,
		TerminalObservationSHA256: hex.EncodeToString(digest[:]),
		CreatedAt:                 time.Now().UTC(),
	}
	require.NoError(t, database.GormDB.Create(recovery).Error)
	return database, organisation, recovery, job
}

func applyRecoveryControllerResolution(resolutionID uuid.UUID) models.ResolveApplyRecoveryRequest {
	return models.ResolveApplyRecoveryRequest{
		ResolutionID:           resolutionID,
		ExpectedRevision:       1,
		Outcome:                "aborted",
		Reason:                 "Executor termination was verified and the state, resources, and result evidence was reviewed",
		ExecutorStopped:        true,
		EvidenceURI:            "s3://recovery-evidence/immutable-package",
		ExecutorEvidenceSHA256: strings.Repeat("1", 64),
		StateEvidenceSHA256:    strings.Repeat("2", 64),
		ResourceEvidenceSHA256: strings.Repeat("3", 64),
		ResultEvidenceSHA256:   strings.Repeat("4", 64),
	}
}

func applyRecoveryTestRouter(controller DiggerController) *gin.Engine {
	router := gin.New()
	router.GET("/admin/apply-recoveries/:operationID", middleware.HttpBasicApiAuth(), middleware.AccessLevel(models.AdminPolicyType), controller.GetApplyRecovery)
	router.PUT("/admin/apply-recoveries/:operationID/resolutions/:resolutionID", middleware.HttpBasicApiAuth(), middleware.AccessLevel(models.AdminPolicyType), controller.ResolveApplyRecovery)
	return router
}

func performApplyRecoveryRequest(t *testing.T, router http.Handler, method, path, bearer string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		require.NoError(t, err)
	}
	return performApplyRecoveryRawRequest(t, router, method, path, bearer, encoded)
}

func performApplyRecoveryRawRequest(t *testing.T, router http.Handler, method, path, bearer string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}
