package controllers

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	backendutils "github.com/diggerhq/digger/backend/utils"
	backendapi "github.com/diggerhq/digger/libs/backendapi"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	durableExecutionIntegrationDatabaseIdentity = "durable-execution-integration"
	durableExecutionIntegrationWriterEpoch      = int64(7)
	durableExecutionIntegrationRepositoryID     = int64(12345)
	durableExecutionIntegrationWorkflowID       = int64(42)
)

var (
	durableExecutionIntegrationFirstSHA  = strings.Repeat("a", 40)
	durableExecutionIntegrationSecondSHA = strings.Repeat("b", 40)
)

type durableExecutionIntegrationCompletionStore struct {
	*models.Database
	completionCalls     atomic.Int32
	firstCompletionLost chan struct{}
	secondCompletionHit chan struct{}
	releaseSecond       chan struct{}
}

func (store *durableExecutionIntegrationCompletionStore) CompleteOutboxEffect(
	ctx context.Context,
	effectID uuid.UUID,
	leaseID string,
	providerReceipt []byte,
	now time.Time,
	databaseIdentity string,
	writerEpoch int64,
) error {
	switch store.completionCalls.Add(1) {
	case 1:
		close(store.firstCompletionLost)
		return errors.New("simulated loss of the first provider receipt commit")
	case 2:
		close(store.secondCompletionHit)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-store.releaseSecond:
		}
	}
	return store.Database.CompleteOutboxEffect(ctx, effectID, leaseID, providerReceipt, now, databaseIdentity, writerEpoch)
}

type durableExecutionIntegrationProvider struct {
	dispatches atomic.Int32
}

func (provider *durableExecutionIntegrationProvider) roundTrip(request *http.Request) (*http.Response, error) {
	response := func(body string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}
	}
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre":
		return response(`{"id":12345,"full_name":"monoai-co/sre","default_branch":"main"}`), nil
	case request.Method == http.MethodGet && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/digger_workflow.yml":
		return response(`{"id":42,"path":".github/workflows/digger_workflow.yml","state":"active"}`), nil
	case request.Method == http.MethodPost && request.URL.Path == "/repos/monoai-co/sre/actions/workflows/42/dispatches":
		var body struct {
			Ref              string         `json:"ref"`
			Inputs           map[string]any `json:"inputs"`
			ReturnRunDetails bool           `json:"return_run_details"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("decode workflow dispatch: %w", err)
		}
		if body.Ref != "main" || !body.ReturnRunDetails {
			return nil, fmt.Errorf("unexpected workflow dispatch target")
		}
		rawSpec, ok := body.Inputs["spec"].(string)
		if !ok {
			return nil, fmt.Errorf("workflow dispatch omitted its specification")
		}
		var workflowSpec struct {
			OperationID     string `json:"operation_id"`
			ProtocolVersion int    `json:"protocol_version"`
			WriterEpoch     int64  `json:"writer_epoch"`
		}
		if err := json.Unmarshal([]byte(rawSpec), &workflowSpec); err != nil {
			return nil, fmt.Errorf("decode workflow specification: %w", err)
		}
		if !operation.ID(workflowSpec.OperationID).Valid() || workflowSpec.ProtocolVersion != operation.ProtocolVersion || workflowSpec.WriterEpoch != durableExecutionIntegrationWriterEpoch {
			return nil, fmt.Errorf("workflow dispatch carried the wrong durable identity")
		}
		runID := int64(900 + provider.dispatches.Add(1))
		return response(fmt.Sprintf(`{"workflow_run_id":%d}`, runID)), nil
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/repos/monoai-co/sre/actions/runs/"):
		runID, err := strconv.ParseInt(strings.TrimPrefix(request.URL.Path, "/repos/monoai-co/sre/actions/runs/"), 10, 64)
		if err != nil || (runID != 901 && runID != 902) {
			return nil, fmt.Errorf("unexpected workflow run %q", request.URL.Path)
		}
		headSHA := durableExecutionIntegrationFirstSHA
		if runID == 902 {
			headSHA = durableExecutionIntegrationSecondSHA
		}
		run := github.WorkflowRun{
			ID:           github.Int64(runID),
			WorkflowID:   github.Int64(durableExecutionIntegrationWorkflowID),
			Repository:   &github.Repository{ID: github.Int64(durableExecutionIntegrationRepositoryID)},
			RunAttempt:   github.Int(1),
			DisplayTitle: github.String("customer-defined display title"),
			Event:        github.String("workflow_dispatch"),
			Status:       github.String("in_progress"),
			HeadBranch:   github.String("main"),
			HeadSHA:      github.String(headSHA),
		}
		body, err := json.Marshal(run)
		if err != nil {
			return nil, err
		}
		return response(string(body)), nil
	default:
		return nil, fmt.Errorf("unexpected GitHub request %s %s", request.Method, request.URL.Path)
	}
}

type durableExecutionIntegrationClaimRecorder struct {
	base http.RoundTripper
	mu   sync.Mutex
	seen []backendapi.ExecutionClaimResponse
}

func (recorder *durableExecutionIntegrationClaimRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := recorder.base.RoundTrip(request)
	if err != nil || response == nil || response.StatusCode != http.StatusOK || !strings.Contains(request.URL.Path, "/execution-claims") {
		return response, err
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	var receipt backendapi.ExecutionClaimResponse
	if err := json.Unmarshal(body, &receipt); err != nil {
		return nil, fmt.Errorf("record execution claim response: %w", err)
	}
	recorder.mu.Lock()
	recorder.seen = append(recorder.seen, receipt)
	recorder.mu.Unlock()
	return response, nil
}

func (recorder *durableExecutionIntegrationClaimRecorder) receipts() []backendapi.ExecutionClaimResponse {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]backendapi.ExecutionClaimResponse(nil), recorder.seen...)
}

type durableExecutionIntegrationTruncateOnce struct {
	base      http.RoundTripper
	truncated atomic.Bool
}

func (transport *durableExecutionIntegrationTruncateOnce) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil || response == nil || response.StatusCode != http.StatusOK || !strings.Contains(request.URL.Path, "/execution-claims") {
		return response, err
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	var receipt backendapi.ExecutionClaimResponse
	if err := json.Unmarshal(body, &receipt); err != nil {
		return nil, fmt.Errorf("inspect execution claim response for truncation: %w", err)
	}
	if receipt.AlreadyGranted || !transport.truncated.CompareAndSwap(false, true) {
		return response, nil
	}
	response.Body = io.NopCloser(strings.NewReader(`{"granted":`))
	response.ContentLength = int64(len(`{"granted":`))
	return response, nil
}

func TestPostgresDurableExecutionRejectsLostDispatchAndReplaysCanonicalClaim(t *testing.T) {
	database, organisation, delivery := newDurableExecutionIntegrationDatabase(t)
	job, effect := createDurableExecutionIntegrationGraph(t, database, organisation, delivery)

	provider := &durableExecutionIntegrationProvider{}
	githubProvider := backendutils.DiggerGithubClientMockProvider{MockedHTTPClient: &http.Client{Transport: githubWorkflowDispatchRoundTripFunc(provider.roundTrip)}}
	dispatch, err := NewGithubWorkflowOutboxDispatch(database, githubProvider, time.Hour)
	require.NoError(t, err)
	store := &durableExecutionIntegrationCompletionStore{
		Database:            database,
		firstCompletionLost: make(chan struct{}),
		secondCompletionHit: make(chan struct{}),
		releaseSecond:       make(chan struct{}),
	}
	dispatcher, err := NewOutboxDispatcher(store, dispatch, OutboxDispatcherConfig{
		Enabled:          true,
		DatabaseIdentity: durableExecutionIntegrationDatabaseIdentity,
		WriterEpoch:      durableExecutionIntegrationWriterEpoch,
		Workers:          1,
		PollInterval:     2 * time.Millisecond,
		LeaseDuration:    120 * time.Millisecond,
		MaxAttempts:      3,
		RetryBase:        2 * time.Millisecond,
		RetryMax:         5 * time.Millisecond,
	})
	require.NoError(t, err)
	dispatcher.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, dispatcher.Shutdown(ctx))
	})

	waitForDurableExecutionIntegrationSignal(t, store.firstCompletionLost, "first completion failure")
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.NotNil(t, token.ActivatedAt)
	require.Nil(t, token.RevokedAt)
	assertDurableExecutionIntegrationEffect(t, database, effect.ID, models.OutboxEffectProcessing, 1, 0)

	verifier, signingKey := githubOIDCTestVerifier(t)
	controller := DiggerController{
		ControlPlaneDatabaseIdentity: durableExecutionIntegrationDatabaseIdentity,
		ControlPlaneWriterEpoch:      durableExecutionIntegrationWriterEpoch,
		ExecutionGrantSigningKeyID:   "integration-key-v1",
		ExecutionGrantSecrets:        map[string][]byte{"integration-key-v1": bytes.Repeat([]byte{7}, 32)},
		ExecutionIdentityVerifier:    verifier,
		TrustedActionRef:             "diggerhq/digger@" + strings.Repeat("c", 40),
		TrustedCLISHA256:             strings.Repeat("d", 64),
	}
	require.NoError(t, database.GormDB.Create(&models.ExecutionGrantKey{
		KeyID:        controller.ExecutionGrantSigningKeyID,
		SecretSHA256: models.ExecutionGrantSecretFingerprint(controller.ExecutionGrantSecrets[controller.ExecutionGrantSigningKeyID]),
		RegisteredAt: time.Now().UTC(),
	}).Error)
	require.NoError(t, controller.ExecutionClaimsReady(context.Background()))
	mismatchedController := controller
	mismatchedController.ExecutionGrantSecrets = map[string][]byte{"integration-key-v1": bytes.Repeat([]byte{8}, 32)}
	require.ErrorIs(t, mismatchedController.ExecutionClaimsReady(context.Background()), models.ErrExecutionGrantKeysNotReady)

	waitForDurableExecutionIntegrationSignal(t, store.secondCompletionHit, "second completion")
	assertDurableExecutionIntegrationEffect(t, database, effect.ID, models.OutboxEffectProcessing, 2, 0)
	server, oidcCalls := newDurableExecutionIntegrationServer(t, controller, signingKey, token.Value, job)

	for _, candidate := range []struct {
		runID int64
		sha   string
	}{{runID: 901, sha: durableExecutionIntegrationFirstSHA}, {runID: 902, sha: durableExecutionIntegrationSecondSHA}} {
		response := postDurableExecutionIntegrationClaim(t, server.Client(), server.URL, token.Value, signingKey, job, controller, candidate.runID, candidate.sha)
		require.Equal(t, http.StatusTooEarly, response.StatusCode)
		_ = response.Body.Close()
	}
	var attemptsBeforeCommit int64
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&attemptsBeforeCommit).Error)
	require.Zero(t, attemptsBeforeCommit)

	close(store.releaseSecond)
	require.Eventually(t, func() bool {
		var current models.OutboxEffect
		return database.GormDB.First(&current, "id = ?", effect.ID).Error == nil && current.Status == models.OutboxEffectSucceeded
	}, 3*time.Second, 5*time.Millisecond)
	claimExpiresAt := assertDurableExecutionIntegrationEffect(t, database, effect.ID, models.OutboxEffectSucceeded, 2, 902)
	require.Equal(t, int32(2), provider.dispatches.Load())

	orphan := postDurableExecutionIntegrationClaim(t, server.Client(), server.URL, token.Value, signingKey, job, controller, 901, durableExecutionIntegrationFirstSHA)
	require.Equal(t, http.StatusConflict, orphan.StatusCode)
	_ = orphan.Body.Close()
	var attemptsAfterOrphan int64
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&attemptsAfterOrphan).Error)
	require.Zero(t, attemptsAfterOrphan)

	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL+"/oidc")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "integration-oidc-request-token")
	baseTransport := server.Client().Transport
	recorder := &durableExecutionIntegrationClaimRecorder{base: baseTransport}
	truncating := &durableExecutionIntegrationTruncateOnce{base: recorder}
	claimRequest := durableExecutionIntegrationBackendClaim(job, controller, 902, durableExecutionIntegrationSecondSHA)
	claimRequest.ClaimExpiresAt = claimExpiresAt

	const contenders = 32
	start := make(chan struct{})
	receipts := make(chan *backendapi.ExecutionClaimResponse, contenders)
	errorsByWorker := make(chan error, contenders)
	var workers sync.WaitGroup
	for worker := 0; worker < contenders; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			client := &http.Client{Transport: truncating}
			api := &backendapi.DiggerApi{DiggerHost: server.URL, AuthToken: token.Value, HttpClient: client}
			receipt, err := api.ClaimProjectJobExecutionContext(context.Background(), "monoai-co/sre", job.ProjectName, job.DiggerJobID, claimRequest)
			if err != nil {
				errorsByWorker <- err
				return
			}
			receipts <- receipt
		}()
	}
	close(start)
	workers.Wait()
	close(receipts)
	close(errorsByWorker)
	for err := range errorsByWorker {
		require.NoError(t, err)
	}

	var canonical []*backendapi.ExecutionClaimResponse
	for receipt := range receipts {
		canonical = append(canonical, receipt)
	}
	require.Len(t, canonical, contenders)
	require.True(t, truncating.truncated.Load())
	for _, receipt := range canonical {
		require.True(t, receipt.Granted)
		require.Equal(t, canonical[0].ExecutionGrant, receipt.ExecutionGrant)
		require.Equal(t, canonical[0].SigningKeyID, receipt.SigningKeyID)
		require.Equal(t, canonical[0].GrantExpiresAt, receipt.GrantExpiresAt)
	}
	serverReceipts := recorder.receipts()
	require.Len(t, serverReceipts, contenders+1)
	initialGrants := 0
	for _, receipt := range serverReceipts {
		require.True(t, receipt.Granted)
		require.Equal(t, canonical[0].ExecutionGrant, receipt.ExecutionGrant)
		if !receipt.AlreadyGranted {
			initialGrants++
		}
	}
	require.Equal(t, 1, initialGrants)
	require.Equal(t, int32(contenders+1), oidcCalls.Load())

	var storedAttempts []models.ExecutionClaimAttempt
	require.NoError(t, database.GormDB.Find(&storedAttempts).Error)
	require.Len(t, storedAttempts, 1)
	expectedAudience, err := operation.ExecutionClaimAudience(*job.OperationID, job.DiggerJobID)
	require.NoError(t, err)
	require.Equal(t, durableExecutionIntegrationRepositoryID, storedAttempts[0].RepositoryID)
	require.Equal(t, githubOIDCIssuer, storedAttempts[0].OIDCIssuer)
	require.Equal(t, expectedAudience, storedAttempts[0].OIDCAudience)
	require.Equal(t, "repo:monoai-co/sre:ref:refs/heads/main", storedAttempts[0].OIDCSubject)
	require.Equal(t, operation.ProtocolVersion, storedAttempts[0].ProtocolVersion)
	require.Equal(t, models.ExecutionClaimGranted, storedAttempts[0].State)
	var storedJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&storedJob, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, storedJob.Status)
	require.Equal(t, int64(2), storedJob.StatusVersion)
}

func newDurableExecutionIntegrationDatabase(t *testing.T) (*models.Database, *models.Organisation, *models.GithubWebhookDelivery) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	adminSQLDB, err := adminDB.DB()
	require.NoError(t, err)
	schemaName := "durable_execution_integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+schemaName).Error)

	parsedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsedDSN.Query()
	query.Set("search_path", schemaName)
	parsedDSN.RawQuery = query.Encode()
	gormDB, err := gorm.Open(postgres.Open(parsedDSN.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(48)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
		require.NoError(t, adminDB.Exec("DROP SCHEMA "+schemaName+" CASCADE").Error)
		require.NoError(t, adminSQLDB.Close())
	})
	require.NoError(t, gormDB.AutoMigrate(
		&models.ExecutionGrantKey{},
		&models.ControlPlaneFence{},
		&models.GithubWebhookOrderingDomain{},
		&models.GithubWebhookDelivery{},
		&models.ControlOperation{},
		&models.Organisation{},
		&models.GithubAppInstallationLink{},
		&models.VCSConnection{},
		&models.DiggerBatch{},
		&models.DiggerJobSummary{},
		&models.DiggerJob{},
		&models.JobToken{},
		&models.GithubDiggerJobLink{},
		&models.DiggerJobParentLink{},
		&models.OutboxEffect{},
		&models.ExecutionClaimAttempt{},
		&models.JobStatusCallback{},
		&models.ApplyRecovery{},
	))
	require.NoError(t, gormDB.Create(&models.ControlPlaneFence{
		ID:               models.ControlPlaneFenceSingletonID,
		DatabaseIdentity: durableExecutionIntegrationDatabaseIdentity,
		WriterEpoch:      durableExecutionIntegrationWriterEpoch,
		Mode:             models.ControlPlaneModeNormal,
		ProtocolFloor:    operation.ProtocolVersion,
		UpdatedAt:        time.Now().UTC(),
	}).Error)
	organisation := &models.Organisation{Name: "integration-organisation", ExternalSource: "test", ExternalId: schemaName}
	require.NoError(t, gormDB.Create(organisation).Error)
	database := &models.Database{GormDB: gormDB}
	previousDatabase := models.DB
	models.DB = database
	t.Cleanup(func() { models.DB = previousDatabase })

	installationID := int64(123)
	require.NoError(t, gormDB.Create(&models.GithubAppInstallationLink{
		GithubInstallationId: installationID,
		OrganisationId:       organisation.ID,
		Status:               models.GithubAppInstallationLinkActive,
	}).Error)
	_, created, err := database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
		DeliveryID:         "durable-execution-integration-delivery",
		PayloadSHA256:      "durable-execution-integration-payload",
		Payload:            []byte(`{"action":"opened"}`),
		EventType:          "pull_request",
		GithubAppID:        456,
		InstallationID:     &installationID,
		RepositoryFullName: "monoai-co/sre",
	}, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	delivery, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "durable-execution-integration-delivery-lease", time.Minute, durableExecutionIntegrationDatabaseIdentity, durableExecutionIntegrationWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	return database, organisation, delivery
}

func createDurableExecutionIntegrationGraph(t *testing.T, database *models.Database, organisation *models.Organisation, delivery *models.GithubWebhookDelivery) (*models.DiggerJob, *models.OutboxEffect) {
	t.Helper()
	project := configuration.Project{Name: "root", WorkflowFile: "digger_workflow.yml"}
	projects, err := configuration.CreateProjectDependencyGraph([]configuration.Project{project})
	require.NoError(t, err)
	pullRequestNumber := 42
	_, jobs, err := backendutils.ConvertJobsToDiggerJobsDurable(context.Background(), backendutils.DurableJobGraphRequest{
		Identity: models.JobCreationIdentity{
			DatabaseIdentity:    durableExecutionIntegrationDatabaseIdentity,
			DeliveryOperationID: delivery.OperationID,
			DeliveryLeaseID:     delivery.LeaseID,
			WriterEpoch:         durableExecutionIntegrationWriterEpoch,
			ProtocolVersion:     operation.ProtocolVersion,
		},
		JobType:              scheduler.DiggerCommandPlan,
		JobReporterType:      "lazy",
		OrganisationID:       organisation.ID,
		Jobs:                 map[string]scheduler.Job{"root": {ProjectName: "root", Commands: []string{"digger plan"}, PullRequestNumber: &pullRequestNumber}},
		Projects:             map[string]configuration.Project{"root": project},
		ProjectsGraph:        projects,
		GithubInstallationID: delivery.InstallationIDValue(),
		Branch:               "feature/integration",
		PullRequestNumber:    pullRequestNumber,
		RepoOwner:            "monoai-co",
		RepoName:             "sre",
		RepoFullName:         "monoai-co/sre",
		CommitSHA:            durableExecutionIntegrationSecondSHA,
		DiggerConfig:         "projects: []",
	})
	require.NoError(t, err)
	job := jobs["root"]
	require.NotNil(t, job)
	require.NotNil(t, job.OperationID)
	var effect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&effect, "operation_id = ? AND effect_kind = ?", *job.OperationID, models.GithubWorkflowDispatchEffectKind).Error)
	return job, &effect
}

func newDurableExecutionIntegrationServer(t *testing.T, controller DiggerController, signingKey *rsa.PrivateKey, jobToken string, job *models.DiggerJob) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	oidcCalls := new(atomic.Int32)
	router := gin.New()
	router.GET("/oidc", func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer integration-oidc-request-token" {
			c.Status(http.StatusForbidden)
			return
		}
		audience := c.Query("audience")
		claims := durableExecutionIntegrationClaims(t, audience, 902, durableExecutionIntegrationSecondSHA)
		claims.ID = fmt.Sprintf("integration-token-%d", oidcCalls.Add(1))
		c.JSON(http.StatusOK, gin.H{"value": signGithubOIDCTestClaims(t, signingKey, claims)})
	})
	router.POST("/v1/jobs/:jobId/execution-claims", middleware.HttpBasicApiAuth(), func(c *gin.Context) {
		if c.Param("jobId") != job.DiggerJobID || c.GetHeader("Authorization") != "Bearer "+jobToken {
			c.Status(http.StatusForbidden)
			return
		}
		controller.ClaimJobExecution(c)
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server, oidcCalls
}

func durableExecutionIntegrationClaims(t *testing.T, audience string, runID int64, workflowSHA string) githubExecutionClaims {
	t.Helper()
	claims := githubOIDCTestClaims(audience)
	claims.RunID = strconv.FormatInt(runID, 10)
	claims.RepositoryID = strconv.FormatInt(durableExecutionIntegrationRepositoryID, 10)
	claims.WorkflowRef = "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main"
	claims.WorkflowSHA = workflowSHA
	return claims
}

func durableExecutionIntegrationBackendClaim(job *models.DiggerJob, controller DiggerController, runID int64, workflowSHA string) backendapi.ExecutionClaimRequest {
	return backendapi.ExecutionClaimRequest{
		RepositoryFullName:  "monoai-co/sre",
		ProjectName:         job.ProjectName,
		OperationID:         *job.OperationID,
		RunID:               runID,
		RunAttempt:          1,
		WorkflowRef:         "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main",
		WorkflowSHA:         workflowSHA,
		ActionRef:           controller.TrustedActionRef,
		CLISHA256:           controller.TrustedCLISHA256,
		ProtocolVersion:     operation.ProtocolVersion,
		DispatchWriterEpoch: durableExecutionIntegrationWriterEpoch,
	}
}

func postDurableExecutionIntegrationClaim(t *testing.T, client *http.Client, host string, jobToken string, signingKey *rsa.PrivateKey, job *models.DiggerJob, controller DiggerController, runID int64, workflowSHA string) *http.Response {
	t.Helper()
	audience, err := operation.ExecutionClaimAudience(*job.OperationID, job.DiggerJobID)
	require.NoError(t, err)
	request := durableExecutionIntegrationBackendClaim(job, controller, runID, workflowSHA)
	request.OIDCToken = signGithubOIDCTestClaims(t, signingKey, durableExecutionIntegrationClaims(t, audience, runID, workflowSHA))
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest, err := http.NewRequest(http.MethodPost, host+"/v1/jobs/"+job.DiggerJobID+"/execution-claims", bytes.NewReader(payload))
	require.NoError(t, err)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+jobToken)
	response, err := client.Do(httpRequest)
	require.NoError(t, err)
	return response
}

func assertDurableExecutionIntegrationEffect(t *testing.T, database *models.Database, effectID uuid.UUID, status models.OutboxEffectStatus, attempts int64, runID int64) time.Time {
	t.Helper()
	var effect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&effect, "id = ?", effectID).Error)
	require.Equal(t, status, effect.Status)
	require.Equal(t, attempts, effect.AttemptCount)
	if runID == 0 {
		require.Empty(t, effect.ProviderReceipt)
		return time.Time{}
	}
	var receipt struct {
		RunID          int64     `json:"run_id"`
		RepositoryID   int64     `json:"repository_id"`
		WorkflowID     int64     `json:"workflow_id"`
		ClaimExpiresAt time.Time `json:"claim_expires_at"`
	}
	require.NoError(t, json.Unmarshal(effect.ProviderReceipt, &receipt))
	require.Equal(t, runID, receipt.RunID)
	require.Equal(t, durableExecutionIntegrationRepositoryID, receipt.RepositoryID)
	require.Equal(t, durableExecutionIntegrationWorkflowID, receipt.WorkflowID)
	require.True(t, receipt.ClaimExpiresAt.After(time.Now()), "persisted claim deadline must remain usable")
	return receipt.ClaimExpiresAt
}

func waitForDurableExecutionIntegrationSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
