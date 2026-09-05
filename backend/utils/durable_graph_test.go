package utils

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	durableGraphTestDatabaseIdentity = "durable-graph-test"
	durableGraphTestWriterEpoch      = int64(7)
	durableGraphTestGrantSigningKey  = "durable-graph-test-key-v1"
)

func durableGraphTestGrantSecrets(secret []byte) map[string][]byte {
	return map[string][]byte{durableGraphTestGrantSigningKey: secret}
}

func newDurableGraphTestDatabase(t *testing.T) (*models.Database, *models.Organisation, *models.GithubWebhookDelivery) {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "durable-graph.sqlite") + "?_busy_timeout=5000&_foreign_keys=on"
	gormDB, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return configureDurableGraphTestDatabase(t, gormDB)
}

func newPostgresDurableGraphTestDatabase(t *testing.T) (*models.Database, *models.Organisation, *models.GithubWebhookDelivery) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	adminSQLDB, err := adminDB.DB()
	require.NoError(t, err)
	schemaName := "durable_graph_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+schemaName).Error)

	schemaDSN := dsn
	parsedDSN, parseErr := url.Parse(dsn)
	if parseErr == nil && (parsedDSN.Scheme == "postgres" || parsedDSN.Scheme == "postgresql") {
		query := parsedDSN.Query()
		query.Set("search_path", schemaName)
		parsedDSN.RawQuery = query.Encode()
		schemaDSN = parsedDSN.String()
	} else {
		schemaDSN += " search_path=" + schemaName
	}
	gormDB, err := gorm.Open(postgres.Open(schemaDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
		require.NoError(t, adminDB.Exec("DROP SCHEMA "+schemaName+" CASCADE").Error)
		require.NoError(t, adminSQLDB.Close())
	})
	return configureDurableGraphTestDatabase(t, gormDB)
}

func configureDurableGraphTestDatabase(t *testing.T, gormDB *gorm.DB) (*models.Database, *models.Organisation, *models.GithubWebhookDelivery) {
	t.Helper()
	require.NoError(t, gormDB.AutoMigrate(
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
		DatabaseIdentity: durableGraphTestDatabaseIdentity,
		WriterEpoch:      durableGraphTestWriterEpoch,
		Mode:             models.ControlPlaneModeNormal,
		ProtocolFloor:    operation.ProtocolVersion,
		UpdatedAt:        time.Now().UTC(),
	}).Error)
	organisation := &models.Organisation{Name: "test-organisation", ExternalSource: "test", ExternalId: "durable-graph"}
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
		DeliveryID:         "durable-graph-delivery",
		PayloadSHA256:      "durable-graph-payload-sha256",
		Payload:            []byte(`{"action":"opened"}`),
		EventType:          "pull_request",
		GithubAppID:        456,
		InstallationID:     &installationID,
		RepositoryFullName: "monoai-co/sre",
	}, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	claimedDelivery, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "durable-graph-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimedDelivery)
	return database, organisation, claimedDelivery
}

func durableGraphTestRequest(t *testing.T, organisation *models.Organisation, delivery *models.GithubWebhookDelivery) DurableJobGraphRequest {
	t.Helper()
	rootOne := configuration.Project{Name: "root-one", WorkflowFile: "digger_workflow.yml"}
	rootTwo := configuration.Project{Name: "root-two", WorkflowFile: "digger_workflow.yml"}
	child := configuration.Project{Name: "child", WorkflowFile: "digger_workflow.yml", DependencyProjects: []string{"root-one", "root-two"}}
	return durableGraphTestRequestForProjects(t, organisation, delivery, []configuration.Project{rootOne, rootTwo, child})
}

func durableGraphTestRequestForProjects(t *testing.T, organisation *models.Organisation, delivery *models.GithubWebhookDelivery, projects []configuration.Project) DurableJobGraphRequest {
	t.Helper()
	projectsGraph, err := configuration.CreateProjectDependencyGraph(projects)
	require.NoError(t, err)
	pullRequestNumber := 42
	jobs := make(map[string]scheduler.Job, len(projects))
	projectByName := make(map[string]configuration.Project, len(projects))
	for index := range projects {
		project := projects[index]
		job := scheduler.Job{ProjectName: project.Name, Commands: []string{"digger plan"}, PullRequestNumber: &pullRequestNumber}
		switch index % 3 {
		case 0:
			job.RunEnvVars = map[string]string{"TEST_SECRET": project.Name + "-run-secret"}
		case 1:
			job.StateEnvVars = map[string]string{"TEST_SECRET": project.Name + "-state-secret"}
		default:
			job.CommandEnvVars = map[string]string{"TEST_SECRET": project.Name + "-command-secret"}
		}
		jobs[project.Name] = job
		projectByName[project.Name] = project
	}
	return DurableJobGraphRequest{
		Identity: models.JobCreationIdentity{
			DatabaseIdentity:    durableGraphTestDatabaseIdentity,
			DeliveryOperationID: delivery.OperationID,
			DeliveryLeaseID:     delivery.LeaseID,
			WriterEpoch:         durableGraphTestWriterEpoch,
			ProtocolVersion:     operation.ProtocolVersion,
		},
		JobType:                  scheduler.DiggerCommandPlan,
		JobReporterType:          "lazy",
		OrganisationID:           organisation.ID,
		Jobs:                     jobs,
		Projects:                 projectByName,
		ProjectsGraph:            projectsGraph,
		GithubInstallationID:     delivery.InstallationIDValue(),
		Branch:                   "feature/durable-graph",
		PullRequestNumber:        pullRequestNumber,
		RepoOwner:                "monoai-co",
		RepoName:                 "sre",
		RepoFullName:             "monoai-co/sre",
		CommitSHA:                "deadbeef",
		DiggerConfig:             "projects: []",
		CoverAllImpactedProjects: true,
	}
}

func prepareDurableExecutionClaimTest(t *testing.T, database *models.Database, organisation *models.Organisation, delivery *models.GithubWebhookDelivery) (*models.DiggerJob, models.DurableExecutionClaimRequest, string) {
	return prepareDurableExecutionClaimForProjectTest(t, database, organisation, delivery, "root-one", 1001)
}

func prepareDurableExecutionClaimForProjectTest(t *testing.T, database *models.Database, organisation *models.Organisation, delivery *models.GithubWebhookDelivery, projectName string, runID int64) (*models.DiggerJob, models.DurableExecutionClaimRequest, string) {
	return prepareDurableExecutionClaimForRequestTest(t, database, durableGraphTestRequest(t, organisation, delivery), projectName, runID)
}

func prepareDurableExecutionClaimForRequestTest(t *testing.T, database *models.Database, request DurableJobGraphRequest, projectName string, runID int64) (*models.DiggerJob, models.DurableExecutionClaimRequest, string) {
	t.Helper()
	_, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	job := jobs[projectName]
	require.NotNil(t, job)
	require.NotNil(t, job.OperationID)
	var effect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&effect, "operation_id = ? AND effect_kind = ?", *job.OperationID, models.GithubWorkflowDispatchEffectKind).Error)
	now := time.Now().UTC()
	leaseID := "execution-claim-dispatch-lease-" + projectName
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
		"status":           models.OutboxEffectProcessing,
		"lease_id":         leaseID,
		"lease_expires_at": now.Add(time.Minute),
		"writer_epoch":     durableGraphTestWriterEpoch,
		"updated_at":       now,
	}).Error)
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), effect.ID, leaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, preparation)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Updates(map[string]any{
		"status":           models.OutboxEffectSucceeded,
		"lease_id":         "",
		"lease_expires_at": nil,
		"provider_receipt": durableGraphDispatchReceipt(t, *job.OperationID, runID, strings.Repeat("a", 40)),
		"updated_at":       time.Now().UTC(),
	}).Error)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	return job, models.DurableExecutionClaimRequest{
		RepositoryID:        12345,
		OIDCIssuer:          "https://token.actions.githubusercontent.com",
		OIDCAudience:        "opentaco-control-plane:execution:" + *job.OperationID + ":job:" + job.DiggerJobID,
		OIDCSubject:         "repo:monoai-co/sre:ref:refs/heads/main",
		OperationID:         *job.OperationID,
		DiggerJobID:         job.DiggerJobID,
		RepositoryFullName:  "monoai-co/sre",
		ProjectName:         job.ProjectName,
		RunID:               runID,
		RunAttempt:          1,
		WorkflowRef:         "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main",
		WorkflowSHA:         strings.Repeat("a", 40),
		ActionRef:           "diggerhq/digger@v1",
		CLISHA256:           strings.Repeat("b", 64),
		ProtocolVersion:     operation.ProtocolVersion,
		DispatchWriterEpoch: durableGraphTestWriterEpoch,
	}, token.Value
}

func durableGraphDispatchReceipt(t *testing.T, operationID string, runID int64, headSHA string) []byte {
	t.Helper()
	receipt, err := json.Marshal(map[string]any{
		"claim_expires_at": time.Now().UTC().Add(time.Hour),
		"repository_id":    12345,
		"workflow_id":      42,
		"accepted":         true,
		"operation_id":     operationID,
		"control_ref":      "main",
		"run_id":           runID,
		"run_attempt":      1,
		"run_url":          fmt.Sprintf("https://github.com/monoai-co/sre/actions/runs/%d", runID),
		"head_sha":         headSHA,
	})
	require.NoError(t, err)
	return receipt
}

func TestClaimDurableJobExecutionIsExactAndReplayable(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	secret := []byte(strings.Repeat("grant-secret-", 3))

	receipt, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, receipt.Granted)
	require.False(t, receipt.AlreadyGranted)
	require.NotEmpty(t, receipt.ExecutionGrant)
	require.Equal(t, durableGraphTestGrantSigningKey, receipt.SigningKeyID)
	require.False(t, receipt.GrantExpiresAt.IsZero())

	replay, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, replay.Granted)
	require.True(t, replay.AlreadyGranted)
	require.Equal(t, receipt.ExecutionGrant, replay.ExecutionGrant)
	require.Equal(t, receipt.SigningKeyID, replay.SigningKeyID)
	require.Equal(t, receipt.GrantExpiresAt, replay.GrantExpiresAt)

	_, err = database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets([]byte(strings.Repeat("different-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
	_, err = database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets(secret), "durable-graph-test-key-v2", durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)

	changed := request
	changed.WorkflowRef += "-tampered"
	_, err = database.ClaimDurableJobExecution(context.Background(), changed, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)

	competitor := request
	competitor.RunID++
	_, err = database.ClaimDurableJobExecution(context.Background(), competitor, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)

	for _, invalidToken := range []string{"cli:wrong", ""} {
		_, err = database.ClaimDurableJobExecution(context.Background(), request, invalidToken, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
	}
	var storedJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&storedJob, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobStarted, storedJob.Status)
	require.Equal(t, int64(2), storedJob.StatusVersion)
	var attempts []models.ExecutionClaimAttempt
	require.NoError(t, database.GormDB.Order("run_id").Find(&attempts).Error)
	require.Len(t, attempts, 1)
	require.Equal(t, models.ExecutionClaimGranted, attempts[0].State)
	for _, attempt := range attempts {
		require.Equal(t, job.DiggerJobID, attempt.DiggerJobID)
		require.Equal(t, job.ID, attempt.DiggerJobDatabaseID)
		require.Equal(t, durableGraphTestGrantSigningKey, attempt.SigningKeyID)
		require.Len(t, attempt.ClaimSHA256, sha256.Size*2)
		require.Equal(t, receipt.GrantExpiresAt, attempt.GrantExpiresAt)
	}
	require.Len(t, attempts[0].GrantTokenSHA256, sha256.Size*2)
	var storedToken models.JobToken
	require.NoError(t, database.GormDB.First(&storedToken, "digger_job_database_id = ?", job.ID).Error)
	require.Equal(t, storedToken.ID, attempts[0].JobTokenID)
}

func TestPostgresClaimDurableJobExecutionGrantsOnlyOneRun(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	_, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	secret := []byte(strings.Repeat("grant-secret-", 3))
	start := make(chan struct{})
	const contenders = 32
	receipts := make(chan *models.DurableExecutionClaimReceipt, contenders)
	errorsByWorker := make(chan error, contenders)
	var workers sync.WaitGroup
	for offset := int64(0); offset < contenders; offset++ {
		claim := request
		claim.RunID += offset
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := database.ClaimDurableJobExecution(context.Background(), claim, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
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
	rejected := 0
	for err := range errorsByWorker {
		require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
		rejected++
	}
	granted := 0
	for receipt := range receipts {
		if receipt.Granted {
			granted++
		}
	}
	require.Equal(t, 1, granted)
	require.Equal(t, contenders-1, rejected)
	var attempts int64
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&attempts).Error)
	require.Equal(t, int64(1), attempts)
}

func TestPostgresExecutionClaimAttemptRejectsMixedBindings(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	job, request, tokenValue := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	secret := []byte(strings.Repeat("grant-secret-", 3))
	_, err := database.ClaimDurableJobExecution(context.Background(), request, tokenValue, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)

	var original models.ExecutionClaimAttempt
	require.NoError(t, database.GormDB.First(&original, "operation_id = ? AND state = ?", request.OperationID, models.ExecutionClaimGranted).Error)
	var otherJob models.DiggerJob
	require.NoError(t, database.GormDB.Where("id <> ? AND operation_id IS NOT NULL", job.ID).First(&otherJob).Error)
	var otherToken models.JobToken
	require.NoError(t, database.GormDB.First(&otherToken, "digger_job_database_id = ?", otherJob.ID).Error)

	rejectedAt := time.Now().UTC()
	mixedJob := original
	mixedJob.ID = uuid.New()
	mixedJob.DiggerJobID = otherJob.DiggerJobID
	mixedJob.RunID += 100
	mixedJob.State = models.ExecutionClaimRejected
	mixedJob.GrantTokenSHA256 = ""
	mixedJob.GrantedAt = nil
	mixedJob.RejectedAt = &rejectedAt
	mixedJob.CreatedAt = rejectedAt
	mixedJob.UpdatedAt = rejectedAt
	require.Error(t, database.GormDB.Create(&mixedJob).Error)

	mixedToken := original
	mixedToken.ID = uuid.New()
	mixedToken.JobTokenID = otherToken.ID
	mixedToken.RunID += 101
	mixedToken.State = models.ExecutionClaimRejected
	mixedToken.GrantTokenSHA256 = ""
	mixedToken.GrantedAt = nil
	mixedToken.RejectedAt = &rejectedAt
	mixedToken.CreatedAt = rejectedAt
	mixedToken.UpdatedAt = rejectedAt
	require.Error(t, database.GormDB.Create(&mixedToken).Error)
}

func TestClaimDurableJobExecutionRequiresCommittedDispatchAndExactRoute(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	_, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	secret := []byte(strings.Repeat("grant-secret-", 3))
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", request.OperationID).Update("status", models.OutboxEffectProcessing).Error)
	_, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchNotReady)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", request.OperationID).Update("status", models.OutboxEffectSucceeded).Error)

	for name, mutate := range map[string]func(*models.DurableExecutionClaimRequest){
		"job":        func(claim *models.DurableExecutionClaimRequest) { claim.DiggerJobID += "-other" },
		"repository": func(claim *models.DurableExecutionClaimRequest) { claim.RepositoryFullName = "monoai-co/other" },
		"project":    func(claim *models.DurableExecutionClaimRequest) { claim.ProjectName = "other" },
		"epoch":      func(claim *models.DurableExecutionClaimRequest) { claim.DispatchWriterEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			_, err := database.ClaimDurableJobExecution(context.Background(), changed, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
		})
	}
}

func TestClaimDurableJobExecutionRejectsDifferentAttestedRepository(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	request.RepositoryID++
	_, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, database.GormDB.First(job, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobTriggered, job.Status)
	require.Equal(t, int64(1), job.StatusVersion)
}

func TestClaimDeadlineRejectsNewGrantButPreservesCommittedReplay(t *testing.T) {
	for _, grantBeforeExpiry := range []bool{false, true} {
		t.Run(fmt.Sprintf("already-granted-%t", grantBeforeExpiry), func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
			secrets := durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3)))
			var original *models.DurableExecutionClaimReceipt
			var err error
			if grantBeforeExpiry {
				original, err = database.ClaimDurableJobExecution(context.Background(), request, token, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
				require.NoError(t, err)
			}
			var effect models.OutboxEffect
			require.NoError(t, database.GormDB.First(&effect, "operation_id = ? AND effect_kind = ?", request.OperationID, models.GithubWorkflowDispatchEffectKind).Error)
			var receipt map[string]any
			require.NoError(t, json.Unmarshal(effect.ProviderReceipt, &receipt))
			receipt["claim_expires_at"] = time.Now().UTC().Add(-time.Minute)
			encoded, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&effect).Update("provider_receipt", encoded).Error)
			got, err := database.ClaimDurableJobExecution(context.Background(), request, token, secrets, durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			if grantBeforeExpiry {
				require.NoError(t, err)
				require.True(t, got.AlreadyGranted)
				require.Equal(t, original.ExecutionGrant, got.ExecutionGrant)
			} else {
				require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
				var count int64
				require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&count).Error)
				require.Zero(t, count)
				require.NoError(t, database.GormDB.First(job, "id = ?", job.ID).Error)
				require.Equal(t, scheduler.DiggerJobTriggered, job.Status)
			}
		})
	}
}

func TestClaimDurableJobExecutionRejectsUncommittedRunAttemptBeforeMutation(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	job, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	request.RunAttempt = 2
	_, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
	var storedJob models.DiggerJob
	require.NoError(t, database.GormDB.First(&storedJob, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobTriggered, storedJob.Status)
	require.Equal(t, int64(1), storedJob.StatusVersion)
	var attempts int64
	require.NoError(t, database.GormDB.Model(&models.ExecutionClaimAttempt{}).Count(&attempts).Error)
	require.Zero(t, attempts)
	var storedToken models.JobToken
	require.NoError(t, database.GormDB.First(&storedToken, "digger_job_database_id = ?", job.ID).Error)
	require.Nil(t, storedToken.RevokedAt)
	require.NotNil(t, storedToken.ActivatedAt)
}

func TestClaimDurableJobExecutionReplaySurvivesWriterHandoff(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	_, request, token := prepareDurableExecutionClaimTest(t, database, organisation, delivery)
	secret := []byte(strings.Repeat("grant-secret-", 3))
	first, err := database.ClaimDurableJobExecution(context.Background(), request, token, durableGraphTestGrantSecrets(secret), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)

	const targetEpoch = durableGraphTestWriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", targetEpoch).Error)
	rotatedSecrets := map[string][]byte{
		durableGraphTestGrantSigningKey: secret,
		"durable-graph-test-key-v2":     []byte(strings.Repeat("rotated-secret-", 3)),
	}
	replayed, err := database.ClaimDurableJobExecution(context.Background(), request, token, rotatedSecrets, "durable-graph-test-key-v2", durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.True(t, replayed.Granted)
	require.True(t, replayed.AlreadyGranted)
	require.Equal(t, first.ExecutionGrant, replayed.ExecutionGrant)
	require.Equal(t, durableGraphTestGrantSigningKey, replayed.SigningKeyID)
	_, err = database.ClaimDurableJobExecution(context.Background(), request, token, map[string][]byte{
		"durable-graph-test-key-v2": rotatedSecrets["durable-graph-test-key-v2"],
	}, "durable-graph-test-key-v2", durableGraphTestDatabaseIdentity, targetEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
}

func TestPostgresRecoveredDispatchCanClaimThroughNewWriterEpoch(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), durableGraphTestRequest(t, organisation, delivery))
	require.NoError(t, err)
	const targetEpoch = durableGraphTestWriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", targetEpoch).Error)
	claimedEffect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "recovered-dispatch", time.Minute, durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimedEffect)
	require.Equal(t, targetEpoch, claimedEffect.WriterEpoch)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", claimedEffect.ControlOperationID).Error)
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), claimedEffect.ID, claimedEffect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.NotNil(t, preparation.Job.WriterEpoch)
	require.Equal(t, durableGraphTestWriterEpoch, *preparation.Job.WriterEpoch)
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), claimedEffect.ID, claimedEffect.LeaseID, durableGraphDispatchReceipt(t, *job.OperationID, 2001, strings.Repeat("c", 40)), time.Now().UTC(), durableGraphTestDatabaseIdentity, targetEpoch))

	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	request := models.DurableExecutionClaimRequest{
		RepositoryID:        12345,
		OIDCIssuer:          "https://token.actions.githubusercontent.com",
		OIDCAudience:        "opentaco-control-plane:execution:" + *job.OperationID + ":job:" + job.DiggerJobID,
		OIDCSubject:         "repo:monoai-co/sre:ref:refs/heads/main",
		OperationID:         *job.OperationID,
		DiggerJobID:         job.DiggerJobID,
		RepositoryFullName:  "monoai-co/sre",
		ProjectName:         job.ProjectName,
		RunID:               2001,
		RunAttempt:          1,
		WorkflowRef:         "monoai-co/sre/.github/workflows/digger_workflow.yml@refs/heads/main",
		WorkflowSHA:         strings.Repeat("c", 40),
		ActionRef:           "diggerhq/digger@v1",
		CLISHA256:           strings.Repeat("d", 64),
		ProtocolVersion:     operation.ProtocolVersion,
		DispatchWriterEpoch: durableGraphTestWriterEpoch,
	}
	receipt, err := database.ClaimDurableJobExecution(context.Background(), request, token.Value, durableGraphTestGrantSecrets([]byte(strings.Repeat("grant-secret-", 3))), durableGraphTestGrantSigningKey, durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.True(t, receipt.Granted)
}

func TestConvertJobsToDiggerJobsDurableIsAtomicAndIdempotent(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	assertDurableGraphIsAtomicAndIdempotent(t, database, organisation, delivery)
}

func TestPostgresConvertJobsToDiggerJobsDurableIsAtomicAndIdempotent(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	assertDurableGraphIsAtomicAndIdempotent(t, database, organisation, delivery)
}

func TestPostgresConvertJobsToDiggerJobsDurableRejectsLeaseExpiredWhileWaitingForTenantLock(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("delivery_id = ?", delivery.DeliveryID).Update("lease_expires_at", time.Now().Add(150*time.Millisecond)).Error)
	blocker := database.GormDB.Begin()
	require.NoError(t, blocker.Error)
	blockerOpen := true
	t.Cleanup(func() {
		if blockerOpen {
			blocker.Rollback()
		}
	})
	var link models.GithubAppInstallationLink
	require.NoError(t, blocker.Clauses(clause.Locking{Strength: "UPDATE"}).First(&link, "github_installation_id = ?", delivery.InstallationIDValue()).Error)

	done := make(chan error, 1)
	go func() {
		_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
		done <- err
	}()
	time.Sleep(250 * time.Millisecond)
	require.NoError(t, blocker.Commit().Error)
	blockerOpen = false
	require.ErrorIs(t, <-done, ErrDurableJobGraphClaim)
	assertDurableGraphCounts(t, database, 0, 0, 0)
}

func TestPostgresConvertJobsToDiggerJobsDurableSerializesConcurrentRetries(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	start := make(chan struct{})
	batchIDs := make(chan uuid.UUID, 8)
	errorsByWorker := make(chan error, 8)
	var workers sync.WaitGroup
	for workerIndex := 0; workerIndex < 8; workerIndex++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			batchID, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			if err != nil {
				errorsByWorker <- err
				return
			}
			if batchID == nil || len(jobs) != 3 {
				errorsByWorker <- errors.New("concurrent durable graph returned an incomplete receipt")
				return
			}
			batchIDs <- *batchID
		}()
	}
	close(start)
	workers.Wait()
	close(batchIDs)
	close(errorsByWorker)
	for workerErr := range errorsByWorker {
		require.NoError(t, workerErr)
	}
	var expectedBatchID uuid.UUID
	for batchID := range batchIDs {
		if expectedBatchID == uuid.Nil {
			expectedBatchID = batchID
		}
		require.Equal(t, expectedBatchID, batchID)
	}
	require.NotEqual(t, uuid.Nil, expectedBatchID)
	for model, expectedCount := range map[any]int64{
		&models.DiggerBatch{}:         1,
		&models.DiggerJob{}:           3,
		&models.JobToken{}:            3,
		&models.GithubDiggerJobLink{}: 3,
	} {
		var count int64
		require.NoError(t, database.GormDB.Model(model).Count(&count).Error)
		require.Equal(t, expectedCount, count)
	}
}

func TestConvertJobsToDiggerJobsDurableRecoversCommittedGraphAfterEpochHandoff(t *testing.T) {
	for name, failJobBeforeReclaim := range map[string]bool{
		"pending graph": false,
		"failed job":    true,
	} {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			batchID, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			if failJobBeforeReclaim {
				job := jobs["root-one"]
				now := time.Now().UTC()
				require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("status", scheduler.DiggerJobFailed).Error)
				require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *job.OperationID).Update("status", models.ControlOperationFailed).Error)
				require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("digger_job_database_id = ?", job.ID).Updates(map[string]any{"activated_at": now.Add(-time.Minute), "expiry": now, "revoked_at": now}).Error)
			}

			const targetEpoch = durableGraphTestWriterEpoch + 1
			require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).Where("id = ?", models.ControlPlaneFenceSingletonID).Update("writer_epoch", targetEpoch).Error)
			require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("delivery_id = ?", delivery.DeliveryID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error)
			now := time.Now().UTC()
			require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("delivery_id = ?", delivery.DeliveryID).Updates(map[string]any{
				"attempt_count":     gorm.Expr("attempt_count + 1"),
				"lease_expires_at":  now.Add(time.Minute),
				"lease_id":          "target-delivery-lease",
				"writer_epoch":      targetEpoch,
				"processing_status": models.GithubWebhookDeliveryProcessing,
				"updated_at":        now,
			}).Error)
			var reclaimed models.GithubWebhookDelivery
			require.NoError(t, database.GormDB.First(&reclaimed, "delivery_id = ?", delivery.DeliveryID).Error)
			request.Identity.WriterEpoch = targetEpoch
			request.Identity.DeliveryLeaseID = reclaimed.LeaseID
			recoveredBatchID, recoveredJobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, *batchID, *recoveredBatchID)
			require.Len(t, recoveredJobs, len(jobs))
			for projectName, originalJob := range jobs {
				require.Equal(t, originalJob.ID, recoveredJobs[projectName].ID)
			}
		})
	}
}

func TestPostgresConvertJobsToDiggerJobsDurableRejectsConcurrentDivergentRetries(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	firstRequest := durableGraphTestRequest(t, organisation, delivery)
	secondRequest := durableGraphTestRequest(t, organisation, delivery)
	secondRequest.CommitSHA = "feedface"
	start := make(chan struct{})
	errorsByWorker := make(chan error, 2)
	var workers sync.WaitGroup
	for _, request := range []DurableJobGraphRequest{firstRequest, secondRequest} {
		request := request
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			errorsByWorker <- err
		}()
	}
	close(start)
	workers.Wait()
	close(errorsByWorker)

	var succeeded int
	var conflicted int
	for workerErr := range errorsByWorker {
		if workerErr == nil {
			succeeded++
		} else if errors.Is(workerErr, ErrDurableJobGraphConflict) {
			conflicted++
		} else {
			require.NoError(t, workerErr)
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, conflicted)
	assertDurableGraphCounts(t, database, 1, 3, 2)
}

func TestPostgresGithubInstallationActiveLinkIsUnique(t *testing.T) {
	database, _, _ := newPostgresDurableGraphTestDatabase(t)
	firstOrganisation := &models.Organisation{Name: "first", ExternalSource: "test", ExternalId: "active-link-first"}
	secondOrganisation := &models.Organisation{Name: "second", ExternalSource: "test", ExternalId: "active-link-second"}
	require.NoError(t, database.GormDB.Create(firstOrganisation).Error)
	require.NoError(t, database.GormDB.Create(secondOrganisation).Error)

	const installationID = int64(999)
	start := make(chan struct{})
	errorsByWorker := make(chan error, 2)
	var workers sync.WaitGroup
	for _, organisationID := range []uint{firstOrganisation.ID, secondOrganisation.ID} {
		organisationID := organisationID
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			errorsByWorker <- database.GormDB.Create(&models.GithubAppInstallationLink{
				GithubInstallationId: installationID,
				OrganisationId:       organisationID,
				Status:               models.GithubAppInstallationLinkActive,
			}).Error
		}()
	}
	close(start)
	workers.Wait()
	close(errorsByWorker)

	var succeeded int
	var rejected int
	for workerErr := range errorsByWorker {
		if workerErr == nil {
			succeeded++
		} else {
			rejected++
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, rejected)
}

func TestPostgresConvertJobsToDiggerJobsDurableLocksTenantLink(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	linkLocked := make(chan struct{})
	releaseGraph := make(chan struct{})
	var signalOnce sync.Once
	require.NoError(t, database.GormDB.Callback().Query().After("gorm:query").Register("durable_graph_test:block_after_link_lock", func(tx *gorm.DB) {
		if tx.Statement.Table == "github_app_installation_links" {
			signalOnce.Do(func() {
				close(linkLocked)
				<-releaseGraph
			})
		}
	}))

	graphDone := make(chan error, 1)
	go func() {
		_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
		graphDone <- err
	}()
	<-linkLocked

	deactivationDone := make(chan error, 1)
	go func() {
		deactivationDone <- database.GormDB.Model(&models.GithubAppInstallationLink{}).
			Where("github_installation_id = ?", delivery.InstallationIDValue()).
			Update("status", models.GithubAppInstallationLinkInactive).Error
	}()
	select {
	case err := <-deactivationDone:
		require.NoError(t, err)
		t.Fatal("tenant link deactivation completed while graph transaction held a share lock")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseGraph)
	require.NoError(t, <-graphDone)
	require.NoError(t, <-deactivationDone)
}

func assertDurableGraphIsAtomicAndIdempotent(t *testing.T, database *models.Database, organisation *models.Organisation, delivery *models.GithubWebhookDelivery) {
	t.Helper()
	request := durableGraphTestRequest(t, organisation, delivery)

	batchID, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, batchID)
	require.Len(t, jobs, 3)
	firstJobIDs := map[string]uint{}
	firstTokens := map[string]string{}
	for projectName, job := range jobs {
		firstJobIDs[projectName] = job.ID
		var token models.JobToken
		require.NoError(t, database.GormDB.Where("digger_job_database_id = ?", job.ID).First(&token).Error)
		firstTokens[projectName] = token.Value
		var spec scheduler.JobJson
		require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
		require.Equal(t, token.Value, spec.BackendJobToken)
	}

	var operationCount int64
	require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Count(&operationCount).Error)
	require.Equal(t, int64(5), operationCount)
	var parentLinks []models.DiggerJobParentLink
	require.NoError(t, database.GormDB.Find(&parentLinks).Error)
	require.Len(t, parentLinks, 2)
	require.Equal(t, jobs["child"].DiggerJobID, parentLinks[0].DiggerJobId)
	require.Equal(t, jobs["child"].DiggerJobID, parentLinks[1].DiggerJobId)

	retryBatchID, retryJobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, *batchID, *retryBatchID)
	require.Len(t, retryJobs, 3)
	for projectName, job := range retryJobs {
		require.Equal(t, firstJobIDs[projectName], job.ID)
		var token models.JobToken
		require.NoError(t, database.GormDB.Where("digger_job_database_id = ?", job.ID).First(&token).Error)
		require.Equal(t, firstTokens[projectName], token.Value)
	}

	for model, expectedCount := range map[any]int64{
		&models.DiggerBatch{}:         1,
		&models.DiggerJob{}:           3,
		&models.DiggerJobSummary{}:    3,
		&models.JobToken{}:            3,
		&models.GithubDiggerJobLink{}: 3,
		&models.OutboxEffect{}:        2,
		&models.DiggerJobParentLink{}: 2,
		&models.OutboxEffect{}:        2,
	} {
		var count int64
		require.NoError(t, database.GormDB.Model(model).Count(&count).Error)
		require.Equal(t, expectedCount, count)
	}
}

func TestConvertJobsToDiggerJobsDurableRollsBackEveryWrite(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	injectedErr := errors.New("injected parent-link failure")
	require.NoError(t, database.GormDB.Callback().Create().Before("gorm:create").Register("durable_graph_test:fail_parent_link", func(tx *gorm.DB) {
		if tx.Statement.Table == "digger_job_parent_links" {
			tx.AddError(injectedErr)
		}
	}))

	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.ErrorIs(t, err, injectedErr)
	for model := range map[any]struct{}{
		&models.DiggerBatch{}:         {},
		&models.DiggerJob{}:           {},
		&models.DiggerJobSummary{}:    {},
		&models.JobToken{}:            {},
		&models.GithubDiggerJobLink{}: {},
		&models.DiggerJobParentLink{}: {},
		&models.OutboxEffect{}:        {},
	} {
		var count int64
		require.NoError(t, database.GormDB.Model(model).Count(&count).Error)
		require.Zero(t, count)
	}
	var operationCount int64
	require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Count(&operationCount).Error)
	require.Equal(t, int64(1), operationCount)
}

func TestPostgresPrepareDurableJobDispatchActivatesExpiredTokenFromDatabaseTime(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)

	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().Add(-24*time.Hour), "dispatch-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, effect)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	var tokenBefore models.JobToken
	require.NoError(t, database.GormDB.First(&tokenBefore, "digger_job_database_id = ?", job.ID).Error)
	require.Nil(t, tokenBefore.ActivatedAt)
	require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("id = ?", tokenBefore.ID).Update("expiry", time.Now().Add(-3*time.Hour)).Error)

	const validity = 31 * 24 * time.Hour
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, validity, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, preparation.SkipProvider)
	require.Equal(t, job.ID, preparation.Job.ID)
	var tokenAfter models.JobToken
	require.NoError(t, database.GormDB.First(&tokenAfter, "id = ?", tokenBefore.ID).Error)
	require.NotNil(t, tokenAfter.ActivatedAt)
	require.True(t, tokenAfter.Expiry.After(time.Now().UTC().Add(validity-time.Minute)))

	activatedAt := *tokenAfter.ActivatedAt
	firstExpiry := tokenAfter.Expiry
	preparation, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, validity, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, preparation.SkipProvider)
	require.NoError(t, database.GormDB.First(&tokenAfter, "id = ?", tokenBefore.ID).Error)
	require.Equal(t, activatedAt, *tokenAfter.ActivatedAt)
	require.False(t, tokenAfter.Expiry.Before(firstExpiry))
}

func TestPostgresPrepareDurableJobDispatchRejectsLeaseExpiredWhileWaitingForJobLock(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "short-dispatch-lease", 150*time.Millisecond, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, effect)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	blocker := database.GormDB.Begin()
	require.NoError(t, blocker.Error)
	blockerOpen := true
	t.Cleanup(func() {
		if blockerOpen {
			blocker.Rollback()
		}
	})
	require.NoError(t, blocker.Clauses(clause.Locking{Strength: "UPDATE"}).First(&models.DiggerJob{}, "id = ?", job.ID).Error)

	done := make(chan error, 1)
	go func() {
		_, prepareErr := database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		done <- prepareErr
	}()
	time.Sleep(250 * time.Millisecond)
	require.NoError(t, blocker.Commit().Error)
	blockerOpen = false
	require.ErrorIs(t, <-done, models.ErrDurableJobDispatchClaim)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.Nil(t, token.ActivatedAt)
}

func TestPostgresPrepareDurableJobDispatchRefreshesLeaseAfterWaitingForJobLock(t *testing.T) {
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "refresh-dispatch-lease", 800*time.Millisecond, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, effect)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	blocker := database.GormDB.Begin()
	require.NoError(t, blocker.Error)
	blockerOpen := true
	t.Cleanup(func() {
		if blockerOpen {
			blocker.Rollback()
		}
	})
	require.NoError(t, blocker.Clauses(clause.Locking{Strength: "UPDATE"}).First(&models.DiggerJob{}, "id = ?", job.ID).Error)

	done := make(chan error, 1)
	go func() {
		_, prepareErr := database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
		done <- prepareErr
	}()
	time.Sleep(400 * time.Millisecond)
	require.NoError(t, blocker.Commit().Error)
	blockerOpen = false
	require.NoError(t, <-done)
	var refreshed models.OutboxEffect
	require.NoError(t, database.GormDB.First(&refreshed, "id = ?", effect.ID).Error)
	require.NotNil(t, refreshed.LeaseExpiresAt)
	require.True(t, refreshed.LeaseExpiresAt.After(time.Now().Add(55*time.Second)))
}

func TestPrepareDurableJobDispatchRejectsStaleClaimAndMismatchedToken(t *testing.T) {
	testCases := map[string]func(*testing.T, *models.Database, *models.OutboxEffect, *models.DiggerJob){
		"wrong lease": func(_ *testing.T, _ *models.Database, effect *models.OutboxEffect, _ *models.DiggerJob) {
			effect.LeaseID = "wrong-lease"
		},
		"expired lease": func(t *testing.T, database *models.Database, effect *models.OutboxEffect, _ *models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error)
		},
		"serialized token": func(t *testing.T, database *models.Database, _ *models.OutboxEffect, job *models.DiggerJob) {
			var spec scheduler.JobJson
			require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
			spec.BackendJobToken = "cli:another-token"
			serialized, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("serialized_job_spec", serialized).Error)
		},
		"backend hostname": func(t *testing.T, database *models.Database, _ *models.OutboxEffect, job *models.DiggerJob) {
			var spec scheduler.JobJson
			require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
			spec.BackendHostname = "https://attacker.example"
			serialized, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("serialized_job_spec", serialized).Error)
		},
	}
	for name, mutate := range testCases {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "dispatch-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.NotNil(t, effect)
			var job models.DiggerJob
			require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
			mutate(t, database, effect, &job)
			_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			if name == "serialized token" || name == "backend hostname" {
				require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
			} else {
				require.ErrorIs(t, err, models.ErrDurableJobDispatchClaim)
			}
			var token models.JobToken
			require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
			require.Nil(t, token.ActivatedAt)
		})
	}
}

func TestPrepareDurableJobDispatchDoesNotReactivateTerminalJob(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "dispatch-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, effect)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, preparation.SkipProvider)
	var activeToken models.JobToken
	require.NoError(t, database.GormDB.First(&activeToken, "digger_job_database_id = ?", job.ID).Error)
	require.NotNil(t, activeToken.ActivatedAt)
	require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("status", scheduler.DiggerJobSucceeded).Error)

	preparation, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, preparation.SkipProvider)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.Equal(t, activeToken.ActivatedAt, token.ActivatedAt)
	require.NotNil(t, token.RevokedAt)
	var jobOperation models.ControlOperation
	require.NoError(t, database.GormDB.First(&jobOperation, "operation_id = ?", effect.ControlOperationID).Error)
	require.Equal(t, models.ControlOperationCompleted, jobOperation.Status)
}

func TestPrepareDurableJobDispatchRejectsNeverActivatedTerminalState(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "unactivated-terminal-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("status", scheduler.DiggerJobSucceeded).Error)

	_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
	require.NoError(t, database.DeadLetterOutboxEffect(context.Background(), effect.ID, effect.LeaseID, "invalid terminal state", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
	var jobOperation models.ControlOperation
	require.NoError(t, database.GormDB.First(&jobOperation, "operation_id = ?", effect.ControlOperationID).Error)
	require.Equal(t, models.ControlOperationPending, jobOperation.Status)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.Nil(t, token.ActivatedAt)
	require.Nil(t, token.RevokedAt)
	var storedEffect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&storedEffect, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectDeadLetter, storedEffect.Status)
}

func TestPrepareDurableJobDispatchAcceptsEffectReclaimedByNewWriterEpoch(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)

	const targetEpoch = durableGraphTestWriterEpoch + 1
	require.NoError(t, database.GormDB.Model(&models.ControlPlaneFence{}).
		Where("id = ?", models.ControlPlaneFenceSingletonID).
		Update("writer_epoch", targetEpoch).Error)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "target-dispatch-lease", time.Minute, durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.NotNil(t, effect)
	require.Equal(t, targetEpoch, effect.WriterEpoch)

	preparation, err := database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, targetEpoch)
	require.NoError(t, err)
	require.False(t, preparation.SkipProvider)
	require.Equal(t, durableGraphTestWriterEpoch, *preparation.Job.WriterEpoch)
	_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrControlPlaneFenced)
}

func TestDurableWorkflowDispatchRejectsRouteTamperingBeforeProviderOrTerminalMutation(t *testing.T) {
	testCases := map[string]func(*testing.T, *models.Database, *models.DiggerJob, *models.OutboxEffect){
		"child delivery": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			installationID := int64(123)
			second, _, err := database.RecordGithubWebhookDelivery(context.Background(), &models.GithubWebhookDelivery{
				DeliveryID:         "second-signed-delivery",
				PayloadSHA256:      "second-payload",
				Payload:            []byte(`{"action":"opened"}`),
				EventType:          "pull_request",
				GithubAppID:        456,
				InstallationID:     &installationID,
				RepositoryFullName: "monoai-co/sre",
			}, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *job.OperationID).Update("delivery_id", second.DeliveryID).Error)
		},
		"batch repository": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *job.BatchID).Update("repo_full_name", "monoai-co/marketplace-infra").Error)
		},
		"batch installation": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *job.BatchID).Update("github_installation_id", int64(999)).Error)
		},
		"batch operation": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *job.BatchID).Update("operation_id", *job.OperationID).Error)
		},
		"batch epoch": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *job.BatchID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
		},
		"batch protocol": func(t *testing.T, database *models.Database, job *models.DiggerJob, _ *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", *job.BatchID).Update("protocol_version", operation.ProtocolVersion+1).Error)
		},
		"effect payload": func(t *testing.T, database *models.Database, _ *models.DiggerJob, effect *models.OutboxEffect) {
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("id = ?", effect.ID).Update("payload", []byte(`{"operation_id":"tampered","digger_job_id":"tampered"}`)).Error)
		},
	}

	for name, tamper := range testCases {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "tamper-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.NotNil(t, effect)
			var job models.DiggerJob
			require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
			tamper(t, database, &job, effect)

			_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
			err = database.CompleteOutboxEffect(context.Background(), effect.ID, effect.LeaseID, []byte(`{"accepted":true}`), time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
			err = database.DeadLetterOutboxEffect(context.Background(), effect.ID, effect.LeaseID, "provider failure", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)

			var storedEffect models.OutboxEffect
			require.NoError(t, database.GormDB.First(&storedEffect, "id = ?", effect.ID).Error)
			require.Equal(t, models.OutboxEffectDeadLetter, storedEffect.Status)
			var storedJob models.DiggerJob
			require.NoError(t, database.GormDB.First(&storedJob, "id = ?", job.ID).Error)
			require.Equal(t, scheduler.DiggerJobCreated, storedJob.Status)
			var token models.JobToken
			require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
			require.Nil(t, token.ActivatedAt)
			require.Nil(t, token.RevokedAt)
		})
	}
}

func TestDurableWorkflowDispatchDeadLettersMissingRouteWithoutMutatingJob(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "missing-route-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	require.NoError(t, database.GormDB.Delete(&models.DiggerJob{}, job.ID).Error)

	require.NoError(t, database.DeadLetterOutboxEffect(context.Background(), effect.ID, effect.LeaseID, "missing route", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
	var storedEffect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&storedEffect, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectDeadLetter, storedEffect.Status)
	var storedJob models.DiggerJob
	require.NoError(t, database.GormDB.Unscoped().First(&storedJob, "id = ?", job.ID).Error)
	require.Equal(t, scheduler.DiggerJobCreated, storedJob.Status)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
	require.Nil(t, token.ActivatedAt)
	require.Nil(t, token.RevokedAt)
}

func TestDurableWorkflowDispatchRequiresEveryBoundParentToSucceed(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	child := jobs["child"]
	payload, err := json.Marshal(models.GithubWorkflowDispatchPayload{OperationID: *child.OperationID, DiggerJobID: child.DiggerJobID})
	require.NoError(t, err)
	childEffect := models.NewOutboxEffect(*child.OperationID, models.GithubWorkflowDispatchEffectKind, "job:"+*child.OperationID, payload, durableGraphTestWriterEpoch, time.Now().UTC())
	_, created, err := database.EnqueueOutboxEffect(context.Background(), childEffect, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).
		Where("operation_id <> ?", *child.OperationID).
		Update("status", models.OutboxEffectSucceeded).Error)
	claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "child-dispatch-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.Equal(t, childEffect.ID, claimed.ID)

	_, err = database.PrepareDurableJobDispatch(context.Background(), claimed.ID, claimed.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.ErrorIs(t, err, models.ErrDurableJobDispatchConflict)
	var childToken models.JobToken
	require.NoError(t, database.GormDB.First(&childToken, "digger_job_database_id = ?", child.ID).Error)
	require.Nil(t, childToken.ActivatedAt)

	now := time.Now().UTC()
	for _, parentName := range []string{"root-one", "root-two"} {
		parent := jobs[parentName]
		require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", parent.ID).Update("status", scheduler.DiggerJobSucceeded).Error)
		require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *parent.OperationID).Update("status", models.ControlOperationCompleted).Error)
		require.NoError(t, database.GormDB.Model(&models.JobToken{}).Where("digger_job_database_id = ?", parent.ID).Updates(map[string]any{
			"activated_at": now.Add(-time.Minute), "expiry": now, "revoked_at": now,
		}).Error)
	}
	preparation, err := database.PrepareDurableJobDispatch(context.Background(), claimed.ID, claimed.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.False(t, preparation.SkipProvider)
	require.Equal(t, child.ID, preparation.Job.ID)
	require.NoError(t, database.GormDB.First(&childToken, "digger_job_database_id = ?", child.ID).Error)
	require.NotNil(t, childToken.ActivatedAt)
}

func TestDurableWorkflowDeadLetterReconcilesTerminalCallbackRace(t *testing.T) {
	for name, terminalJobStatus := range map[string]scheduler.DiggerJobStatus{
		"succeeded": scheduler.DiggerJobSucceeded,
		"failed":    scheduler.DiggerJobFailed,
	} {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "terminal-race-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.NotNil(t, effect)
			var job models.DiggerJob
			require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
			_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("status", terminalJobStatus).Error)
			if terminalJobStatus == scheduler.DiggerJobFailed {
				var batch models.DiggerBatch
				require.NoError(t, database.GormDB.First(&batch, "id = ?", *job.BatchID).Error)
				require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *batch.OperationID).Update("status", models.ControlOperationFailed).Error)
			}

			require.NoError(t, database.DeadLetterOutboxEffect(context.Background(), effect.ID, effect.LeaseID, "late provider failure", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch))
			var storedEffect models.OutboxEffect
			require.NoError(t, database.GormDB.First(&storedEffect, "id = ?", effect.ID).Error)
			require.Equal(t, models.OutboxEffectSucceeded, storedEffect.Status)
			var operation models.ControlOperation
			require.NoError(t, database.GormDB.First(&operation, "operation_id = ?", effect.ControlOperationID).Error)
			if terminalJobStatus == scheduler.DiggerJobSucceeded {
				require.Equal(t, models.ControlOperationCompleted, operation.Status)
			} else {
				require.Equal(t, models.ControlOperationFailed, operation.Status)
			}
			var token models.JobToken
			require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", job.ID).Error)
			require.NotNil(t, token.RevokedAt)
			require.False(t, token.Expiry.After(*token.RevokedAt))
			var storedJob models.DiggerJob
			require.NoError(t, database.GormDB.First(&storedJob, "id = ?", job.ID).Error)
			require.Equal(t, terminalJobStatus, storedJob.Status)
		})
	}
}

func TestDurableWorkflowDeadLetterNeverDowngradesCompletedOperation(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	effect, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "inconsistent-terminal-lease", time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	var job models.DiggerJob
	require.NoError(t, database.GormDB.First(&job, "operation_id = ?", effect.ControlOperationID).Error)
	_, err = database.PrepareDurableJobDispatch(context.Background(), effect.ID, effect.LeaseID, time.Hour, time.Minute, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("status", scheduler.DiggerJobFailed).Error)
	require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", effect.ControlOperationID).Update("status", models.ControlOperationCompleted).Error)

	err = database.DeadLetterOutboxEffect(context.Background(), effect.ID, effect.LeaseID, "late provider failure", time.Now().UTC(), durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.NoError(t, err)
	var operation models.ControlOperation
	require.NoError(t, database.GormDB.First(&operation, "operation_id = ?", effect.ControlOperationID).Error)
	require.Equal(t, models.ControlOperationCompleted, operation.Status)
	var storedEffect models.OutboxEffect
	require.NoError(t, database.GormDB.First(&storedEffect, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectDeadLetter, storedEffect.Status)
}

func TestDurableGraphEnforcesUniqueTokenAndWorkflowDispatch(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	var token models.JobToken
	require.NoError(t, database.GormDB.First(&token, "digger_job_database_id = ?", jobs["root-one"].ID).Error)
	token.ID = 0
	token.DiggerJobDatabaseID = nil
	require.Error(t, database.GormDB.Create(&token).Error)

	operationID := *jobs["root-one"].OperationID
	payload, err := json.Marshal(models.GithubWorkflowDispatchPayload{OperationID: operationID, DiggerJobID: jobs["root-one"].DiggerJobID})
	require.NoError(t, err)
	secondEffect := models.NewOutboxEffect(operationID, models.GithubWorkflowDispatchEffectKind, "job:changed", payload, durableGraphTestWriterEpoch, time.Now().UTC())
	_, _, err = database.EnqueueOutboxEffect(context.Background(), secondEffect, durableGraphTestDatabaseIdentity, durableGraphTestWriterEpoch)
	require.Error(t, err)
}

func TestConvertJobsToDiggerJobsDurableRejectsRetryDrift(t *testing.T) {
	_, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)

	testCases := map[string]func(DurableJobGraphRequest) DurableJobGraphRequest{
		"commit": func(changed DurableJobGraphRequest) DurableJobGraphRequest {
			changed.CommitSHA = "feedface"
			return changed
		},
		"reporter": func(changed DurableJobGraphRequest) DurableJobGraphRequest {
			changed.JobReporterType = "reporter"
			return changed
		},
		"configuration": func(changed DurableJobGraphRequest) DurableJobGraphRequest {
			changed.DiggerConfig = "projects: [changed]"
			return changed
		},
		"coverage": func(changed DurableJobGraphRequest) DurableJobGraphRequest {
			changed.CoverAllImpactedProjects = false
			return changed
		},
		"workflow": func(changed DurableJobGraphRequest) DurableJobGraphRequest {
			projects := make(map[string]configuration.Project, len(changed.Projects))
			for name, project := range changed.Projects {
				projects[name] = project
			}
			child := projects["child"]
			child.WorkflowFile = "changed_workflow.yml"
			projects["child"] = child
			changed.Projects = projects
			return changed
		},
	}
	for name, change := range testCases {
		t.Run(name, func(t *testing.T) {
			_, _, retryErr := ConvertJobsToDiggerJobsDurable(context.Background(), change(request))
			require.ErrorIs(t, retryErr, ErrDurableJobGraphConflict)
		})
	}
}

func TestConvertJobsToDiggerJobsDurableRetryIgnoresRuntimeDerivedDrift(t *testing.T) {
	t.Setenv("PUBLIC_BASE_URL", "https://source.opentaco.example")
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	batchID, firstJobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)

	t.Setenv("PUBLIC_BASE_URL", "https://target.opentaco.example")
	require.NoError(t, database.GormDB.Model(&models.Organisation{}).Where("id = ?", organisation.ID).Update("name", "renamed-organisation").Error)
	retryBatchID, retryJobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, *batchID, *retryBatchID)
	for projectName, firstJob := range firstJobs {
		require.Equal(t, firstJob.ID, retryJobs[projectName].ID)
		var persistedSpec scheduler.JobJson
		require.NoError(t, json.Unmarshal(retryJobs[projectName].SerializedJobSpec, &persistedSpec))
		require.Equal(t, "https://source.opentaco.example", persistedSpec.BackendHostname)
		require.Equal(t, "test-organisation", persistedSpec.BackendOrganisationName)
	}
}

func TestConvertJobsToDiggerJobsDurableRejectsPersistedGraphTampering(t *testing.T) {
	testCases := map[string]func(*testing.T, *models.Database, map[string]*models.DiggerJob){
		"batch provider": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", jobs["root-one"].BatchID).Update("vcs", models.DiggerVCSBitbucket).Error)
		},
		"batch writer epoch": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Where("id = ?", jobs["root-one"].BatchID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
		},
		"batch operation writer epoch": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			var batch models.DiggerBatch
			require.NoError(t, database.GormDB.First(&batch, "id = ?", jobs["root-one"].BatchID).Error)
			require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *batch.OperationID).Update("writer_epoch", durableGraphTestWriterEpoch+1).Error)
		},
		"batch operation protocol": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			var batch models.DiggerBatch
			require.NoError(t, database.GormDB.First(&batch, "id = ?", jobs["root-one"].BatchID).Error)
			require.NoError(t, database.GormDB.Model(&models.ControlOperation{}).Where("operation_id = ?", *batch.OperationID).Update("protocol_version", operation.ProtocolVersion+1).Error)
		},
		"extra provider link": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Omit("DiggerJob").Create(&models.GithubDiggerJobLink{
				Status:       models.DiggerJobLinkCreated,
				DiggerJobId:  jobs["root-one"].DiggerJobID,
				RepoFullName: "monoai-co/other",
			}).Error)
		},
		"boundary parent link": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			summary := &models.DiggerJobSummary{}
			require.NoError(t, database.GormDB.Create(summary).Error)
			external := &models.DiggerJob{
				DiggerJobID:        "external-job",
				Status:             scheduler.DiggerJobCreated,
				DiggerJobSummaryID: summary.ID,
				DiggerJobSummary:   *summary,
			}
			require.NoError(t, database.GormDB.Omit("Operation", "Batch", "DiggerJobSummary").Create(external).Error)
			require.NoError(t, database.GormDB.Omit("DiggerJob", "ParentDiggerJob").Create(&models.DiggerJobParentLink{
				ParentDiggerJobId: jobs["root-one"].DiggerJobID,
				DiggerJobId:       external.DiggerJobID,
			}).Error)
		},
		"backend hostname": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			job := jobs["root-one"]
			var spec scheduler.JobJson
			require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
			spec.BackendHostname = "https://attacker.example"
			serialized, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("serialized_job_spec", serialized).Error)
		},
		"backend organisation": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			job := jobs["root-one"]
			var spec scheduler.JobJson
			require.NoError(t, json.Unmarshal(job.SerializedJobSpec, &spec))
			spec.BackendOrganisationName = "attacker"
			serialized, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, database.GormDB.Model(&models.DiggerJob{}).Where("id = ?", job.ID).Update("serialized_job_spec", serialized).Error)
		},
		"dispatch key": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", *jobs["root-one"].OperationID).Update("effect_key", "job:tampered").Error)
		},
		"dispatch payload digest": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", *jobs["root-one"].OperationID).Update("payload_sha256", "tampered").Error)
		},
		"dispatch payload bytes": func(t *testing.T, database *models.Database, jobs map[string]*models.DiggerJob) {
			require.NoError(t, database.GormDB.Model(&models.OutboxEffect{}).Where("operation_id = ?", *jobs["root-one"].OperationID).Update("payload", []byte(`{"operation_id":"tampered","digger_job_id":"tampered"}`)).Error)
		},
	}

	for name, tamper := range testCases {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			_, jobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.NoError(t, err)
			tamper(t, database, jobs)
			_, _, err = ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.ErrorIs(t, err, ErrDurableJobGraphConflict)
		})
	}
}

func TestConvertJobsToDiggerJobsDurableBindsClaimedDeliveryTenant(t *testing.T) {
	testCases := map[string]func(*testing.T, *models.Database, *models.Organisation, *models.GithubWebhookDelivery, *DurableJobGraphRequest){
		"organisation": func(t *testing.T, database *models.Database, _ *models.Organisation, _ *models.GithubWebhookDelivery, request *DurableJobGraphRequest) {
			other := &models.Organisation{Name: "other", ExternalSource: "test", ExternalId: "tenant-other"}
			require.NoError(t, database.GormDB.Create(other).Error)
			request.OrganisationID = other.ID
		},
		"installation": func(_ *testing.T, _ *models.Database, _ *models.Organisation, _ *models.GithubWebhookDelivery, request *DurableJobGraphRequest) {
			request.GithubInstallationID++
		},
		"repository": func(_ *testing.T, _ *models.Database, _ *models.Organisation, _ *models.GithubWebhookDelivery, request *DurableJobGraphRequest) {
			request.RepoFullName = "monoai-co/marketplace-infra"
			request.RepoName = "marketplace-infra"
		},
		"repository components": func(_ *testing.T, _ *models.Database, _ *models.Organisation, _ *models.GithubWebhookDelivery, request *DurableJobGraphRequest) {
			request.RepoOwner = "another-owner"
		},
		"missing link": func(t *testing.T, database *models.Database, _ *models.Organisation, delivery *models.GithubWebhookDelivery, _ *DurableJobGraphRequest) {
			require.NoError(t, database.GormDB.Unscoped().Where("github_installation_id = ?", delivery.InstallationIDValue()).Delete(&models.GithubAppInstallationLink{}).Error)
		},
		"inactive link": func(t *testing.T, database *models.Database, _ *models.Organisation, delivery *models.GithubWebhookDelivery, _ *DurableJobGraphRequest) {
			require.NoError(t, database.GormDB.Model(&models.GithubAppInstallationLink{}).Where("github_installation_id = ?", delivery.InstallationIDValue()).Update("status", models.GithubAppInstallationLinkInactive).Error)
		},
		"soft deleted link": func(t *testing.T, database *models.Database, _ *models.Organisation, delivery *models.GithubWebhookDelivery, _ *DurableJobGraphRequest) {
			require.NoError(t, database.GormDB.Where("github_installation_id = ?", delivery.InstallationIDValue()).Delete(&models.GithubAppInstallationLink{}).Error)
		},
		"cross-organisation connection": func(t *testing.T, database *models.Database, _ *models.Organisation, delivery *models.GithubWebhookDelivery, request *DurableJobGraphRequest) {
			other := &models.Organisation{Name: "connection-other", ExternalSource: "test", ExternalId: "connection-other"}
			require.NoError(t, database.GormDB.Create(other).Error)
			connection := &models.VCSConnection{GithubId: delivery.GithubAppID, VCSType: models.DiggerVCSGithub, OrganisationID: other.ID}
			require.NoError(t, database.GormDB.Create(connection).Error)
			request.VCSConnectionID = &connection.ID
		},
	}

	for name, mutate := range testCases {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			mutate(t, database, organisation, delivery, &request)
			_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
			require.ErrorIs(t, err, ErrDurableJobGraphTenant)
			assertDurableGraphCounts(t, database, 0, 0, 0)
		})
	}
}

func TestConvertJobsToDiggerJobsDurableRejectsDuplicateActiveTenantLinks(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	require.NoError(t, database.GormDB.Exec("DROP INDEX idx_github_installation_active_link").Error)
	other := &models.Organisation{Name: "duplicate", ExternalSource: "test", ExternalId: "tenant-duplicate"}
	require.NoError(t, database.GormDB.Create(other).Error)
	require.NoError(t, database.GormDB.Create(&models.GithubAppInstallationLink{
		GithubInstallationId: delivery.InstallationIDValue(),
		OrganisationId:       other.ID,
		Status:               models.GithubAppInstallationLinkActive,
	}).Error)

	request := durableGraphTestRequest(t, organisation, delivery)
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.ErrorIs(t, err, ErrDurableJobGraphTenant)
	assertDurableGraphCounts(t, database, 0, 0, 0)
}

func TestConvertJobsToDiggerJobsDurableRejectsUnownedOrExpiredClaim(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	request.Identity.DeliveryLeaseID = "another-worker"
	_, _, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.ErrorIs(t, err, ErrDurableJobGraphClaim)

	request.Identity.DeliveryLeaseID = delivery.LeaseID
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).
		Where("delivery_id = ?", delivery.DeliveryID).
		Update("lease_expires_at", time.Now().Add(-time.Minute)).Error)
	_, _, err = ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.ErrorIs(t, err, ErrDurableJobGraphClaim)

	var batchCount int64
	require.NoError(t, database.GormDB.Model(&models.DiggerBatch{}).Count(&batchCount).Error)
	require.Zero(t, batchCount)
}

func assertDurableGraphCounts(t *testing.T, database *models.Database, expectedBatches int64, expectedJobs int64, expectedEffects int64) {
	t.Helper()
	for model, expectedCount := range map[any]int64{
		&models.DiggerBatch{}:         expectedBatches,
		&models.DiggerJob{}:           expectedJobs,
		&models.DiggerJobSummary{}:    expectedJobs,
		&models.JobToken{}:            expectedJobs,
		&models.GithubDiggerJobLink{}: expectedJobs,
		&models.OutboxEffect{}:        expectedEffects,
	} {
		var count int64
		require.NoError(t, database.GormDB.Model(model).Count(&count).Error)
		require.Equal(t, expectedCount, count)
	}
}
