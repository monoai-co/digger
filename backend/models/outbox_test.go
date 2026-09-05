package models

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newOutboxTestDatabase(t *testing.T) *Database {
	database := newGithubWebhookDeliveryTestDatabase(t)
	require.NoError(t, database.GormDB.AutoMigrate(&OutboxEffect{}, &GithubReportCreateAttempt{}))
	return database
}

func newPostgresOutboxTestDatabase(t *testing.T) *Database {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	adminDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQLDB, err := adminDB.DB()
	require.NoError(t, err)
	schema := "outbox_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, adminDB.Exec("CREATE SCHEMA "+schema).Error)

	schemaDSN := dsn
	parsedDSN, parseErr := url.Parse(dsn)
	if parseErr == nil && (parsedDSN.Scheme == "postgres" || parsedDSN.Scheme == "postgresql") {
		query := parsedDSN.Query()
		query.Set("search_path", schema)
		parsedDSN.RawQuery = query.Encode()
		schemaDSN = parsedDSN.String()
	} else {
		schemaDSN += " search_path=" + schema
	}
	gormDB, err := gorm.Open(postgres.Open(schemaDSN), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(16)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
		require.NoError(t, adminDB.Exec("DROP SCHEMA "+schema+" CASCADE").Error)
		require.NoError(t, adminSQLDB.Close())
	})

	require.NoError(t, gormDB.AutoMigrate(&ControlPlaneFence{}, &GithubWebhookOrderingDomain{}, &GithubWebhookDelivery{}, &GithubWebhookDeliveryRequeue{}, &ControlOperation{}, &OutboxEffect{}, &GithubReportCreateAttempt{}))
	require.NoError(t, gormDB.Create(&ControlPlaneFence{
		ID:               ControlPlaneFenceSingletonID,
		DatabaseIdentity: testControlPlaneDatabaseIdentity,
		WriterEpoch:      testControlPlaneWriterEpoch,
		Mode:             ControlPlaneModeNormal,
		ProtocolFloor:    1,
		UpdatedAt:        time.Now().UTC(),
	}).Error)
	return &Database{GormDB: gormDB}
}

func createOutboxTestOperation(t *testing.T, database *Database, operationID string) {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, database.GormDB.Create(&ControlOperation{
		OperationID:     operationID,
		OperationKind:   "test",
		IdentitySHA256:  "identity",
		WriterEpoch:     testControlPlaneWriterEpoch,
		ProtocolVersion: 1,
		Status:          ControlOperationPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}).Error)
}

func TestOutboxEffectEnqueueIsImmutableAndIdempotent(t *testing.T) {
	database := newOutboxTestDatabase(t)
	createOutboxTestOperation(t, database, "operation-1")
	now := time.Now().UTC()
	effect := NewOutboxEffect("operation-1", "workflow_dispatch", "job-1", []byte(`{"job":"one"}`), testControlPlaneWriterEpoch, now)

	receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, effect.ID, receipt.ID)

	duplicate := NewOutboxEffect("operation-1", "workflow_dispatch", "job-1", []byte(`{"job":"one"}`), testControlPlaneWriterEpoch, now)
	receipt, created, err = database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, effect.ID, receipt.ID)

	conflict := NewOutboxEffect("operation-1", "workflow_dispatch", "job-1", []byte(`{"job":"changed"}`), testControlPlaneWriterEpoch, now)
	_, created, err = database.EnqueueOutboxEffect(context.Background(), conflict, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrOutboxEffectConflict)
	require.False(t, created)

	forgedDigest := NewOutboxEffect("operation-1", "workflow_dispatch", "job-1", []byte(`{"job":"forged"}`), testControlPlaneWriterEpoch, now)
	forgedDigest.PayloadSHA256 = effect.PayloadSHA256
	_, created, err = database.EnqueueOutboxEffect(context.Background(), forgedDigest, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrOutboxEffectConflict)
	require.False(t, created)
}

func TestEnqueueOutboxEffectTxCommitsAndRollsBackWithBusinessState(t *testing.T) {
	database := newOutboxTestDatabase(t)
	const operationID = "operation-atomic-outbox"
	createOutboxTestOperation(t, database, operationID)
	effect := NewOutboxEffect(operationID, "workflow_dispatch", "job-atomic", []byte(`{"job":"atomic"}`), testControlPlaneWriterEpoch, time.Now().UTC())
	forcedRollback := errors.New("force transaction rollback")

	err := database.WithAuthoritativeWriteTx(context.Background(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		if err := tx.Model(&ControlOperation{}).Where("operation_id = ?", operationID).Update("status", ControlOperationCompleted).Error; err != nil {
			return err
		}
		_, created, err := EnqueueOutboxEffectTx(tx, effect)
		if err != nil {
			return err
		}
		if !created {
			return errors.New("expected rollback outbox effect to be created")
		}
		return forcedRollback
	})
	require.ErrorIs(t, err, forcedRollback)

	var operation ControlOperation
	require.NoError(t, database.GormDB.First(&operation, "operation_id = ?", operationID).Error)
	require.Equal(t, ControlOperationPending, operation.Status)
	var effectCount int64
	require.NoError(t, database.GormDB.Model(&OutboxEffect{}).Where("operation_id = ?", operationID).Count(&effectCount).Error)
	require.Zero(t, effectCount)

	err = database.WithAuthoritativeWriteTx(context.Background(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		if err := tx.Model(&ControlOperation{}).Where("operation_id = ?", operationID).Update("status", ControlOperationCompleted).Error; err != nil {
			return err
		}
		_, created, err := EnqueueOutboxEffectTx(tx, effect)
		if err != nil {
			return err
		}
		if !created {
			return errors.New("expected outbox effect to be created")
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, database.GormDB.First(&operation, "operation_id = ?", operationID).Error)
	require.Equal(t, ControlOperationCompleted, operation.Status)
	require.NoError(t, database.GormDB.Model(&OutboxEffect{}).Where("operation_id = ?", operationID).Count(&effectCount).Error)
	require.Equal(t, int64(1), effectCount)
}

func TestPostgresOutboxEffectDuplicateJSONBEnqueueIsIdempotent(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	createOutboxTestOperation(t, database, "operation-postgres-jsonb")

	payload := []byte(`{"z":1,"a":2}`)
	callerFuture := time.Now().UTC().Add(24 * time.Hour)
	effect := NewOutboxEffect("operation-postgres-jsonb", "workflow_dispatch", "job-jsonb", payload, testControlPlaneWriterEpoch, callerFuture)
	receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.WithinDuration(t, time.Now().UTC(), receipt.CreatedAt, 5*time.Second)
	require.WithinDuration(t, receipt.CreatedAt, receipt.UpdatedAt, time.Millisecond)

	duplicate := NewOutboxEffect("operation-postgres-jsonb", "workflow_dispatch", "job-jsonb", payload, testControlPlaneWriterEpoch, time.Now().UTC())
	duplicateReceipt, created, err := database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err, "stored payload was %s", string(receipt.Payload))
	require.False(t, created)
	require.Equal(t, receipt.ID, duplicateReceipt.ID)

	delivery := testGithubWebhookDelivery("postgres-database-clock", "payload")
	delivery.ReceivedAt = callerFuture
	delivery.UpdatedAt = callerFuture
	deliveryReceipt, created, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.WithinDuration(t, time.Now().UTC(), deliveryReceipt.ReceivedAt, 5*time.Second)
	require.WithinDuration(t, deliveryReceipt.ReceivedAt, deliveryReceipt.UpdatedAt, time.Millisecond)
}

func TestPostgresGithubWorkflowOutboxPayloadCanonicalizesAndDetectsTampering(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	const operationID = "operation-postgres-workflow-payload"
	createOutboxTestOperation(t, database, operationID)
	reversedPayload := []byte(`{ "digger_job_id" : "job-1", "operation_id" : "operation-postgres-workflow-payload" }`)
	effect := NewOutboxEffect(operationID, GithubWorkflowDispatchEffectKind, "job:"+operationID, reversedPayload, testControlPlaneWriterEpoch, time.Now().UTC())
	receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, `{"operation_id":"operation-postgres-workflow-payload","digger_job_id":"job-1"}`, string(receipt.Payload))

	canonicalPayload := []byte(`{"operation_id":"operation-postgres-workflow-payload","digger_job_id":"job-1"}`)
	duplicate := NewOutboxEffect(operationID, GithubWorkflowDispatchEffectKind, "job:"+operationID, canonicalPayload, testControlPlaneWriterEpoch, time.Now().UTC())
	duplicateReceipt, created, err := database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, receipt.ID, duplicateReceipt.ID)
	require.True(t, duplicateReceipt.ValidPayloadDigest())

	require.NoError(t, database.GormDB.Model(&OutboxEffect{}).Where("id = ?", receipt.ID).Update("payload", []byte(`{"operation_id":"tampered","digger_job_id":"job-1"}`)).Error)
	var tampered OutboxEffect
	require.NoError(t, database.GormDB.First(&tampered, "id = ?", receipt.ID).Error)
	require.False(t, tampered.ValidPayloadDigest())
}

func TestPostgresGithubWorkflowReconciliationPayloadCanonicalizesAndDetectsTampering(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	const operationID = "operation-postgres-reconciliation-payload"
	createOutboxTestOperation(t, database, operationID)
	dispatchEffectID := uuid.New()
	reversedPayload := []byte(fmt.Sprintf(`{ "dispatch_effect_id" : %q, "operation_id" : %q }`, dispatchEffectID, operationID))
	effect := NewOutboxEffect(operationID, GithubWorkflowReconcileEffectKind, "run:12345:1001", reversedPayload, testControlPlaneWriterEpoch, time.Now().UTC())
	receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)

	var stored OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", receipt.ID).Error)
	require.True(t, stored.ValidPayloadDigest(), "PostgreSQL returned payload %q with digest %s", stored.Payload, stored.PayloadSHA256)

	canonicalPayload := []byte(fmt.Sprintf(`{"operation_id":%q,"dispatch_effect_id":%q}`, operationID, dispatchEffectID))
	duplicate := NewOutboxEffect(operationID, GithubWorkflowReconcileEffectKind, "run:12345:1001", canonicalPayload, testControlPlaneWriterEpoch, time.Now().UTC())
	duplicateReceipt, created, err := database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, receipt.ID, duplicateReceipt.ID)
	require.True(t, duplicateReceipt.ValidPayloadDigest())

	unknownFieldPayload := []byte(fmt.Sprintf(`{"operation_id":%q,"dispatch_effect_id":%q,"unexpected":true}`, operationID, dispatchEffectID))
	unknownField := NewOutboxEffect(operationID, GithubWorkflowReconcileEffectKind, "run:12345:1002", unknownFieldPayload, testControlPlaneWriterEpoch, time.Now().UTC())
	_, created, err = database.EnqueueOutboxEffect(context.Background(), unknownField, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrOutboxEffectPayload)
	require.False(t, created)

	require.NoError(t, database.GormDB.Model(&OutboxEffect{}).Where("id = ?", receipt.ID).Update("payload", []byte(fmt.Sprintf(`{"operation_id":%q,"dispatch_effect_id":%q}`, "tampered-operation", dispatchEffectID))).Error)
	require.NoError(t, database.GormDB.First(&stored, "id = ?", receipt.ID).Error)
	require.False(t, stored.ValidPayloadDigest())
}

func TestPostgresDatabaseTransactionNowAdvancesWithinTransaction(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	tx := database.GormDB.Begin()
	require.NoError(t, tx.Error)
	t.Cleanup(func() { tx.Rollback() })
	first, err := databaseTransactionNow(tx, time.Time{})
	require.NoError(t, err)
	require.NoError(t, tx.Exec("SELECT pg_sleep(0.1)").Error)
	second, err := databaseTransactionNow(tx, time.Time{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, second.Sub(first), 90*time.Millisecond)
}

func TestPostgresOutboxEffectClaimsAreUniqueUnderConcurrency(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	const effectCount = 40
	for effectIndex := 0; effectIndex < effectCount; effectIndex++ {
		operationID := fmt.Sprintf("operation-postgres-claim-%02d", effectIndex)
		createOutboxTestOperation(t, database, operationID)
		effect := NewOutboxEffect(operationID, "workflow_dispatch", fmt.Sprintf("job-%02d", effectIndex), []byte(fmt.Sprintf(`{"job":%d}`, effectIndex)), testControlPlaneWriterEpoch, time.Now().UTC())
		_, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.True(t, created)
	}

	start := make(chan struct{})
	var workers sync.WaitGroup
	claimedCounts := make(map[uuid.UUID]int)
	var claimedMu sync.Mutex
	claimErrors := make(chan error, 8)
	for workerIndex := 0; workerIndex < 8; workerIndex++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for {
				claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), fmt.Sprintf("worker-%d", worker), time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
				if err != nil {
					claimErrors <- err
					return
				}
				if claimed == nil {
					return
				}
				claimedMu.Lock()
				claimedCounts[claimed.ID]++
				claimedMu.Unlock()
			}
		}(workerIndex)
	}
	close(start)
	workers.Wait()
	close(claimErrors)
	for err := range claimErrors {
		require.NoError(t, err)
	}
	require.Len(t, claimedCounts, effectCount)
	for effectID, count := range claimedCounts {
		require.Equal(t, 1, count, effectID)
	}
}

func TestOutboxEffectReplayIsIdempotentAcrossWriterEpochs(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		database := newOutboxTestDatabase(t)
		createOutboxTestOperation(t, database, "operation-epoch-pending")
		payload := []byte(`{"job":"pending"}`)
		effect := NewOutboxEffect("operation-epoch-pending", "workflow_dispatch", "job-pending", payload, testControlPlaneWriterEpoch, time.Now().UTC())
		receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.True(t, created)

		require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("writer_epoch", testControlPlaneWriterEpoch+1).Error)
		duplicate := NewOutboxEffect("operation-epoch-pending", "workflow_dispatch", "job-pending", payload, testControlPlaneWriterEpoch+1, time.Now().UTC())
		duplicateReceipt, created, err := database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
		require.NoError(t, err)
		require.False(t, created)
		require.Equal(t, receipt.ID, duplicateReceipt.ID)
	})

	t.Run("completed", func(t *testing.T) {
		database := newOutboxTestDatabase(t)
		createOutboxTestOperation(t, database, "operation-epoch-completed")
		payload := []byte(`{"job":"completed"}`)
		effect := NewOutboxEffect("operation-epoch-completed", "workflow_dispatch", "job-completed", payload, testControlPlaneWriterEpoch, time.Now().UTC())
		receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.True(t, created)

		claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "completed-lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.Equal(t, receipt.ID, claimed.ID)
		require.NoError(t, database.CompleteOutboxEffect(context.Background(), receipt.ID, "completed-lease", []byte(`{"run":1}`), time.Now().UTC(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch))

		require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("writer_epoch", testControlPlaneWriterEpoch+1).Error)
		duplicate := NewOutboxEffect("operation-epoch-completed", "workflow_dispatch", "job-completed", payload, testControlPlaneWriterEpoch+1, time.Now().UTC())
		duplicateReceipt, created, err := database.EnqueueOutboxEffect(context.Background(), duplicate, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
		require.NoError(t, err)
		require.False(t, created)
		require.Equal(t, receipt.ID, duplicateReceipt.ID)
		require.Equal(t, OutboxEffectSucceeded, duplicateReceipt.Status)
	})
}

func TestOutboxEffectLeaseIsEpochBoundAndRecoverable(t *testing.T) {
	database := newOutboxTestDatabase(t)
	createOutboxTestOperation(t, database, "operation-2")
	now := time.Now().UTC()
	effect := NewOutboxEffect("operation-2", "workflow_dispatch", "job-2", []byte(`{"job":"two"}`), testControlPlaneWriterEpoch, now)
	_, _, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)

	claimed, err := database.ClaimNextOutboxEffect(context.Background(), now, "lease-1", time.Second, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, int64(1), claimed.AttemptCount)

	require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("writer_epoch", testControlPlaneWriterEpoch+1).Error)
	err = database.CompleteOutboxEffect(context.Background(), effect.ID, "lease-1", []byte(`{"run":1}`), now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrControlPlaneFenced)

	require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Updates(map[string]any{"writer_epoch": testControlPlaneWriterEpoch + 1, "mode": ControlPlaneModeNormal}).Error)
	recovered, err := database.ClaimNextOutboxEffect(context.Background(), now.Add(2*time.Second), "lease-2", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, int64(2), recovered.AttemptCount)
	require.Equal(t, testControlPlaneWriterEpoch+1, recovered.WriterEpoch)

	err = database.CompleteOutboxEffect(context.Background(), effect.ID, "lease-1", nil, now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound))
	require.NoError(t, database.CompleteOutboxEffect(context.Background(), effect.ID, "lease-2", []byte(`{"run":2}`), now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1))
}

func TestOutboxHoldDoesNotClaimEffects(t *testing.T) {
	database := newOutboxTestDatabase(t)
	createOutboxTestOperation(t, database, "operation-3")
	effect := NewOutboxEffect("operation-3", "workflow_dispatch", "job-3", []byte(`{"job":"three"}`), testControlPlaneWriterEpoch, time.Now().UTC())
	_, _, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("mode", ControlPlaneModeHold).Error)

	claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrControlPlaneHold)
	require.Nil(t, claimed)
}

func TestOutboxEffectDeadLetterIsLeaseAndEpochBound(t *testing.T) {
	t.Run("lease", func(t *testing.T) {
		database := newOutboxTestDatabase(t)
		createOutboxTestOperation(t, database, "operation-dead-letter-lease")
		effect := NewOutboxEffect("operation-dead-letter-lease", "workflow_dispatch", "job-dead-letter-lease", []byte(`{"job":"lease"}`), testControlPlaneWriterEpoch, time.Now().UTC())
		_, _, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "dead-letter-lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.Equal(t, effect.ID, claimed.ID)

		err = database.DeadLetterOutboxEffect(context.Background(), effect.ID, "wrong-lease", "poison effect", time.Now().UTC(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
		require.NoError(t, database.DeadLetterOutboxEffect(context.Background(), effect.ID, "dead-letter-lease", "poison effect", time.Now().UTC(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch))

		var stored OutboxEffect
		require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
		require.Equal(t, OutboxEffectDeadLetter, stored.Status)
		require.Equal(t, "poison effect", stored.LastError)
		require.Empty(t, stored.LeaseID)
		require.Nil(t, stored.LeaseExpiresAt)
		require.Nil(t, stored.NextAttemptAt)
	})

	t.Run("epoch", func(t *testing.T) {
		database := newOutboxTestDatabase(t)
		createOutboxTestOperation(t, database, "operation-dead-letter-epoch")
		effect := NewOutboxEffect("operation-dead-letter-epoch", "workflow_dispatch", "job-dead-letter-epoch", []byte(`{"job":"epoch"}`), testControlPlaneWriterEpoch, time.Now().UTC())
		_, _, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		claimed, err := database.ClaimNextOutboxEffect(context.Background(), time.Now().UTC(), "stale-lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, err)
		require.Equal(t, effect.ID, claimed.ID)

		require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("writer_epoch", testControlPlaneWriterEpoch+1).Error)
		err = database.DeadLetterOutboxEffect(context.Background(), effect.ID, "stale-lease", "stale worker", time.Now().UTC(), testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.ErrorIs(t, err, ErrControlPlaneFenced)

		var stored OutboxEffect
		require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
		require.Equal(t, OutboxEffectProcessing, stored.Status)
		require.Equal(t, "stale-lease", stored.LeaseID)
	})
}
