package models

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	testControlPlaneDatabaseIdentity = "test-database"
	testControlPlaneWriterEpoch      = int64(1)
)

func newGithubWebhookDeliveryTestDatabase(t *testing.T) *Database {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "github-webhook.sqlite") + "?_busy_timeout=5000&_journal_mode=WAL"
	gormDB, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, gormDB.AutoMigrate(&ControlPlaneFence{}, &GithubWebhookOrderingDomain{}, &GithubWebhookDelivery{}, &GithubWebhookDeliveryRequeue{}, &ControlOperation{}))
	require.NoError(t, gormDB.Create(&ControlPlaneFence{ID: ControlPlaneFenceSingletonID, DatabaseIdentity: testControlPlaneDatabaseIdentity, WriterEpoch: testControlPlaneWriterEpoch, Mode: ControlPlaneModeNormal, ProtocolFloor: 1, UpdatedAt: time.Now().UTC()}).Error)
	return &Database{GormDB: gormDB}
}

func TestPostgresGithubWebhookClaimsAreUniqueUnderConcurrency(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS job_status_callbacks").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS execution_claim_attempts").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS outbox_effects").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS control_operations").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_delivery_requeues").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_deliveries").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_ordering_domains").Error)
	require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS control_plane_fence").Error)
	t.Cleanup(func() {
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS job_status_callbacks").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS execution_claim_attempts").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS outbox_effects").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS control_operations").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_delivery_requeues").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_deliveries").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS github_webhook_ordering_domains").Error)
		require.NoError(t, gormDB.Exec("DROP TABLE IF EXISTS control_plane_fence").Error)
	})
	require.NoError(t, gormDB.AutoMigrate(&ControlPlaneFence{}, &GithubWebhookOrderingDomain{}, &GithubWebhookDelivery{}, &GithubWebhookDeliveryRequeue{}, &ControlOperation{}))
	require.NoError(t, gormDB.Create(&ControlPlaneFence{ID: ControlPlaneFenceSingletonID, DatabaseIdentity: testControlPlaneDatabaseIdentity, WriterEpoch: testControlPlaneWriterEpoch, Mode: ControlPlaneModeNormal, ProtocolFloor: 1, UpdatedAt: time.Now().UTC()}).Error)
	database := &Database{GormDB: gormDB}

	const deliveryCount = 40
	for deliveryIndex := 0; deliveryIndex < deliveryCount; deliveryIndex++ {
		delivery := testGithubWebhookDelivery(fmt.Sprintf("postgres-delivery-%02d", deliveryIndex), fmt.Sprintf("payload-%02d", deliveryIndex))
		installationID := int64(deliveryIndex + 1)
		delivery.InstallationID = &installationID
		_, created, recordErr := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.NoError(t, recordErr)
		require.True(t, created)
	}

	start := make(chan struct{})
	var workers sync.WaitGroup
	claimedCounts := make(map[string]int)
	var claimedMu sync.Mutex
	claimErrors := make(chan error, 8)
	for workerIndex := 0; workerIndex < 8; workerIndex++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for {
				claimed, claimErr := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), fmt.Sprintf("worker-%d", worker), time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
				if claimErr != nil {
					claimErrors <- claimErr
					return
				}
				if claimed == nil {
					return
				}
				claimedMu.Lock()
				claimedCounts[claimed.DeliveryID]++
				claimedMu.Unlock()
			}
		}(workerIndex)
	}
	close(start)
	workers.Wait()
	close(claimErrors)
	for claimErr := range claimErrors {
		require.NoError(t, claimErr)
	}
	require.Len(t, claimedCounts, deliveryCount)
	for deliveryID, count := range claimedCounts {
		require.Equal(t, 1, count, deliveryID)
	}
}

func testGithubWebhookDelivery(deliveryID string, payload string) *GithubWebhookDelivery {
	now := time.Now().UTC()
	return &GithubWebhookDelivery{
		DeliveryID:                 deliveryID,
		PayloadSHA256:              payload + "-sha256",
		Payload:                    []byte(payload),
		EventType:                  "ping",
		GithubAppID:                123,
		HookID:                     "456",
		HookInstallationTargetType: "integration",
		RepositoryFullName:         "monoai-co/example",
		ReceivedAt:                 now,
		ProcessingStatus:           GithubWebhookDeliveryPending,
		UpdatedAt:                  now,
	}
}

func TestRecordGithubWebhookDeliveryIsImmutableAndIdempotent(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	original := testGithubWebhookDelivery("delivery-1", "original-payload")

	receipt, created, err := database.RecordGithubWebhookDelivery(context.Background(), original, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, original.DeliveryID, receipt.DeliveryID)

	matchingRetry := testGithubWebhookDelivery("delivery-1", "original-payload")
	receipt, created, err = database.RecordGithubWebhookDelivery(context.Background(), matchingRetry, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, original.Payload, receipt.Payload)

	matchingRetryAfterSecretRotation := testGithubWebhookDelivery("delivery-1", "original-payload")
	receipt, created, err = database.RecordGithubWebhookDelivery(context.Background(), matchingRetryAfterSecretRotation, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)

	conflictingRetry := testGithubWebhookDelivery("delivery-1", "changed-payload")
	receipt, created, err = database.RecordGithubWebhookDelivery(context.Background(), conflictingRetry, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrGithubWebhookDeliveryConflict)
	require.False(t, created)
	require.Equal(t, original.Payload, receipt.Payload)

	var stored GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", original.DeliveryID).Error)
	require.Equal(t, original.Payload, stored.Payload)
	require.Equal(t, original.PayloadSHA256, stored.PayloadSHA256)
}

func TestGithubWebhookDeliveryClaimUsesLeaseCompareAndSet(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	delivery := testGithubWebhookDelivery("delivery-2", "payload")
	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)

	now := time.Now().UTC()
	claimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-1", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, int64(1), claimed.AttemptCount)
	require.Equal(t, "lease-1", claimed.LeaseID)

	notClaimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-2", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.Nil(t, notClaimed)

	err = database.CompleteGithubWebhookDelivery(context.Background(), delivery.DeliveryID, "wrong-lease", GithubWebhookDeliverySucceeded, "processed", now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound))
	require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), delivery.DeliveryID, "lease-1", GithubWebhookDeliverySucceeded, "processed", now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch))

	notClaimed, err = database.ClaimNextGithubWebhookDelivery(context.Background(), now.Add(2*time.Minute), "lease-3", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.Nil(t, notClaimed)
}

func TestGithubWebhookDeliveryExpiredLeaseIsRecoverable(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	delivery := testGithubWebhookDelivery("delivery-3", "payload")
	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)

	now := time.Now().UTC()
	claimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-1", time.Second, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	recovered, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now.Add(2*time.Second), "lease-2", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, int64(2), recovered.AttemptCount)
	require.Equal(t, "lease-2", recovered.LeaseID)
}

func TestGithubWebhookAdmissionAllocatesOneSequenceForDuplicate(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	delivery := testGithubWebhookDelivery("delivery-duplicate-sequence", "payload")

	first, created, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(1), first.OrderingSequence)

	retry := testGithubWebhookDelivery("delivery-duplicate-sequence", "payload")
	second, created, err := database.RecordGithubWebhookDelivery(context.Background(), retry, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.OrderingSequence, second.OrderingSequence)

	var domain GithubWebhookOrderingDomain
	require.NoError(t, database.GormDB.First(&domain, "ordering_domain = ?", first.OrderingDomain).Error)
	require.Equal(t, int64(2), domain.NextSequence)
}

func TestGithubWebhookClaimPreservesOrderingWithinInstallation(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	installationID := int64(987)
	first := testGithubWebhookDelivery("delivery-ordered-1", "payload-1")
	first.InstallationID = &installationID
	second := testGithubWebhookDelivery("delivery-ordered-2", "payload-2")
	second.InstallationID = &installationID
	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), first, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	_, _, err = database.RecordGithubWebhookDelivery(context.Background(), second, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)

	now := time.Now().UTC()
	claimedFirst, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-first", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.Equal(t, first.DeliveryID, claimedFirst.DeliveryID)
	blockedSecond, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-second", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.Nil(t, blockedSecond)

	require.NoError(t, database.CompleteGithubWebhookDelivery(context.Background(), first.DeliveryID, "lease-first", GithubWebhookDeliverySucceeded, "processed", now, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch))
	claimedSecond, err := database.ClaimNextGithubWebhookDelivery(context.Background(), now, "lease-second", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.Equal(t, second.DeliveryID, claimedSecond.DeliveryID)
}

func TestGithubWebhookHoldAcceptsReceiptButDoesNotClaim(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("mode", ControlPlaneModeHold).Error)
	delivery := testGithubWebhookDelivery("delivery-held", "payload")
	_, created, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)

	claimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrControlPlaneHold)
	require.Nil(t, claimed)
}

func TestGithubWebhookStaleWriterCannotAdmitOrClaim(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	delivery := testGithubWebhookDelivery("delivery-stale", "payload")
	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
	require.ErrorIs(t, err, ErrControlPlaneFenced)

	claimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "lease", time.Minute, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch+1)
	require.ErrorIs(t, err, ErrControlPlaneFenced)
	require.Nil(t, claimed)
}

func TestGithubWebhookWriterBelowProtocolFloorIsFenced(t *testing.T) {
	database := newGithubWebhookDeliveryTestDatabase(t)
	require.NoError(t, database.GormDB.Model(&ControlPlaneFence{}).Where("id = ?", ControlPlaneFenceSingletonID).Update("protocol_floor", operation.ProtocolVersion+1).Error)
	delivery := testGithubWebhookDelivery("delivery-old-protocol", "payload")

	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrControlPlaneProtocol)
}
