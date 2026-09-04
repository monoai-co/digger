package controllers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	githubWebhookTestDatabaseIdentity = "test-database"
	githubWebhookTestWriterEpoch      = int64(1)
)

type blockingGithubWebhookStore struct {
	*models.Database
	started chan struct{}
}

func (s *blockingGithubWebhookStore) RecordGithubWebhookDelivery(ctx context.Context, _ *models.GithubWebhookDelivery, _ string, _ int64) (*models.GithubWebhookDelivery, bool, error) {
	close(s.started)
	<-ctx.Done()
	return nil, false, ctx.Err()
}

type leaseLossGithubWebhookStore struct {
	*models.Database
}

func (s *leaseLossGithubWebhookStore) RenewGithubWebhookDeliveryLease(context.Context, string, string, time.Time, time.Duration, string, int64) error {
	return gorm.ErrRecordNotFound
}

type panicOnceGithubWebhookStore struct {
	*models.Database
	panicked atomic.Bool
}

func (s *panicOnceGithubWebhookStore) ClaimNextGithubWebhookDelivery(ctx context.Context, now time.Time, leaseID string, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) (*models.GithubWebhookDelivery, error) {
	if s.panicked.CompareAndSwap(false, true) {
		panic("claim panic")
	}
	return s.Database.ClaimNextGithubWebhookDelivery(ctx, now, leaseID, leaseDuration, databaseIdentity, writerEpoch)
}

type githubWebhookTestProvider struct {
	secret string
}

func (p githubWebhookTestProvider) NewClient(client *http.Client) (*github.Client, error) {
	return github.NewClient(client), nil
}

func (p githubWebhookTestProvider) Get(_ int64, _ int64) (*github.Client, *string, error) {
	token := "token"
	return github.NewClient(nil), &token, nil
}

func (p githubWebhookTestProvider) FetchCredentials(_ string) (string, string, string, string, error) {
	return "client", "client-secret", p.secret, "private-key", nil
}

func newGithubWebhookProcessorTestDatabase(t *testing.T) *models.Database {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "github-webhook.sqlite") + "?_busy_timeout=5000&_journal_mode=WAL"
	gormDB, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := gormDB.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, gormDB.AutoMigrate(&models.ControlPlaneFence{}, &models.GithubWebhookOrderingDomain{}, &models.GithubWebhookDelivery{}, &models.GithubWebhookDeliveryRequeue{}, &models.ControlOperation{}, &models.OutboxEffect{}))
	require.NoError(t, gormDB.Create(&models.ControlPlaneFence{ID: models.ControlPlaneFenceSingletonID, DatabaseIdentity: githubWebhookTestDatabaseIdentity, WriterEpoch: githubWebhookTestWriterEpoch, Mode: models.ControlPlaneModeNormal, ProtocolFloor: 1, UpdatedAt: time.Now().UTC()}).Error)
	return &models.Database{GormDB: gormDB}
}

func newGithubWebhookProcessorTestDelivery(deliveryID string) *models.GithubWebhookDelivery {
	now := time.Now().UTC()
	return &models.GithubWebhookDelivery{
		DeliveryID:       deliveryID,
		PayloadSHA256:    "payload-sha256",
		Payload:          []byte(`{"zen":"test"}`),
		EventType:        "ping",
		GithubAppID:      123,
		ProcessingStatus: models.GithubWebhookDeliveryPending,
		ReceivedAt:       now,
		UpdatedAt:        now,
	}
}

func testGithubWebhookProcessorConfig() GithubWebhookProcessorConfig {
	return GithubWebhookProcessorConfig{
		Enabled:          true,
		DatabaseIdentity: githubWebhookTestDatabaseIdentity,
		WriterEpoch:      githubWebhookTestWriterEpoch,
		Workers:          1,
		PollInterval:     2 * time.Millisecond,
		LeaseDuration:    90 * time.Millisecond,
		MaxAttempts:      3,
		RetryBase:        2 * time.Millisecond,
		RetryMax:         5 * time.Millisecond,
		RetryHorizon:     time.Nanosecond,
	}
}

func waitForGithubWebhookStatus(t *testing.T, database *models.Database, deliveryID string, status models.GithubWebhookDeliveryStatus) models.GithubWebhookDelivery {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var delivery models.GithubWebhookDelivery
		err := database.GormDB.First(&delivery, "delivery_id = ?", deliveryID).Error
		if err == nil && delivery.ProcessingStatus == status {
			return delivery
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("delivery %s did not reach status %s", deliveryID, status)
	return models.GithubWebhookDelivery{}
}

func TestGithubWebhookProcessorProcessesDuplicateReceiptOnce(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	var effects atomic.Int32
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		effects.Add(1)
		return ignoredGithubWebhookResult("test"), nil
	}, testGithubWebhookProcessorConfig())

	delivery := newGithubWebhookProcessorTestDelivery("delivery-once")
	_, created, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	require.True(t, created)
	_, created, err = processor.Admit(context.Background(), newGithubWebhookProcessorTestDelivery("delivery-once"))
	require.NoError(t, err)
	require.False(t, created)

	processor.Start()
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliveryIgnored)
	require.Equal(t, int32(1), effects.Load())
	require.Equal(t, int64(1), stored.AttemptCount)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookProcessorRetriesThenSucceeds(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	var attempts atomic.Int32
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		attempt := attempts.Add(1)
		if attempt < 3 {
			return GithubWebhookProcessingResult{}, errors.New("retryable test failure")
		}
		return succeededGithubWebhookResult("processed"), nil
	}, testGithubWebhookProcessorConfig())
	processor.Start()

	delivery := newGithubWebhookProcessorTestDelivery("delivery-retry")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliverySucceeded)
	require.Equal(t, int32(3), attempts.Load())
	require.Equal(t, int64(3), stored.AttemptCount)
	require.Empty(t, stored.LastError)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookProcessorDeadLettersPoisonDelivery(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return GithubWebhookProcessingResult{}, errors.New("poison")
	}, testGithubWebhookProcessorConfig())
	processor.Start()

	delivery := newGithubWebhookProcessorTestDelivery("delivery-poison")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliveryDeadLetter)
	require.Equal(t, int64(3), stored.AttemptCount)
	require.Equal(t, "poison", stored.LastError)
	require.NotNil(t, stored.DeadLetteredAt)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookProcessorPanicNeverCommitsSuccess(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		panic("test panic")
	}, testGithubWebhookProcessorConfig())
	processor.Start()

	delivery := newGithubWebhookProcessorTestDelivery("delivery-panic")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliveryDeadLetter)
	require.Contains(t, stored.LastError, "panic while processing webhook")
	require.NotEqual(t, models.GithubWebhookDeliverySucceeded, stored.ProcessingStatus)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookHandlerPanicPropagatesAsProcessingError(t *testing.T) {
	controller := DiggerController{}
	delivery := newGithubWebhookProcessorTestDelivery("delivery-handler-panic")
	delivery.EventType = "push"
	delivery.Payload = []byte(`{"ref":"refs/heads/main","repository":{},"installation":{}}`)
	result, err := controller.ProcessGithubWebhookDelivery(context.Background(), delivery)
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic in handlePushEvent")
	require.Empty(t, result.Status)
}

func TestGithubWebhookMalformedStoredPayloadNeverReturnsTerminalSuccess(t *testing.T) {
	controller := DiggerController{}
	delivery := newGithubWebhookProcessorTestDelivery("delivery-malformed")
	delivery.Payload = []byte(`{"unterminated"`)
	result, err := controller.ProcessGithubWebhookDelivery(context.Background(), delivery)
	require.Error(t, err)
	require.Empty(t, result.Status)
}

func TestGithubWebhookProcessorKeepsTransientFailureRetryableThroughOutage(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	config := testGithubWebhookProcessorConfig()
	config.MaxAttempts = 1
	config.RetryHorizon = time.Hour
	config.RetryBase = time.Hour
	config.RetryMax = time.Hour
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return GithubWebhookProcessingResult{}, errors.New("database outage")
	}, config)
	processor.Start()

	delivery := newGithubWebhookProcessorTestDelivery("delivery-outage")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliveryRetrying)
	require.Equal(t, int64(1), stored.AttemptCount)
	require.Nil(t, stored.DeadLetteredAt)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookProcessorAuditsDeadLetterReplay(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return GithubWebhookProcessingResult{}, errors.New("poison")
	}, testGithubWebhookProcessorConfig())
	processor.Start()
	delivery := newGithubWebhookProcessorTestDelivery("delivery-requeue")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliveryDeadLetter)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, processor.Shutdown(shutdownCtx))
	cancel()

	operatorProcessor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return succeededGithubWebhookResult("replayed"), nil
	}, testGithubWebhookProcessorConfig())
	require.NoError(t, operatorProcessor.RequeueDeadLetter(context.Background(), delivery.DeliveryID, "sre@example.com", "dependency recovered"))
	var stored models.GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", delivery.DeliveryID).Error)
	require.Equal(t, models.GithubWebhookDeliveryPending, stored.ProcessingStatus)
	require.Nil(t, stored.DeadLetteredAt)
	var audit models.GithubWebhookDeliveryRequeue
	require.NoError(t, database.GormDB.First(&audit, "delivery_id = ?", delivery.DeliveryID).Error)
	require.NotEqual(t, uuid.Nil, audit.ID)
	require.Equal(t, "sre@example.com", audit.Actor)
	require.Equal(t, "dependency recovered", audit.Reason)
}

func TestGithubWebhookOperatorRequeueEndpointRequiresIdentityAndAuditsReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := newGithubWebhookProcessorTestDatabase(t)
	delivery := newGithubWebhookProcessorTestDelivery("delivery-operator-endpoint")
	_, _, err := database.RecordGithubWebhookDelivery(context.Background(), delivery, githubWebhookTestDatabaseIdentity, githubWebhookTestWriterEpoch)
	require.NoError(t, err)
	claimed, err := database.ClaimNextGithubWebhookDelivery(context.Background(), time.Now().UTC(), "operator-test-lease", time.Minute, githubWebhookTestDatabaseIdentity, githubWebhookTestWriterEpoch)
	require.NoError(t, err)
	require.NoError(t, database.DeadLetterGithubWebhookDelivery(context.Background(), claimed.DeliveryID, claimed.LeaseID, "poison", time.Now().UTC(), githubWebhookTestDatabaseIdentity, githubWebhookTestWriterEpoch))
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("unused"), nil
	}, testGithubWebhookProcessorConfig())
	controller := DiggerController{GithubWebhookProcessor: processor}

	unauthorizedRecorder := httptest.NewRecorder()
	unauthorizedContext, _ := gin.CreateTestContext(unauthorizedRecorder)
	unauthorizedContext.Params = gin.Params{{Key: "deliveryID", Value: delivery.DeliveryID}}
	unauthorizedContext.Request = httptest.NewRequest(http.MethodPost, "/admin/github-webhooks/"+delivery.DeliveryID+"/requeue", bytes.NewBufferString(`{"reason":"dependency recovered"}`))
	controller.RequeueGithubWebhookDelivery(unauthorizedContext)
	require.Equal(t, http.StatusForbidden, unauthorizedRecorder.Code)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "deliveryID", Value: delivery.DeliveryID}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/admin/github-webhooks/"+delivery.DeliveryID+"/requeue", bytes.NewBufferString(`{"reason":"dependency recovered"}`))
	ctx.Set(middleware.USER_ID_KEY, "operator-123")
	controller.RequeueGithubWebhookDelivery(ctx)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	var audit models.GithubWebhookDeliveryRequeue
	require.NoError(t, database.GormDB.First(&audit, "delivery_id = ?", delivery.DeliveryID).Error)
	require.Equal(t, "user:operator-123", audit.Actor)
}

func TestGithubWebhookProcessorAdmissionCancellationDoesNotBlockStop(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	store := &blockingGithubWebhookStore{Database: database, started: make(chan struct{})}
	processor := NewGithubWebhookProcessor(store, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("unused"), nil
	}, testGithubWebhookProcessorConfig())
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	admitDone := make(chan error, 1)
	go func() {
		_, _, err := processor.Admit(requestCtx, newGithubWebhookProcessorTestDelivery("delivery-blocked-admit"))
		admitDone <- err
	}()
	<-store.started
	drained := processor.StopAdmission()
	select {
	case <-drained:
		t.Fatal("blocked admission reported drained before request cancellation")
	default:
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, processor.Shutdown(shutdownCtx), context.DeadlineExceeded)
	cancelShutdown()
	cancelRequest()
	require.ErrorIs(t, <-admitDone, context.Canceled)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("admission did not drain after request cancellation")
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	defer cancelCleanup()
	require.NoError(t, processor.Shutdown(cleanupCtx))
}

func TestGithubWebhookProcessorCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	store := &leaseLossGithubWebhookStore{Database: database}
	handlerCancelled := make(chan struct{})
	config := testGithubWebhookProcessorConfig()
	config.LeaseDuration = 30 * time.Millisecond
	processor := NewGithubWebhookProcessor(store, func(ctx context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		<-ctx.Done()
		close(handlerCancelled)
		return succeededGithubWebhookResult("must_not_commit"), nil
	}, config)
	processor.Start()
	delivery := newGithubWebhookProcessorTestDelivery("delivery-lease-loss")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	select {
	case <-handlerCancelled:
	case <-time.After(time.Second):
		t.Fatal("handler was not cancelled when its lease was lost")
	}
	var stored models.GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", delivery.DeliveryID).Error)
	require.NotEqual(t, models.GithubWebhookDeliverySucceeded, stored.ProcessingStatus)
	require.NotEqual(t, "must_not_commit", stored.TerminalResult)
	processor.StopAdmission()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookProcessorSupervisesWorkersAndReportsReadiness(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	store := &panicOnceGithubWebhookStore{Database: database}
	processor := NewGithubWebhookProcessor(store, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("unused"), nil
	}, testGithubWebhookProcessorConfig())
	require.Error(t, processor.Ready(context.Background()))
	processor.Start()
	require.Eventually(t, func() bool {
		return processor.workerRestarts.Load() == 1 && processor.Ready(context.Background()) == nil
	}, time.Second, 5*time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
	require.ErrorIs(t, processor.Ready(context.Background()), ErrGithubWebhookProcessorStopping)
}

func TestGithubWebhookProcessorShutdownDrainsClaimedDelivery(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	processingStarted := make(chan struct{})
	releaseProcessing := make(chan struct{})
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		close(processingStarted)
		<-releaseProcessing
		return succeededGithubWebhookResult("processed_during_drain"), nil
	}, testGithubWebhookProcessorConfig())
	processor.Start()

	delivery := newGithubWebhookProcessorTestDelivery("delivery-drain")
	_, _, err := processor.Admit(context.Background(), delivery)
	require.NoError(t, err)
	select {
	case <-processingStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not claim delivery")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- processor.Shutdown(shutdownCtx)
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight processing drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseProcessing)
	require.NoError(t, <-shutdownDone)
	stored := waitForGithubWebhookStatus(t, database, delivery.DeliveryID, models.GithubWebhookDeliverySucceeded)
	require.Equal(t, "processed_during_drain", stored.TerminalResult)
	_, _, err = processor.Admit(context.Background(), newGithubWebhookProcessorTestDelivery("delivery-after-drain"))
	require.ErrorIs(t, err, ErrGithubWebhookProcessorStopping)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Where("delivery_id = ?", "delivery-after-drain").Count(&count).Error)
	require.Zero(t, count)
}

func TestGithubWebhookHandlerCommitsBeforeAcknowledgingAndRejectsConflicts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("ping"), nil
	}, testGithubWebhookProcessorConfig())
	controller := DiggerController{
		GithubClientProvider:   githubWebhookTestProvider{secret: "webhook-secret"},
		GithubWebhookProcessor: processor,
	}

	firstResponse := deliverGithubWebhookForTest(t, controller, "delivery-handler", []byte(`{"zen":"first"}`))
	require.Equal(t, http.StatusAccepted, firstResponse.Code)
	var firstReceipt map[string]any
	require.NoError(t, json.Unmarshal(firstResponse.Body.Bytes(), &firstReceipt))
	require.Equal(t, false, firstReceipt["duplicate"])

	var stored models.GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", "delivery-handler").Error)
	require.Equal(t, []byte(`{"zen":"first"}`), stored.Payload)
	require.False(t, database.GormDB.Migrator().HasColumn(&models.GithubWebhookDelivery{}, "signature"))

	duplicateResponse := deliverGithubWebhookForTest(t, controller, "delivery-handler", []byte(`{"zen":"first"}`))
	require.Equal(t, http.StatusAccepted, duplicateResponse.Code)
	var duplicateReceipt map[string]any
	require.NoError(t, json.Unmarshal(duplicateResponse.Body.Bytes(), &duplicateReceipt))
	require.Equal(t, true, duplicateReceipt["duplicate"])

	conflictResponse := deliverGithubWebhookForTest(t, controller, "delivery-handler", []byte(`{"zen":"changed"}`))
	require.Equal(t, http.StatusConflict, conflictResponse.Code)
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", "delivery-handler").Error)
	require.Equal(t, []byte(`{"zen":"first"}`), stored.Payload)

	processor.Start()
	waitForGithubWebhookStatus(t, database, "delivery-handler", models.GithubWebhookDeliveryIgnored)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func TestGithubWebhookDisabledModePreservesLegacyHeadersAndResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("unused"), nil
	}, DefaultGithubWebhookProcessorConfig())
	controller := DiggerController{
		GithubClientProvider:   githubWebhookTestProvider{secret: "webhook-secret"},
		GithubWebhookProcessor: processor,
	}
	payload := []byte(`{"zen":"legacy"}`)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/github/webhook", bytesReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "ping")
	request.Header.Set("X-GitHub-Hook-Installation-Target-ID", "123")
	request.Header.Set("X-Hub-Signature-256", githubWebhookSignature("webhook-secret", payload))
	ctx.Request = request

	controller.GithubAppWebHook(ctx)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.JSONEq(t, `"ok"`, recorder.Body.String())
}

func TestGithubWebhookHandlerBoundsPayloadAndIdentityHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := newGithubWebhookProcessorTestDatabase(t)
	processor := NewGithubWebhookProcessor(database, func(_ context.Context, _ *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		return ignoredGithubWebhookResult("ping"), nil
	}, testGithubWebhookProcessorConfig())
	controller := DiggerController{GithubClientProvider: githubWebhookTestProvider{secret: "webhook-secret"}, GithubWebhookProcessor: processor}

	largePayload := bytes.Repeat([]byte("a"), maxGithubWebhookBodyBytes+1)
	response := deliverGithubWebhookForTest(t, controller, "delivery-large", largePayload)
	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)

	response = deliverGithubWebhookForTest(t, controller, string(bytes.Repeat([]byte("d"), maxGithubDeliveryHeaderBytes+1)), []byte(`{"zen":"test"}`))
	require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, response.Code)
	var count int64
	require.NoError(t, database.GormDB.Model(&models.GithubWebhookDelivery{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestGithubWebhookHandlerDurablyAcceptsValidUnknownEventBeforeParsing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := newGithubWebhookProcessorTestDatabase(t)
	controller := DiggerController{GithubClientProvider: githubWebhookTestProvider{secret: "webhook-secret"}}
	processor := NewGithubWebhookProcessor(database, controller.ProcessGithubWebhookDelivery, testGithubWebhookProcessorConfig())
	controller.GithubWebhookProcessor = processor

	response := deliverGithubWebhookEventForTest(t, controller, "delivery-future-event", "future_event", []byte(`{"future":"payload"}`))
	require.Equal(t, http.StatusAccepted, response.Code)

	var stored models.GithubWebhookDelivery
	require.NoError(t, database.GormDB.First(&stored, "delivery_id = ?", "delivery-future-event").Error)
	require.Equal(t, "future_event", stored.EventType)
	require.Equal(t, []byte(`{"future":"payload"}`), stored.Payload)

	processor.Start()
	stored = waitForGithubWebhookStatus(t, database, stored.DeliveryID, models.GithubWebhookDeliveryIgnored)
	require.Equal(t, "event_type_ignored", stored.TerminalResult)
	require.Equal(t, int64(1), stored.AttemptCount)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(shutdownCtx))
}

func deliverGithubWebhookForTest(t *testing.T, controller DiggerController, deliveryID string, payload []byte) *httptest.ResponseRecorder {
	return deliverGithubWebhookEventForTest(t, controller, deliveryID, "ping", payload)
}

func deliverGithubWebhookEventForTest(t *testing.T, controller DiggerController, deliveryID string, eventType string, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/github/webhook", bytesReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Delivery", deliveryID)
	request.Header.Set("X-GitHub-Event", eventType)
	request.Header.Set("X-GitHub-Hook-Installation-Target-ID", "123")
	request.Header.Set("X-GitHub-Hook-Installation-Target-Type", "integration")
	request.Header.Set("X-Hub-Signature-256", githubWebhookSignature("webhook-secret", payload))
	ctx.Request = request
	controller.GithubAppWebHook(ctx)
	return recorder
}

func bytesReader(payload []byte) *bytes.Reader {
	return bytes.NewReader(payload)
}

func githubWebhookSignature(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

var _ utils.GithubClientProvider = githubWebhookTestProvider{}
