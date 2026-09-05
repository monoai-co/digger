package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type completionFailureOutboxStore struct {
	*models.Database
	failed atomic.Bool
}

func (s *completionFailureOutboxStore) CompleteOutboxEffect(ctx context.Context, effectID uuid.UUID, leaseID string, providerReceipt []byte, now time.Time, databaseIdentity string, writerEpoch int64) error {
	if s.failed.CompareAndSwap(false, true) {
		return errors.New("simulated completion commit failure")
	}
	return s.Database.CompleteOutboxEffect(ctx, effectID, leaseID, providerReceipt, now, databaseIdentity, writerEpoch)
}

type leaseLossOutboxStore struct {
	*models.Database
}

func (s *leaseLossOutboxStore) RenewOutboxEffectLease(context.Context, uuid.UUID, string, time.Time, time.Duration, string, int64) error {
	return gorm.ErrRecordNotFound
}

type panicRenewOutboxStore struct {
	*models.Database
	renewals atomic.Int32
}

type recordingRenewOutboxStore struct {
	*models.Database
	renewals atomic.Int32
}

func (s *recordingRenewOutboxStore) RenewOutboxEffectLease(ctx context.Context, effectID uuid.UUID, leaseID string, now time.Time, leaseDuration time.Duration, databaseIdentity string, writerEpoch int64) error {
	s.renewals.Add(1)
	return s.Database.RenewOutboxEffectLease(ctx, effectID, leaseID, now, leaseDuration, databaseIdentity, writerEpoch)
}

func (s *panicRenewOutboxStore) RenewOutboxEffectLease(context.Context, uuid.UUID, string, time.Time, time.Duration, string, int64) error {
	s.renewals.Add(1)
	panic("simulated renewal panic")
}

func testOutboxDispatcherConfig() OutboxDispatcherConfig {
	return OutboxDispatcherConfig{
		Enabled:          true,
		DatabaseIdentity: githubWebhookTestDatabaseIdentity,
		WriterEpoch:      githubWebhookTestWriterEpoch,
		Workers:          1,
		PollInterval:     2 * time.Millisecond,
		LeaseDuration:    90 * time.Millisecond,
		MaxAttempts:      3,
		RetryBase:        2 * time.Millisecond,
		RetryMax:         5 * time.Millisecond,
	}
}

func newTestOutboxDispatcher(t *testing.T, store outboxEffectStore, dispatch OutboxDispatchFunc, config OutboxDispatcherConfig) *OutboxDispatcher {
	t.Helper()
	dispatcher, err := NewOutboxDispatcher(store, dispatch, config)
	require.NoError(t, err)
	return dispatcher
}

func enqueueOutboxDispatcherTestEffect(t *testing.T, database *models.Database, operationID string) *models.OutboxEffect {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, database.GormDB.Create(&models.ControlOperation{
		OperationID:     operationID,
		OperationKind:   "test",
		IdentitySHA256:  "identity",
		WriterEpoch:     githubWebhookTestWriterEpoch,
		ProtocolVersion: 1,
		Status:          models.ControlOperationPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}).Error)
	effect := models.NewOutboxEffect(operationID, "workflow_dispatch", "job:"+operationID, []byte(`{"job":"test"}`), githubWebhookTestWriterEpoch, now)
	receipt, created, err := database.EnqueueOutboxEffect(context.Background(), effect, githubWebhookTestDatabaseIdentity, githubWebhookTestWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	return receipt
}

func waitForOutboxEffectStatus(t *testing.T, database *models.Database, effectID uuid.UUID, status models.OutboxEffectStatus) models.OutboxEffect {
	t.Helper()
	var effect models.OutboxEffect
	require.Eventually(t, func() bool {
		var current models.OutboxEffect
		if err := database.GormDB.First(&current, "id = ?", effectID).Error; err != nil {
			return false
		}
		effect = current
		return effect.Status == status
	}, 3*time.Second, 5*time.Millisecond)
	return effect
}

func shutdownOutboxDispatcher(t *testing.T, dispatcher *OutboxDispatcher) {
	t.Helper()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, dispatcher.Shutdown(shutdownCtx))
}

func TestOutboxDispatcherDisabledModeNeverClaims(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-disabled")
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		t.Fatal("disabled dispatcher invoked provider")
		return OutboxDispatchResult{}, nil
	}, DefaultOutboxDispatcherConfig())
	dispatcher.Start()
	shutdownOutboxDispatcher(t, dispatcher)

	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectPending, stored.Status)
	require.Zero(t, stored.AttemptCount)
}

func TestOutboxDispatcherShutdownBeforeStartNeverClaims(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-shutdown-before-start")
	var dispatches atomic.Int32
	config := testOutboxDispatcherConfig()
	config.Workers = 512
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		dispatches.Add(1)
		return OutboxDispatchResult{}, nil
	}, config)

	shutdownOutboxDispatcher(t, dispatcher)
	dispatcher.Start()
	shutdownOutboxDispatcher(t, dispatcher)
	require.Zero(t, dispatches.Load())
	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectPending, stored.Status)
	require.Zero(t, stored.AttemptCount)
}

func TestOutboxDispatcherRetriesWithStableIdempotencyKeyThenSucceeds(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-retry")
	requests := make(chan OutboxDispatchRequest, 3)
	var attempts atomic.Int32
	dispatcher := newTestOutboxDispatcher(t, database, func(_ context.Context, request OutboxDispatchRequest) (OutboxDispatchResult, error) {
		requests <- request
		if attempts.Add(1) == 1 {
			return OutboxDispatchResult{}, errors.New("transient provider failure")
		}
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"run_id":123}`)}, nil
	}, testOutboxDispatcherConfig())
	dispatcher.Start()

	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.Equal(t, int64(2), stored.AttemptCount)
	require.JSONEq(t, `{"run_id":123}`, string(stored.ProviderReceipt))
	first := <-requests
	second := <-requests
	require.Equal(t, first.IdempotencyKey, second.IdempotencyKey)
	require.Equal(t, "digger-outbox:"+effect.ID.String(), first.IdempotencyKey)
	require.Equal(t, effect.ID, first.EffectID)
	require.Equal(t, effect.ControlOperationID, first.OperationID)
	shutdownOutboxDispatcher(t, dispatcher)
}

func TestOutboxDispatcherRetriesSameEffectWhenCompletionCommitFails(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-completion-failure")
	store := &completionFailureOutboxStore{Database: database}
	requests := make(chan OutboxDispatchRequest, 3)
	dispatcher := newTestOutboxDispatcher(t, store, func(_ context.Context, request OutboxDispatchRequest) (OutboxDispatchResult, error) {
		requests <- request
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"accepted":true}`)}, nil
	}, testOutboxDispatcherConfig())
	dispatcher.Start()

	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.Equal(t, int64(2), stored.AttemptCount)
	first := <-requests
	second := <-requests
	require.Equal(t, first.IdempotencyKey, second.IdempotencyKey)
	require.True(t, store.failed.Load())
	shutdownOutboxDispatcher(t, dispatcher)
}

func TestOutboxDispatcherDeadLettersProviderPoisonAtMaxAttempts(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-provider-poison")
	requests := make(chan OutboxDispatchRequest, 4)
	config := testOutboxDispatcherConfig()
	config.MaxAttempts = 3
	dispatcher := newTestOutboxDispatcher(t, database, func(_ context.Context, request OutboxDispatchRequest) (OutboxDispatchResult, error) {
		requests <- request
		return OutboxDispatchResult{}, errors.New("provider poison")
	}, config)
	dispatcher.Start()

	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectDeadLetter)
	require.Equal(t, int64(3), stored.AttemptCount)
	require.Equal(t, "provider poison", stored.LastError)
	require.Empty(t, stored.LeaseID)
	require.Nil(t, stored.LeaseExpiresAt)
	require.Nil(t, stored.NextAttemptAt)
	require.Empty(t, stored.ProviderReceipt)
	for attempt := 0; attempt < 3; attempt++ {
		request := <-requests
		require.Equal(t, "digger-outbox:"+effect.ID.String(), request.IdempotencyKey)
	}
	select {
	case <-requests:
		t.Fatal("dead-lettered effect was dispatched again")
	case <-time.After(50 * time.Millisecond):
	}
	shutdownOutboxDispatcher(t, dispatcher)
}

func TestOutboxDispatcherReconciliationPollsPastMaxAttempts(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	operationEffect := enqueueOutboxDispatcherTestEffect(t, database, "operation-reconcile-poll")
	require.NoError(t, database.GormDB.Delete(operationEffect).Error)
	payload, err := json.Marshal(models.GithubRunReconciliationPayload{OperationID: operationEffect.ControlOperationID, DispatchEffectID: uuid.New()})
	require.NoError(t, err)
	effect := models.NewOutboxEffect(operationEffect.ControlOperationID, models.GithubWorkflowReconcileEffectKind, "run:1:2", payload, githubWebhookTestWriterEpoch, time.Now().UTC())
	_, _, err = database.EnqueueOutboxEffect(context.Background(), effect, githubWebhookTestDatabaseIdentity, githubWebhookTestWriterEpoch)
	require.NoError(t, err)
	config := testOutboxDispatcherConfig()
	config.MaxAttempts = 2
	var attempts atomic.Int32
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		if attempts.Add(1) <= 5 {
			return OutboxDispatchResult{RetryAfter: time.Millisecond}, nil
		}
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"terminal_noop":true}`)}, nil
	}, config)
	dispatcher.Start()
	t.Cleanup(func() { shutdownOutboxDispatcher(t, dispatcher) })
	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.Equal(t, int64(6), stored.AttemptCount)
	require.Equal(t, int32(6), attempts.Load())
	require.Empty(t, stored.LastError)
}

func TestOutboxDispatcherDoesNotRedispatchPermanentIdentityMismatch(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-identity-mismatch")
	var attempts atomic.Int32
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		attempts.Add(1)
		return OutboxDispatchResult{}, ErrOutboxDispatchPermanent
	}, testOutboxDispatcherConfig())
	dispatcher.Start()
	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectDeadLetter)
	shutdownOutboxDispatcher(t, dispatcher)
	require.Equal(t, int64(1), stored.AttemptCount)
	require.Equal(t, int32(1), attempts.Load())
}

func TestEnabledOutboxDispatcherRejectsMisconfigurationBeforeClaim(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-no-handler")
	dispatch := OutboxDispatchFunc(func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		return OutboxDispatchResult{}, nil
	})
	tests := []struct {
		name     string
		store    outboxEffectStore
		dispatch OutboxDispatchFunc
		config   OutboxDispatcherConfig
	}{
		{name: "store", store: nil, dispatch: dispatch, config: testOutboxDispatcherConfig()},
		{name: "handler", store: database, dispatch: nil, config: testOutboxDispatcherConfig()},
		{name: "database identity", store: database, dispatch: dispatch, config: func() OutboxDispatcherConfig {
			config := testOutboxDispatcherConfig()
			config.DatabaseIdentity = ""
			return config
		}()},
		{name: "writer epoch", store: database, dispatch: dispatch, config: func() OutboxDispatcherConfig {
			config := testOutboxDispatcherConfig()
			config.WriterEpoch = 0
			return config
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatcher, err := NewOutboxDispatcher(test.store, test.dispatch, test.config)
			require.ErrorIs(t, err, ErrOutboxDispatcherMisconfigured)
			require.Nil(t, dispatcher)
		})
	}

	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectPending, stored.Status)
	require.Zero(t, stored.AttemptCount)
}

func TestOutboxDispatcherCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-lease-loss")
	store := &leaseLossOutboxStore{Database: database}
	handlerCancelled := make(chan struct{})
	var cancelOnce sync.Once
	config := testOutboxDispatcherConfig()
	config.LeaseDuration = 30 * time.Millisecond
	dispatcher := newTestOutboxDispatcher(t, store, func(ctx context.Context, _ OutboxDispatchRequest) (OutboxDispatchResult, error) {
		<-ctx.Done()
		cancelOnce.Do(func() { close(handlerCancelled) })
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"must_not_commit":true}`)}, nil
	}, config)
	dispatcher.Start()

	select {
	case <-handlerCancelled:
	case <-time.After(time.Second):
		t.Fatal("handler was not cancelled when its lease was lost")
	}
	shutdownOutboxDispatcher(t, dispatcher)
	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.NotEqual(t, models.OutboxEffectSucceeded, stored.Status)
	require.Empty(t, stored.ProviderReceipt)
}

func TestOutboxDispatcherRenewPanicDoesNotOrphanHandler(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	enqueueOutboxDispatcherTestEffect(t, database, "operation-renew-panic")
	store := &panicRenewOutboxStore{Database: database}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var activeHandlers atomic.Int32
	var maxHandlers atomic.Int32
	config := testOutboxDispatcherConfig()
	config.LeaseDuration = 30 * time.Millisecond
	dispatcher := newTestOutboxDispatcher(t, store, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		active := activeHandlers.Add(1)
		defer activeHandlers.Add(-1)
		for {
			observed := maxHandlers.Load()
			if active <= observed || maxHandlers.CompareAndSwap(observed, active) {
				break
			}
		}
		close(handlerStarted)
		<-releaseHandler
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"must_not_commit":true}`)}, nil
	}, config)
	dispatcher.Start()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not dispatch effect")
	}
	require.Eventually(t, func() bool { return store.renewals.Load() > 0 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), activeHandlers.Load())
	require.Equal(t, int32(1), maxHandlers.Load())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- dispatcher.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned while canceled handler was still running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseHandler)
	require.NoError(t, <-shutdownDone)
	require.Zero(t, activeHandlers.Load())
	require.Equal(t, int32(1), maxHandlers.Load())
}

func TestOutboxDispatcherLongHandlerRemainsSinglyOwnedWhileLeaseRenews(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-long-handler")
	store := &recordingRenewOutboxStore{Database: database}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var startOnce sync.Once
	var dispatches atomic.Int32
	config := testOutboxDispatcherConfig()
	config.Workers = 4
	config.LeaseDuration = 30 * time.Millisecond
	dispatcher := newTestOutboxDispatcher(t, store, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		dispatches.Add(1)
		startOnce.Do(func() { close(handlerStarted) })
		<-releaseHandler
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"renewed":true}`)}, nil
	}, config)
	dispatcher.Start()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not dispatch effect")
	}
	require.Eventually(t, func() bool { return store.renewals.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), dispatches.Load())
	var processing models.OutboxEffect
	require.NoError(t, database.GormDB.First(&processing, "id = ?", effect.ID).Error)
	require.Equal(t, models.OutboxEffectProcessing, processing.Status)
	require.NotEmpty(t, processing.LeaseID)

	close(releaseHandler)
	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.JSONEq(t, `{"renewed":true}`, string(stored.ProviderReceipt))
	require.Equal(t, int32(1), dispatches.Load())
	shutdownOutboxDispatcher(t, dispatcher)
}

func TestOutboxDispatcherShutdownDrainsClaimedEffect(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "operation-drain")
	dispatchStarted := make(chan struct{})
	releaseDispatch := make(chan struct{})
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		close(dispatchStarted)
		<-releaseDispatch
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"drained":true}`)}, nil
	}, testOutboxDispatcherConfig())
	dispatcher.Start()

	select {
	case <-dispatchStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not claim effect")
	}
	shutdownDone := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { shutdownDone <- dispatcher.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight dispatch drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseDispatch)
	require.NoError(t, <-shutdownDone)
	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.JSONEq(t, `{"drained":true}`, string(stored.ProviderReceipt))
}
