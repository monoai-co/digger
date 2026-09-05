package controllers

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/stretchr/testify/require"
)

func TestOutboxStopClaimsBeforeStartCannotBeRestarted(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "stop-before-start")
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		t.Error("stopped dispatcher called provider")
		return OutboxDispatchResult{}, nil
	}, testOutboxDispatcherConfig())
	require.Error(t, dispatcher.Ready(context.Background()))
	dispatcher.StopClaims()
	dispatcher.StopClaims()
	dispatcher.Start()
	require.Error(t, dispatcher.Ready(context.Background()))
	shutdownOutboxDispatcher(t, dispatcher)
	var stored models.OutboxEffect
	require.NoError(t, database.GormDB.First(&stored, "id = ?", effect.ID).Error)
	require.Zero(t, stored.AttemptCount)
}

func TestOutboxStopClaimsPreservesOwnedEffectUntilDrained(t *testing.T) {
	database := newGithubWebhookProcessorTestDatabase(t)
	effect := enqueueOutboxDispatcherTestEffect(t, database, "stop-owned-effect")
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	dispatcher := newTestOutboxDispatcher(t, database, func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error) {
		close(started)
		<-release
		return OutboxDispatchResult{ProviderReceipt: json.RawMessage(`{"accepted":true}`)}, nil
	}, testOutboxDispatcherConfig())
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		shutdownOutboxDispatcher(t, dispatcher)
	})
	dispatcher.Start()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("effect was not claimed")
	}
	require.NoError(t, dispatcher.Ready(context.Background()))
	dispatcher.StopClaims()
	require.Error(t, dispatcher.Ready(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	require.ErrorIs(t, dispatcher.Shutdown(ctx), context.DeadlineExceeded)
	cancel()
	releaseOnce.Do(func() { close(release) })
	shutdownOutboxDispatcher(t, dispatcher)
	stored := waitForOutboxEffectStatus(t, database, effect.ID, models.OutboxEffectSucceeded)
	require.Equal(t, int64(1), stored.AttemptCount)
}

func TestWebhookStopClaimsBeforeStartCannotCallHandler(t *testing.T) {
	processor := NewGithubWebhookProcessor(nil, func(context.Context, *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
		t.Error("stopped processor called handler")
		return GithubWebhookProcessingResult{}, nil
	}, GithubWebhookProcessorConfig{Enabled: true})
	processor.StopAdmission()
	processor.StopClaims()
	processor.StopClaims()
	processor.Start()
	require.ErrorIs(t, processor.Ready(context.Background()), ErrGithubWebhookProcessorStopping)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, processor.Shutdown(ctx))
}

func TestDisabledOutboxLifecycleReadiness(t *testing.T) {
	dispatcher := newTestOutboxDispatcher(t, nil, nil, DefaultOutboxDispatcherConfig())
	require.Error(t, dispatcher.Ready(context.Background()))
	dispatcher.Start()
	require.NoError(t, dispatcher.Ready(context.Background()))
	dispatcher.StopClaims()
	require.Error(t, dispatcher.Ready(context.Background()))
	shutdownOutboxDispatcher(t, dispatcher)
}
