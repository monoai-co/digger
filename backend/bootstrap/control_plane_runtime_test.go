package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/controllers"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type runtimeTestWorker struct {
	starts           atomic.Int32
	stops            atomic.Int32
	admissionStopped atomic.Bool
	readyErr         error
	drain            func(context.Context) error
}

func newRuntimeTestWorker() *runtimeTestWorker           { return &runtimeTestWorker{} }
func (w *runtimeTestWorker) Start()                      { w.starts.Add(1) }
func (w *runtimeTestWorker) StopClaims()                 { w.stops.Add(1) }
func (w *runtimeTestWorker) Ready(context.Context) error { return w.readyErr }
func (w *runtimeTestWorker) StopAdmission() <-chan struct{} {
	w.admissionStopped.Store(true)
	done := make(chan struct{})
	close(done)
	return done
}
func (w *runtimeTestWorker) Shutdown(ctx context.Context) error {
	if w.drain != nil {
		return w.drain(ctx)
	}
	return nil
}

func TestControlPlaneRuntimeDisabledPreservesLegacyProvider(t *testing.T) {
	controller := controllers.DiggerController{}
	runtime, err := newControlPlaneRuntime(&controller, nil, controllers.DefaultGithubWebhookProcessorConfig())
	require.NoError(t, err)
	require.NotNil(t, controller.GithubWebhookProcessor)
	require.False(t, controller.GithubWebhookProcessor.Enabled())
	require.NotNil(t, controller.OutboxDispatcher)
	require.Error(t, runtime.Ready(context.Background()))
	require.NoError(t, runtime.Start(context.Background()))
	require.NoError(t, runtime.Ready(context.Background()))
	require.NoError(t, runtime.Shutdown(context.Background()))
	require.NoError(t, runtime.Shutdown(context.Background()))
	require.Error(t, runtime.Ready(context.Background()))
	require.Error(t, runtime.Start(context.Background()))
}

func TestControlPlaneRuntimeRejectsIncompleteConfigurationBeforeWorkersStart(t *testing.T) {
	config := controllers.DefaultGithubWebhookProcessorConfig()
	config.Enabled = true
	controller := controllers.DiggerController{}
	_, err := newControlPlaneRuntime(&controller, &models.Database{}, config)
	require.Error(t, err)
	require.Nil(t, controller.GithubWebhookProcessor)
	config.DatabaseIdentity, config.WriterEpoch = "test", 7
	controller.ControlPlaneDatabaseIdentity, controller.ControlPlaneWriterEpoch = "test", 7
	_, err = newControlPlaneRuntime(&controller, &models.Database{}, config)
	require.ErrorContains(t, err, "context-aware")
	controller.GithubClientProvider = utils.DiggerGithubRealClientProvider{}
	runtime, err := newControlPlaneRuntime(&controller, &models.Database{}, config)
	require.NoError(t, err)
	require.ErrorIs(t, runtime.Start(context.Background()), models.ErrExecutionGrantKeysNotReady)
	require.False(t, runtime.started)
	require.NoError(t, runtime.Shutdown(context.Background()))
}

func TestControlPlaneRuntimeValidatesBeforeStartingEitherWorker(t *testing.T) {
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox}
	for _, cause := range []string{"key fingerprint mismatch", "durable schema missing apply recovery revision"} {
		validationErr := errors.New(cause)
		runtime.validate = func(context.Context) error { return validationErr }
		require.ErrorIs(t, runtime.Start(context.Background()), validationErr)
		require.Zero(t, ingress.starts.Load())
		require.Zero(t, outbox.starts.Load())
	}
	runtime.validate = func(context.Context) error { return nil }
	require.NoError(t, runtime.Start(context.Background()))
	require.NoError(t, runtime.Start(context.Background()))
	require.EqualValues(t, 1, ingress.starts.Load())
	require.EqualValues(t, 1, outbox.starts.Load())
}

func TestControlPlaneRuntimeStopsBothClaimLoopsBeforeDraining(t *testing.T) {
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error { return nil }}
	require.NoError(t, runtime.Start(context.Background()))
	drainsStarted := make(chan struct{}, 2)
	release := make(chan struct{})
	drain := func(ctx context.Context) error {
		if !ingress.admissionStopped.Load() || ingress.stops.Load() == 0 || outbox.stops.Load() == 0 {
			return errors.New("drain began before admission and both claim loops stopped")
		}
		drainsStarted <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ingress.drain, outbox.drain = drain, drain
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { done <- runtime.Shutdown(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-drainsStarted:
		case <-ctx.Done():
			t.Fatal("both workers did not begin draining")
		}
	}
	select {
	case <-done:
		t.Fatal("shutdown did not wait for active work")
	default:
	}
	close(release)
	require.NoError(t, <-done)
}

func TestControlPlaneRuntimeDrainDeadlineStopsBothWorkers(t *testing.T) {
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	var drained atomic.Int32
	drain := func(ctx context.Context) error { drained.Add(1); <-ctx.Done(); return ctx.Err() }
	ingress.drain, outbox.drain = drain, drain
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error { return nil }}
	require.NoError(t, runtime.Start(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, runtime.Shutdown(ctx), context.DeadlineExceeded)
	require.EqualValues(t, 2, drained.Load())
	require.Positive(t, ingress.stops.Load())
	require.Positive(t, outbox.stops.Load())
}

type runtimeWakeRecorder struct{ calls atomic.Int32 }

func (w *runtimeWakeRecorder) Wake() { w.calls.Add(1) }

func TestWebhookProcessingClosureUsesFinalControllerBindings(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, database.AutoMigrate(&models.ControlPlaneFence{}))
	require.NoError(t, database.Exec("CREATE TABLE outbox_effects (effect_kind text, effect_key text, status text, next_attempt_at datetime, updated_at datetime)").Error)
	require.NoError(t, database.Create(&models.ControlPlaneFence{ID: models.ControlPlaneFenceSingletonID, DatabaseIdentity: "final-writer", WriterEpoch: 7, Mode: models.ControlPlaneModeNormal, ProtocolFloor: 1}).Error)
	previous := models.DB
	models.DB = &models.Database{GormDB: database}
	t.Cleanup(func() { models.DB = previous })
	controller := controllers.DiggerController{}
	process := bindGithubWebhookProcessing(&controller)
	wake := &runtimeWakeRecorder{}
	controller.OutboxDispatcher = wake
	controller.ControlPlaneDatabaseIdentity, controller.ControlPlaneWriterEpoch = "final-writer", 7
	_, err = process(context.Background(), &models.GithubWebhookDelivery{EventType: "workflow_run", Payload: []byte(`{"repository":{"id":42},"workflow_run":{"id":1001}}`)})
	require.NoError(t, err)
	require.EqualValues(t, 1, wake.calls.Load())
}

func TestRunServerStopsBothWorkersWhenHTTPHandlerExceedsDrainDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	_, portString, err := net.SplitHostPort(address)
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error { return nil }}
	require.NoError(t, runtime.Start(context.Background()))
	handlerStarted, handlerFinished, releaseHandler := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		close(releaseHandler)
		select {
		case <-handlerFinished:
		case <-time.After(time.Second):
			t.Error("HTTP handler did not finish")
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunServer(ctx, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(handlerStarted)
			defer close(handlerFinished)
			<-releaseHandler
		}), port, runtime, 40*time.Millisecond)
	}()
	var connection net.Conn
	require.Eventually(t, func() bool {
		connection, err = net.DialTimeout("tcp", address, 10*time.Millisecond)
		return err == nil
	}, time.Second, time.Millisecond)
	defer connection.Close()
	_, err = connection.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("server shutdown exceeded its deadline")
	}
	require.True(t, ingress.admissionStopped.Load())
	require.Positive(t, ingress.stops.Load())
	require.Positive(t, outbox.stops.Load())
}
