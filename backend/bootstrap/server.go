package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type gracefulDrainer interface {
	StopAdmission() <-chan struct{}
	Shutdown(context.Context) error
}

type httpHandlerTracker struct {
	handler http.Handler
	active  sync.WaitGroup
}

func (t *httpHandlerTracker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	t.active.Add(1)
	defer t.active.Done()
	t.handler.ServeHTTP(writer, request)
}

func (t *httpHandlerTracker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.active.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunServer serves until the process context is cancelled, then stops HTTP
// admission before draining the control-plane workers.
func RunServer(ctx context.Context, handler http.Handler, port int, drainer gracefulDrainer, shutdownTimeout time.Duration) error {
	trackedHandler := &httpHandlerTracker{handler: handler}
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           trackedHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErrCh:
		if !errors.Is(err, http.ErrServerClosed) {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			closeErr := server.Close()
			if handlerErr := trackedHandler.Wait(shutdownCtx); handlerErr != nil {
				drainer.StopAdmission()
				return errors.Join(err, closeErr, fmt.Errorf("drain accepted HTTP handlers: %w", handlerErr), drainer.Shutdown(shutdownCtx))
			}
			drainer.StopAdmission()
			return errors.Join(err, closeErr, drainer.Shutdown(shutdownCtx))
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	slog.Info("Stopping HTTP admission and draining control-plane workers", "timeout", shutdownTimeout)
	httpBudget := shutdownTimeout / 4
	if httpBudget > 10*time.Second {
		httpBudget = 10 * time.Second
	}
	if httpBudget <= 0 {
		httpBudget = time.Millisecond
	}
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), httpBudget)
	httpErr := server.Shutdown(httpCtx)
	cancelHTTP()
	if httpErr != nil {
		// Cancel handlers that did not finish their receipt write within the
		// reserved HTTP budget before closing processor admission.
		httpErr = errors.Join(httpErr, server.Close())
	}
	if handlerErr := trackedHandler.Wait(shutdownCtx); handlerErr != nil {
		drainer.StopAdmission()
		return errors.Join(httpErr, fmt.Errorf("drain accepted HTTP handlers: %w", handlerErr), drainer.Shutdown(shutdownCtx))
	}
	drainer.StopAdmission()
	drainErr := drainer.Shutdown(shutdownCtx)
	if httpErr != nil || drainErr != nil {
		return errors.Join(httpErr, drainErr)
	}
	slog.Info("HTTP server and control-plane workers drained")
	return nil
}
