package bootstrap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestControlPlaneReadinessRequiresBothWorkersAndValidation(t *testing.T) {
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error { return nil }}
	router := gin.New()
	router.GET("/ready", controlPlaneReadinessHandler(runtime))
	check := func(expected int) {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, expected, response.Code)
	}
	check(http.StatusServiceUnavailable)
	require.NoError(t, runtime.Start(context.Background()))
	check(http.StatusOK)
	outbox.readyErr = errors.New("outbox unavailable")
	check(http.StatusServiceUnavailable)
	outbox.readyErr = nil
	ingress.readyErr = errors.New("ingress unavailable")
	check(http.StatusServiceUnavailable)
	ingress.readyErr = nil
	runtime.validate = func(context.Context) error { return errors.New("writer or keys unavailable") }
	check(http.StatusServiceUnavailable)
	runtime.validate = func(context.Context) error { return errors.New("durable schema missing apply recovery revision") }
	check(http.StatusServiceUnavailable)
	runtime.validate = func(context.Context) error { return nil }
	runtime.StopAdmission()
	check(http.StatusServiceUnavailable)
}
