package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunServerBindFailureCannotStartDurableWorkers(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()
	_, portString, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error { return nil }}
	err = RunServer(context.Background(), http.NotFoundHandler(), port, runtime, time.Second)
	require.Error(t, err)
	require.Zero(t, ingress.starts.Load())
	require.Zero(t, outbox.starts.Load())
	require.Positive(t, ingress.stops.Load())
	require.Positive(t, outbox.stops.Load())
}

func TestRunServerValidatesRuntimeAfterBindingAndClosesOnFailure(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	_, portString, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	validationErr := errors.New("schema unavailable")
	ingress, outbox := newRuntimeTestWorker(), newRuntimeTestWorker()
	runtime := &ControlPlaneRuntime{ingress: ingress, outbox: outbox, validate: func(context.Context) error {
		probe, bindErr := net.Listen("tcp", ":"+portString)
		if bindErr == nil {
			_ = probe.Close()
			t.Error("runtime validation ran before the listener was bound")
		}
		return validationErr
	}}
	err = RunServer(context.Background(), http.NotFoundHandler(), port, runtime, time.Second)
	require.ErrorIs(t, err, validationErr)
	require.Zero(t, ingress.starts.Load())
	require.Zero(t, outbox.starts.Load())
	probe, err := net.Listen("tcp", ":"+portString)
	require.NoError(t, err)
	require.NoError(t, probe.Close())
}
