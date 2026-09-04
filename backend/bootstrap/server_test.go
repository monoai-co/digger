package bootstrap

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunServerExitsAfterSIGTERM(t *testing.T) {
	if os.Getenv("DIGGER_SIGTERM_HELPER") == "1" {
		processCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		fmt.Println("ready")
		if err := RunServer(processCtx, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusNoContent)
		}), 0, &recordingDrainer{}, time.Second); err != nil {
			os.Exit(2)
		}
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestRunServerExitsAfterSIGTERM$")
	command.Env = append(os.Environ(), "DIGGER_SIGTERM_HELPER=1")
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	command.Stderr = command.Stdout
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
		}
	})

	scanner := bufio.NewScanner(stdout)
	require.True(t, scanner.Scan())
	require.Equal(t, "ready", scanner.Text())
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	require.NoError(t, command.Wait())
}

type recordingDrainer struct {
	called           atomic.Bool
	admissionStopped atomic.Bool
}

func (d *recordingDrainer) StopAdmission() <-chan struct{} {
	d.admissionStopped.Store(true)
	done := make(chan struct{})
	close(done)
	return done
}

func (d *recordingDrainer) Shutdown(_ context.Context) error {
	d.called.Store(true)
	return nil
}

func TestRunServerDrainsAfterProcessCancellation(t *testing.T) {
	processCtx, cancel := context.WithCancel(context.Background())
	drainer := &recordingDrainer{}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- RunServer(processCtx, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusNoContent)
		}), 0, drainer, time.Second)
	}()

	cancel()
	select {
	case err := <-serverDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
	require.True(t, drainer.called.Load())
	require.True(t, drainer.admissionStopped.Load())
}

func TestRunServerCancelsBlockedRequestBeforeStoppingAdmission(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	processCtx, cancelProcess := context.WithCancel(context.Background())
	drainer := &recordingDrainer{}
	handlerStarted := make(chan struct{})
	handlerCancelled := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- RunServer(processCtx, http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(handlerStarted)
			<-request.Context().Done()
			if drainer.admissionStopped.Load() {
				t.Error("processor admission stopped before accepted HTTP handler was cancelled")
			}
			close(handlerCancelled)
		}), port, drainer, 200*time.Millisecond)
	}()
	require.Eventually(t, func() bool {
		connection, dialErr := net.DialTimeout("tcp", "127.0.0.1:"+fmt.Sprint(port), 20*time.Millisecond)
		if dialErr != nil {
			return false
		}
		return connection.Close() == nil
	}, time.Second, 5*time.Millisecond)

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		_, _ = http.Get("http://127.0.0.1:" + fmt.Sprint(port))
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	cancelProcess()
	select {
	case <-handlerCancelled:
	case <-time.After(time.Second):
		t.Fatal("blocked request context was not cancelled")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not respect shutdown deadline")
	}
	require.True(t, drainer.admissionStopped.Load())
	<-requestDone
}
