package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/diggerhq/digger/backend/controllers"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
)

type controlPlaneWorker interface {
	Start()
	StopClaims()
	Shutdown(context.Context) error
	Ready(context.Context) error
}

type controlPlaneIngress interface {
	controlPlaneWorker
	StopAdmission() <-chan struct{}
}

// ControlPlaneRuntime owns worker lifecycle together, so shutdown cannot leave
// workflow dispatch running after webhook processing has been drained.
type ControlPlaneRuntime struct {
	ingress   controlPlaneIngress
	outbox    controlPlaneWorker
	validate  func(context.Context) error
	lifecycle sync.Mutex
	started   bool
	stopped   bool
}

func newControlPlaneRuntime(controller *controllers.DiggerController, database *models.Database, config controllers.GithubWebhookProcessorConfig) (*ControlPlaneRuntime, error) {
	if controller == nil {
		return nil, errors.New("control-plane controller is required")
	}
	outboxConfig := controllers.DefaultOutboxDispatcherConfig()
	outboxConfig.Enabled = config.Enabled
	outboxConfig.DatabaseIdentity = config.DatabaseIdentity
	outboxConfig.WriterEpoch = config.WriterEpoch
	var dispatch controllers.OutboxDispatchFunc
	if config.Enabled {
		if database == nil || strings.TrimSpace(config.DatabaseIdentity) == "" || config.WriterEpoch <= 0 ||
			controller.ControlPlaneDatabaseIdentity != config.DatabaseIdentity || controller.ControlPlaneWriterEpoch != config.WriterEpoch {
			return nil, errors.New("durable workers require a shared database identity and positive writer epoch")
		}
		provider, ok := controller.GithubClientProvider.(utils.ContextGithubClientProvider)
		if !ok {
			return nil, errors.New("durable workers require a context-aware GitHub provider")
		}
		var err error
		dispatch, err = controllers.NewGithubWorkflowOutboxDispatch(database, provider, controllers.DefaultDurableJobTokenValidity)
		if err != nil {
			return nil, err
		}
	}
	outbox, err := controllers.NewOutboxDispatcher(database, dispatch, outboxConfig)
	if err != nil {
		return nil, err
	}
	controller.OutboxDispatcher = outbox
	// A method value would capture a copy before GithubWebhookProcessor is set.
	// The closure instead reads the fully assembled controller when invoked.
	processor := controllers.NewGithubWebhookProcessor(database, bindGithubWebhookProcessing(controller), config)
	controller.GithubWebhookProcessor = processor
	runtime := &ControlPlaneRuntime{ingress: processor, outbox: outbox}
	runtime.validate = func(ctx context.Context) error {
		if config.Enabled || controller.ExecutionGrantSigningKeyID != "" {
			if err := controller.ExecutionClaimsReady(ctx); err != nil {
				return fmt.Errorf("execution keys unavailable: %w", err)
			}
		}
		if config.Enabled {
			if err := database.CheckAuthoritativeWriter(ctx, config.DatabaseIdentity, config.WriterEpoch, false); err != nil {
				return err
			}
			return database.CheckDurableControlPlaneSchema(ctx)
		}
		return nil
	}
	return runtime, nil
}

func bindGithubWebhookProcessing(controller *controllers.DiggerController) controllers.GithubWebhookProcessFunc {
	return func(ctx context.Context, delivery *models.GithubWebhookDelivery) (controllers.GithubWebhookProcessingResult, error) {
		return controller.ProcessGithubWebhookDelivery(ctx, delivery)
	}
}

func (r *ControlPlaneRuntime) Start(ctx context.Context) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if r.stopped {
		return errors.New("control-plane runtime is stopping")
	}
	if r.started {
		return nil
	}
	if err := r.validate(ctx); err != nil {
		return err
	}
	r.outbox.Start()
	r.ingress.Start()
	r.started = true
	return nil
}

func (r *ControlPlaneRuntime) Ready(ctx context.Context) error {
	r.lifecycle.Lock()
	ready := r.started && !r.stopped
	r.lifecycle.Unlock()
	if !ready {
		return errors.New("control-plane runtime is not accepting work")
	}
	if err := r.validate(ctx); err != nil {
		return err
	}
	return errors.Join(r.ingress.Ready(ctx), r.outbox.Ready(ctx))
}

func (r *ControlPlaneRuntime) StopAdmission() <-chan struct{} {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.stopped = true
	drained := r.ingress.StopAdmission()
	r.ingress.StopClaims()
	r.outbox.StopClaims()
	return drained
}

func (r *ControlPlaneRuntime) Shutdown(ctx context.Context) error {
	r.StopAdmission()
	// Both claim loops are stopped before either wait; drain in parallel so
	// one worker exhausting the deadline cannot prevent stopping the other.
	results := make(chan error, 2)
	go func() { results <- r.ingress.Shutdown(ctx) }()
	go func() { results <- r.outbox.Shutdown(ctx) }()
	return errors.Join(<-results, <-results)
}
