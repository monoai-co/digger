package controllers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/diggerhq/digger/backend/logging"
	"github.com/diggerhq/digger/backend/models"
	"github.com/google/uuid"
)

var ErrGithubWebhookProcessorStopping = errors.New("github webhook processor is stopping")
var ErrGithubWebhookProcessorDisabled = errors.New("durable github webhook processing is disabled")

type githubWebhookDeliveryStore interface {
	RecordGithubWebhookDelivery(context.Context, *models.GithubWebhookDelivery, string, int64) (*models.GithubWebhookDelivery, bool, error)
	ClaimNextGithubWebhookDelivery(context.Context, time.Time, string, time.Duration, string, int64) (*models.GithubWebhookDelivery, error)
	RenewGithubWebhookDeliveryLease(context.Context, string, string, time.Time, time.Duration, string, int64) error
	CompleteGithubWebhookDelivery(context.Context, string, string, models.GithubWebhookDeliveryStatus, string, time.Time, string, int64) error
	RetryGithubWebhookDelivery(context.Context, string, string, string, time.Time, time.Time, string, int64) error
	DeadLetterGithubWebhookDelivery(context.Context, string, string, string, time.Time, string, int64) error
	RequeueGithubWebhookDelivery(context.Context, string, string, string, time.Time, string, int64) error
	CheckGithubWebhookInbox(context.Context) error
	CheckAuthoritativeWriter(context.Context, string, int64, bool) error
}

type githubWebhookAdmitter interface {
	Enabled() bool
	Admit(ctx context.Context, delivery *models.GithubWebhookDelivery) (*models.GithubWebhookDelivery, bool, error)
	RequeueDeadLetter(context.Context, string, string, string) error
}

type GithubWebhookProcessorConfig struct {
	Enabled          bool
	DatabaseIdentity string
	WriterEpoch      int64
	Workers          int
	PollInterval     time.Duration
	LeaseDuration    time.Duration
	MaxAttempts      int64
	RetryBase        time.Duration
	RetryMax         time.Duration
	RetryHorizon     time.Duration
}

func DefaultGithubWebhookProcessorConfig() GithubWebhookProcessorConfig {
	return GithubWebhookProcessorConfig{
		Enabled:       false,
		Workers:       4,
		PollInterval:  time.Second,
		LeaseDuration: 2 * time.Minute,
		MaxAttempts:   10000,
		RetryBase:     time.Second,
		RetryMax:      5 * time.Minute,
		RetryHorizon:  30 * 24 * time.Hour,
	}
}

type GithubWebhookProcessingResult struct {
	Status         models.GithubWebhookDeliveryStatus
	TerminalResult string
}

type GithubWebhookProcessFunc func(context.Context, *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error)

// GithubWebhookProcessor runs a fixed worker pool over the durable webhook inbox.
// Work is claimed with renewable leases, so pending and interrupted deliveries are
// recovered after a process restart without creating a goroutine per request.
type GithubWebhookProcessor struct {
	store   githubWebhookDeliveryStore
	process GithubWebhookProcessFunc
	config  GithubWebhookProcessorConfig

	wakeCh       chan struct{}
	stopCh       chan struct{}
	doneCh       chan struct{}
	startOnce    sync.Once
	stopOnce     sync.Once
	workers      sync.WaitGroup
	claimCtx     context.Context
	cancelClaims context.CancelFunc

	admissionMu       sync.Mutex
	accepting         bool
	admissions        int
	admissionsDrained chan struct{}

	started        atomic.Bool
	activeWorkers  atomic.Int32
	workerRestarts atomic.Uint64
}

func NewGithubWebhookProcessor(store githubWebhookDeliveryStore, process GithubWebhookProcessFunc, config GithubWebhookProcessorConfig) *GithubWebhookProcessor {
	drained := make(chan struct{})
	close(drained)
	claimCtx, cancelClaims := context.WithCancel(context.Background())
	return &GithubWebhookProcessor{
		store:             store,
		process:           process,
		config:            normalizeGithubWebhookProcessorConfig(config),
		wakeCh:            make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		claimCtx:          claimCtx,
		cancelClaims:      cancelClaims,
		accepting:         true,
		admissionsDrained: drained,
	}
}

func normalizeGithubWebhookProcessorConfig(config GithubWebhookProcessorConfig) GithubWebhookProcessorConfig {
	defaults := DefaultGithubWebhookProcessorConfig()
	if config.Workers <= 0 {
		config.Workers = defaults.Workers
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = defaults.LeaseDuration
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaults.MaxAttempts
	}
	if config.RetryBase <= 0 {
		config.RetryBase = defaults.RetryBase
	}
	if config.RetryMax <= 0 {
		config.RetryMax = defaults.RetryMax
	}
	if config.RetryHorizon <= 0 {
		config.RetryHorizon = defaults.RetryHorizon
	}
	return config
}

func (p *GithubWebhookProcessor) Start() {
	p.startOnce.Do(func() {
		p.started.Store(true)
		if !p.config.Enabled {
			close(p.doneCh)
			return
		}
		for workerIndex := 0; workerIndex < p.config.Workers; workerIndex++ {
			p.workers.Add(1)
			go p.superviseWorker(workerIndex)
		}
		go func() {
			p.workers.Wait()
			close(p.doneCh)
		}()
		p.Wake()
	})
}

func (p *GithubWebhookProcessor) Enabled() bool {
	return p.config.Enabled
}

func (p *GithubWebhookProcessor) Admit(ctx context.Context, delivery *models.GithubWebhookDelivery) (*models.GithubWebhookDelivery, bool, error) {
	if !p.config.Enabled {
		return nil, false, ErrGithubWebhookProcessorDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !p.beginAdmission() {
		return nil, false, ErrGithubWebhookProcessorStopping
	}
	defer p.endAdmission()

	receipt, created, err := p.store.RecordGithubWebhookDelivery(ctx, delivery, p.config.DatabaseIdentity, p.config.WriterEpoch)
	if err != nil {
		return receipt, false, err
	}
	p.Wake()
	return receipt, created, nil
}

func (p *GithubWebhookProcessor) beginAdmission() bool {
	p.admissionMu.Lock()
	defer p.admissionMu.Unlock()
	if !p.accepting {
		return false
	}
	if p.admissions == 0 {
		p.admissionsDrained = make(chan struct{})
	}
	p.admissions++
	return true
}

func (p *GithubWebhookProcessor) endAdmission() {
	p.admissionMu.Lock()
	defer p.admissionMu.Unlock()
	p.admissions--
	if p.admissions == 0 {
		close(p.admissionsDrained)
	}
}

// StopAdmission atomically rejects new receipts while allowing already-started
// database writes to finish. The returned channel closes when those writes drain.
func (p *GithubWebhookProcessor) StopAdmission() <-chan struct{} {
	p.admissionMu.Lock()
	defer p.admissionMu.Unlock()
	p.accepting = false
	return p.admissionsDrained
}

func (p *GithubWebhookProcessor) Wake() {
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

func (p *GithubWebhookProcessor) Shutdown(ctx context.Context) error {
	p.Start()
	admissionsDrained := p.StopAdmission()
	select {
	case <-admissionsDrained:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.cancelClaims()
	})

	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *GithubWebhookProcessor) superviseWorker(workerIndex int) {
	defer p.workers.Done()
	for {
		panicked := p.runWorker(workerIndex)
		if !panicked {
			return
		}
		p.workerRestarts.Add(1)
		slog.Error("Restarting GitHub webhook worker after panic", "worker", workerIndex)
		select {
		case <-p.stopCh:
			return
		case <-time.After(p.config.PollInterval):
		}
	}
}

func (p *GithubWebhookProcessor) runWorker(workerIndex int) (panicked bool) {
	p.activeWorkers.Add(1)
	defer p.activeWorkers.Add(-1)
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			slog.Error("GitHub webhook worker panicked", "worker", workerIndex, "error", recovered, "stack", string(debug.Stack()))
		}
	}()
	p.worker(workerIndex)
	return false
}

func (p *GithubWebhookProcessor) worker(workerIndex int) {
	pollTicker := time.NewTicker(p.config.PollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		leaseID := uuid.NewString()
		delivery, err := p.store.ClaimNextGithubWebhookDelivery(p.claimCtx, time.Now().UTC(), leaseID, p.config.LeaseDuration, p.config.DatabaseIdentity, p.config.WriterEpoch)
		if err != nil {
			if !errors.Is(err, models.ErrControlPlaneHold) && !errors.Is(err, models.ErrControlPlaneDrain) {
				slog.Error("Failed to claim GitHub webhook delivery", "worker", workerIndex, "error", err)
			}
			if !p.waitForWork(pollTicker) {
				return
			}
			continue
		}
		if delivery == nil {
			if !p.waitForWork(pollTicker) {
				return
			}
			continue
		}

		p.processClaim(workerIndex, delivery, leaseID)
	}
}

func (p *GithubWebhookProcessor) waitForWork(pollTicker *time.Ticker) bool {
	select {
	case <-p.stopCh:
		return false
	case <-p.wakeCh:
		return true
	case <-pollTicker.C:
		return true
	}
}

func (p *GithubWebhookProcessor) processClaim(workerIndex int, delivery *models.GithubWebhookDelivery, leaseID string) {
	logger := slog.Default().With(
		"github_delivery_id", delivery.DeliveryID,
		"github_event_type", delivery.EventType,
		"webhook_worker", workerIndex,
	)
	ctx, cancel := context.WithCancel(logging.Inject(context.Background(), logger))
	defer cancel()

	type processOutcome struct {
		result GithubWebhookProcessingResult
		err    error
	}
	outcomeCh := make(chan processOutcome, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				outcomeCh <- processOutcome{err: fmt.Errorf("panic while processing webhook: %v\n%s", recovered, debug.Stack())}
			}
		}()
		result, err := p.process(ctx, delivery)
		outcomeCh <- processOutcome{result: result, err: err}
	}()

	leaseTicker := time.NewTicker(p.config.LeaseDuration / 3)
	defer leaseTicker.Stop()

	for {
		select {
		case outcome := <-outcomeCh:
			p.finishClaim(delivery, leaseID, outcome)
			return
		case <-leaseTicker.C:
			if err := p.store.RenewGithubWebhookDeliveryLease(context.Background(), delivery.DeliveryID, leaseID, time.Now().UTC(), p.config.LeaseDuration, p.config.DatabaseIdentity, p.config.WriterEpoch); err != nil {
				logger.Error("Failed to renew GitHub webhook delivery lease", "error", err)
				cancel()
				// Do not record an outcome after ownership is uncertain. Waiting here
				// keeps the bounded worker responsible for the handler it started.
				<-outcomeCh
				return
			}
		}
	}
}

func (p *GithubWebhookProcessor) finishClaim(delivery *models.GithubWebhookDelivery, leaseID string, outcome struct {
	result GithubWebhookProcessingResult
	err    error
}) {
	now := time.Now().UTC()
	if outcome.err == nil {
		status := outcome.result.Status
		if status != models.GithubWebhookDeliverySucceeded && status != models.GithubWebhookDeliveryIgnored {
			outcome.err = fmt.Errorf("webhook processor returned invalid terminal status %q", status)
		} else if err := p.store.CompleteGithubWebhookDelivery(context.Background(), delivery.DeliveryID, leaseID, status, outcome.result.TerminalResult, now, p.config.DatabaseIdentity, p.config.WriterEpoch); err != nil {
			slog.Error("Failed to commit GitHub webhook terminal result", "deliveryID", delivery.DeliveryID, "error", err)
			return
		} else {
			slog.Info("GitHub webhook delivery reached a terminal result", "deliveryID", delivery.DeliveryID, "status", status, "result", outcome.result.TerminalResult)
			return
		}
	}

	lastError := truncateWebhookError(outcome.err.Error())
	if delivery.AttemptCount >= p.config.MaxAttempts && now.Sub(delivery.ReceivedAt) >= p.config.RetryHorizon {
		if err := p.store.DeadLetterGithubWebhookDelivery(context.Background(), delivery.DeliveryID, leaseID, lastError, now, p.config.DatabaseIdentity, p.config.WriterEpoch); err != nil {
			slog.Error("Failed to dead-letter GitHub webhook delivery", "deliveryID", delivery.DeliveryID, "error", err)
			return
		}
		slog.Error("GitHub webhook delivery moved to dead letter", "deliveryID", delivery.DeliveryID, "attempts", delivery.AttemptCount, "error", lastError)
		return
	}

	nextAttemptAt := now.Add(p.retryDelay(delivery.AttemptCount))
	if err := p.store.RetryGithubWebhookDelivery(context.Background(), delivery.DeliveryID, leaseID, lastError, nextAttemptAt, now, p.config.DatabaseIdentity, p.config.WriterEpoch); err != nil {
		slog.Error("Failed to schedule GitHub webhook delivery retry", "deliveryID", delivery.DeliveryID, "error", err)
		return
	}
	slog.Warn("GitHub webhook delivery scheduled for retry", "deliveryID", delivery.DeliveryID, "attempts", delivery.AttemptCount, "nextAttemptAt", nextAttemptAt, "error", lastError)
	p.Wake()
}

// RequeueDeadLetter is the audited operator service for replaying a quarantined
// delivery. Authentication and authorization belong to the caller.
func (p *GithubWebhookProcessor) RequeueDeadLetter(ctx context.Context, deliveryID string, actor string, reason string) error {
	if !p.config.Enabled {
		return ErrGithubWebhookProcessorDisabled
	}
	actor = strings.TrimSpace(actor)
	reason = strings.TrimSpace(reason)
	if actor == "" || reason == "" {
		return errors.New("operator actor and requeue reason are required")
	}
	if len(actor) > 256 || len(reason) > 2048 {
		return errors.New("operator actor or requeue reason exceeds the audit limit")
	}
	if err := p.store.RequeueGithubWebhookDelivery(ctx, deliveryID, actor, reason, time.Now().UTC(), p.config.DatabaseIdentity, p.config.WriterEpoch); err != nil {
		return err
	}
	p.Wake()
	return nil
}

func (p *GithubWebhookProcessor) Ready(ctx context.Context) error {
	if !p.started.Load() {
		return errors.New("github webhook workers have not started")
	}
	if !p.config.Enabled {
		return nil
	}
	if err := p.store.CheckAuthoritativeWriter(ctx, p.config.DatabaseIdentity, p.config.WriterEpoch, false); err != nil {
		return fmt.Errorf("github webhook writer unavailable: %w", err)
	}
	p.admissionMu.Lock()
	accepting := p.accepting
	p.admissionMu.Unlock()
	if !accepting {
		return ErrGithubWebhookProcessorStopping
	}
	if p.activeWorkers.Load() != int32(p.config.Workers) {
		return fmt.Errorf("github webhook workers unavailable: active=%d expected=%d", p.activeWorkers.Load(), p.config.Workers)
	}
	if err := p.store.CheckGithubWebhookInbox(ctx); err != nil {
		return fmt.Errorf("github webhook inbox unavailable: %w", err)
	}
	return nil
}

func (p *GithubWebhookProcessor) retryDelay(attempt int64) time.Duration {
	delay := p.config.RetryBase
	for exponent := int64(1); exponent < attempt && delay < p.config.RetryMax; exponent++ {
		delay *= 2
		if delay > p.config.RetryMax {
			return p.config.RetryMax
		}
	}
	return delay
}

func truncateWebhookError(message string) string {
	const maxWebhookErrorLength = 16 * 1024
	return truncateValidUTF8(message, maxWebhookErrorLength)
}
