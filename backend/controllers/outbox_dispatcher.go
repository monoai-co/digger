package controllers

import (
	"context"
	"encoding/json"
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

var ErrOutboxDispatcherMisconfigured = errors.New("outbox dispatcher is misconfigured")

type outboxEffectStore interface {
	ClaimNextOutboxEffect(context.Context, time.Time, string, time.Duration, string, int64) (*models.OutboxEffect, error)
	RenewOutboxEffectLease(context.Context, uuid.UUID, string, time.Time, time.Duration, string, int64) error
	CompleteOutboxEffect(context.Context, uuid.UUID, string, []byte, time.Time, string, int64) error
	RetryOutboxEffect(context.Context, uuid.UUID, string, string, time.Duration, time.Time, string, int64) error
	DeadLetterOutboxEffect(context.Context, uuid.UUID, string, string, time.Time, string, int64) error
}

type OutboxDispatchRequest struct {
	EffectID       uuid.UUID
	OperationID    string
	EffectKind     string
	EffectKey      string
	Payload        json.RawMessage
	IdempotencyKey string
}

type OutboxDispatchResult struct {
	ProviderReceipt json.RawMessage
}

// OutboxDispatchFunc must forward IdempotencyKey to a provider that supports
// idempotency, or include it in the downstream execution-claim protocol.
type OutboxDispatchFunc func(context.Context, OutboxDispatchRequest) (OutboxDispatchResult, error)

type OutboxDispatcherConfig struct {
	Enabled          bool
	DatabaseIdentity string
	WriterEpoch      int64
	Workers          int
	PollInterval     time.Duration
	LeaseDuration    time.Duration
	MaxAttempts      int64
	RetryBase        time.Duration
	RetryMax         time.Duration
}

func DefaultOutboxDispatcherConfig() OutboxDispatcherConfig {
	return OutboxDispatcherConfig{
		Enabled:       false,
		Workers:       4,
		PollInterval:  time.Second,
		LeaseDuration: 2 * time.Minute,
		MaxAttempts:   10000,
		RetryBase:     time.Second,
		RetryMax:      5 * time.Minute,
	}
}

type OutboxDispatcher struct {
	store    outboxEffectStore
	dispatch OutboxDispatchFunc
	config   OutboxDispatcherConfig

	wakeCh       chan struct{}
	stopCh       chan struct{}
	doneCh       chan struct{}
	workers      sync.WaitGroup
	claimCtx     context.Context
	cancelClaims context.CancelFunc
	lifecycleMu  sync.Mutex
	started      bool
	stopped      bool

	activeWorkers  atomic.Int32
	workerRestarts atomic.Uint64
}

func NewOutboxDispatcher(store outboxEffectStore, dispatch OutboxDispatchFunc, config OutboxDispatcherConfig) (*OutboxDispatcher, error) {
	config = normalizeOutboxDispatcherConfig(config)
	if config.Enabled {
		switch {
		case store == nil:
			return nil, fmt.Errorf("%w: store is required", ErrOutboxDispatcherMisconfigured)
		case dispatch == nil:
			return nil, fmt.Errorf("%w: dispatch handler is required", ErrOutboxDispatcherMisconfigured)
		case strings.TrimSpace(config.DatabaseIdentity) == "":
			return nil, fmt.Errorf("%w: database identity is required", ErrOutboxDispatcherMisconfigured)
		case config.WriterEpoch <= 0:
			return nil, fmt.Errorf("%w: positive writer epoch is required", ErrOutboxDispatcherMisconfigured)
		}
	}
	claimCtx, cancelClaims := context.WithCancel(context.Background())
	return &OutboxDispatcher{
		store:        store,
		dispatch:     dispatch,
		config:       config,
		wakeCh:       make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		claimCtx:     claimCtx,
		cancelClaims: cancelClaims,
	}, nil
}

func normalizeOutboxDispatcherConfig(config OutboxDispatcherConfig) OutboxDispatcherConfig {
	defaults := DefaultOutboxDispatcherConfig()
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
	return config
}

func (d *OutboxDispatcher) Enabled() bool {
	return d.config.Enabled
}

func (d *OutboxDispatcher) Start() {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.started || d.stopped {
		return
	}
	d.started = true
	if !d.config.Enabled {
		close(d.doneCh)
		return
	}
	for workerIndex := 0; workerIndex < d.config.Workers; workerIndex++ {
		d.workers.Add(1)
		go d.superviseWorker(workerIndex)
	}
	go func() {
		d.workers.Wait()
		close(d.doneCh)
	}()
	d.Wake()
}

func (d *OutboxDispatcher) Wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

func (d *OutboxDispatcher) Shutdown(ctx context.Context) error {
	d.lifecycleMu.Lock()
	if !d.started {
		if !d.stopped {
			d.stopped = true
			close(d.stopCh)
			d.cancelClaims()
			close(d.doneCh)
		}
		d.lifecycleMu.Unlock()
		return nil
	}
	if !d.stopped {
		d.stopped = true
		close(d.stopCh)
		d.cancelClaims()
	}
	d.lifecycleMu.Unlock()
	select {
	case <-d.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *OutboxDispatcher) superviseWorker(workerIndex int) {
	defer d.workers.Done()
	for {
		panicked := d.runWorker(workerIndex)
		if !panicked {
			return
		}
		d.workerRestarts.Add(1)
		slog.Error("Restarting outbox worker after panic", "worker", workerIndex)
		select {
		case <-d.stopCh:
			return
		case <-time.After(d.config.PollInterval):
		}
	}
}

func (d *OutboxDispatcher) runWorker(workerIndex int) (panicked bool) {
	d.activeWorkers.Add(1)
	defer d.activeWorkers.Add(-1)
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			slog.Error("Outbox worker panicked", "worker", workerIndex, "error", recovered, "stack", string(debug.Stack()))
		}
	}()
	d.worker(workerIndex)
	return false
}

func (d *OutboxDispatcher) worker(workerIndex int) {
	pollTicker := time.NewTicker(d.config.PollInterval)
	defer pollTicker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		leaseID := uuid.NewString()
		effect, err := d.store.ClaimNextOutboxEffect(d.claimCtx, time.Now().UTC(), leaseID, d.config.LeaseDuration, d.config.DatabaseIdentity, d.config.WriterEpoch)
		if err != nil {
			if !errors.Is(err, models.ErrControlPlaneHold) && !errors.Is(err, models.ErrControlPlaneDrain) && !errors.Is(err, context.Canceled) {
				slog.Error("Failed to claim outbox effect", "worker", workerIndex, "error", err)
			}
			if !d.waitForWork(pollTicker) {
				return
			}
			continue
		}
		if effect == nil {
			if !d.waitForWork(pollTicker) {
				return
			}
			continue
		}
		d.dispatchClaim(workerIndex, effect, leaseID)
	}
}

func (d *OutboxDispatcher) waitForWork(pollTicker *time.Ticker) bool {
	select {
	case <-d.stopCh:
		return false
	case <-d.wakeCh:
		return true
	case <-pollTicker.C:
		return true
	}
}

type outboxDispatchOutcome struct {
	result OutboxDispatchResult
	err    error
}

func (d *OutboxDispatcher) dispatchClaim(workerIndex int, effect *models.OutboxEffect, leaseID string) {
	logger := slog.Default().With(
		"outbox_effect_id", effect.ID,
		"outbox_effect_kind", effect.EffectKind,
		"outbox_worker", workerIndex,
	)
	ctx, cancel := context.WithCancel(logging.Inject(context.Background(), logger))
	defer cancel()
	outcomeCh := make(chan outboxDispatchOutcome, 1)
	request := OutboxDispatchRequest{
		EffectID:       effect.ID,
		OperationID:    effect.ControlOperationID,
		EffectKind:     effect.EffectKind,
		EffectKey:      effect.EffectKey,
		Payload:        json.RawMessage(effect.Payload),
		IdempotencyKey: "digger-outbox:" + effect.ID.String(),
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				outcomeCh <- outboxDispatchOutcome{err: fmt.Errorf("panic while dispatching outbox effect: %v\n%s", recovered, debug.Stack())}
			}
		}()
		result, err := d.dispatch(ctx, request)
		outcomeCh <- outboxDispatchOutcome{result: result, err: err}
	}()

	renewEvery := d.config.LeaseDuration / 3
	if renewEvery <= 0 {
		renewEvery = time.Nanosecond
	}
	leaseTicker := time.NewTicker(renewEvery)
	defer leaseTicker.Stop()
	for {
		select {
		case outcome := <-outcomeCh:
			d.finishClaim(effect, leaseID, outcome)
			return
		case <-leaseTicker.C:
			if err := d.renewLease(effect.ID, leaseID); err != nil {
				logger.Error("Failed to renew outbox effect lease", "error", err)
				cancel()
				<-outcomeCh
				return
			}
		}
	}
}

func (d *OutboxDispatcher) renewLease(effectID uuid.UUID, leaseID string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic while renewing outbox effect lease: %v\n%s", recovered, debug.Stack())
		}
	}()
	return d.store.RenewOutboxEffectLease(context.Background(), effectID, leaseID, time.Now().UTC(), d.config.LeaseDuration, d.config.DatabaseIdentity, d.config.WriterEpoch)
}

func (d *OutboxDispatcher) finishClaim(effect *models.OutboxEffect, leaseID string, outcome outboxDispatchOutcome) {
	now := time.Now().UTC()
	if outcome.err == nil && len(outcome.result.ProviderReceipt) > 0 && !json.Valid(outcome.result.ProviderReceipt) {
		outcome.err = errors.New("outbox provider receipt is not valid JSON")
	}
	if outcome.err == nil {
		if err := d.store.CompleteOutboxEffect(context.Background(), effect.ID, leaseID, outcome.result.ProviderReceipt, now, d.config.DatabaseIdentity, d.config.WriterEpoch); err != nil {
			slog.Error("Failed to commit outbox provider receipt", "effectID", effect.ID, "error", err)
			return
		}
		slog.Info("Outbox effect succeeded", "effectID", effect.ID, "effectKind", effect.EffectKind)
		return
	}

	lastError := truncateOutboxError(outcome.err.Error())
	if effect.AttemptCount >= d.config.MaxAttempts {
		if err := d.store.DeadLetterOutboxEffect(context.Background(), effect.ID, leaseID, lastError, now, d.config.DatabaseIdentity, d.config.WriterEpoch); err != nil {
			slog.Error("Failed to dead-letter outbox effect", "effectID", effect.ID, "error", err)
			return
		}
		slog.Error("Outbox effect moved to dead letter", "effectID", effect.ID, "attempts", effect.AttemptCount, "error", lastError)
		return
	}

	retryDelay := d.retryDelay(effect.AttemptCount)
	if err := d.store.RetryOutboxEffect(context.Background(), effect.ID, leaseID, lastError, retryDelay, now, d.config.DatabaseIdentity, d.config.WriterEpoch); err != nil {
		slog.Error("Failed to schedule outbox effect retry", "effectID", effect.ID, "error", err)
		return
	}
	slog.Warn("Outbox effect scheduled for retry", "effectID", effect.ID, "attempts", effect.AttemptCount, "retryDelay", retryDelay, "error", lastError)
	d.Wake()
}

func (d *OutboxDispatcher) retryDelay(attempt int64) time.Duration {
	delay := d.config.RetryBase
	for exponent := int64(1); exponent < attempt && delay < d.config.RetryMax; exponent++ {
		delay *= 2
		if delay > d.config.RetryMax {
			return d.config.RetryMax
		}
	}
	return delay
}

func truncateOutboxError(message string) string {
	const maxOutboxErrorLength = 16 * 1024
	return truncateValidUTF8(message, maxOutboxErrorLength)
}
