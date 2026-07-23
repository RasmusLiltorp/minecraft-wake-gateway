package lifecycle

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Target identifies the one MinecraftServer handled by the proof-of-concept.
type Target struct {
	Namespace string
	Name      string
}

func (t Target) String() string { return t.Namespace + "/" + t.Name }

// Waiter is an authenticated player currently connected to the lobby.
type Waiter interface {
	ID() string
	Active() bool
	Transfer(context.Context) error
}

// Waker requests one replica without owning any suspend behavior.
type Waker interface {
	Wake(context.Context, Target) error
}

// ReadinessSource blocks until both the CR and its endpoint are ready.
type ReadinessSource interface {
	WaitReady(context.Context, Target) error
}

type CoordinatorOptions struct {
	CycleTimeout       time.Duration
	Cooldown           time.Duration
	TransferTimeout    time.Duration
	WakeRetryDelay     func(attempt int) time.Duration
	TransferRetryDelay func(attempt int) time.Duration
	Now                func() time.Time
	RetryWait          func(context.Context, time.Duration, <-chan struct{}) bool
}

type targetState struct {
	waiters      map[string]Waiter
	transfers    map[string]*inFlightTransfer
	retries      map[string]transferRetry
	changed      chan struct{}
	cancel       context.CancelFunc
	running      bool
	backendReady bool
	lastError    string
}

type inFlightTransfer struct {
	cancel context.CancelFunc
}

type transferRetry struct {
	failures  int
	notBefore time.Time
}

type CoordinatorSnapshot struct {
	LobbySessions int
	WakeInFlight  bool
	BackendReady  bool
	LastWakeError string
}

// Coordinator deduplicates wake/watch work per target and retains failed
// transfers in the lobby for a later readiness cycle.
type Coordinator struct {
	ctx       context.Context
	cancel    context.CancelFunc
	waker     Waker
	readiness ReadinessSource
	options   CoordinatorOptions

	mu      sync.Mutex
	targets map[Target]*targetState
	wg      sync.WaitGroup
	active  atomic.Int32
}

func NewCoordinator(waker Waker, readiness ReadinessSource, options CoordinatorOptions) *Coordinator {
	if options.WakeRetryDelay == nil {
		options.WakeRetryDelay = func(attempt int) time.Duration {
			return jitteredExponentialDelay(250*time.Millisecond, 10*time.Second, attempt)
		}
	}
	if options.TransferRetryDelay == nil {
		options.TransferRetryDelay = defaultTransferRetryDelay
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.RetryWait == nil {
		options.RetryWait = waitContextOrChange
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		ctx:       ctx,
		cancel:    cancel,
		waker:     waker,
		readiness: readiness,
		options:   options,
		targets:   make(map[Target]*targetState),
	}
}

func (c *Coordinator) Add(target Target, waiter Waiter) {
	c.mu.Lock()
	state := c.targets[target]
	if state == nil {
		state = newTargetState()
		c.targets[target] = state
	}
	_, existed := state.waiters[waiter.ID()]
	state.waiters[waiter.ID()] = waiter
	if state.running {
		if !existed {
			notifyState(state)
		}
		c.mu.Unlock()
		return
	}
	watchCtx, cancel := context.WithCancel(c.ctx)
	state.cancel = cancel
	state.running = true
	c.wg.Add(1)
	c.active.Add(1)
	c.mu.Unlock()

	go c.run(watchCtx, target, state)
}

func (c *Coordinator) Remove(target Target, id string) {
	c.mu.Lock()
	state := c.targets[target]
	if state == nil {
		c.mu.Unlock()
		return
	}
	delete(state.waiters, id)
	delete(state.retries, id)
	notifyState(state)
	var transferCancel context.CancelFunc
	if transfer := state.transfers[id]; transfer != nil {
		transferCancel = transfer.cancel
		delete(state.transfers, id)
	}
	var targetCancel context.CancelFunc
	if len(state.waiters) == 0 && state.cancel != nil {
		targetCancel = state.cancel
	}
	c.mu.Unlock()
	if transferCancel != nil {
		transferCancel()
	}
	if targetCancel != nil {
		targetCancel()
	}
}

func (c *Coordinator) Complete(target Target, id string) {
	c.Remove(target, id)
}

func (c *Coordinator) SetBackendReady(target Target, ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.targets[target]
	if state == nil {
		state = newTargetState()
		c.targets[target] = state
	}
	state.backendReady = ready
}

func (c *Coordinator) BackendReady(target Target) bool {
	return c.Snapshot(target).BackendReady
}

func (c *Coordinator) Snapshot(target Target) CoordinatorSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.targets[target]
	if state == nil {
		return CoordinatorSnapshot{}
	}
	return CoordinatorSnapshot{
		LobbySessions: len(state.waiters),
		WakeInFlight:  state.running && len(state.waiters) != 0,
		BackendReady:  state.backendReady,
		LastWakeError: state.lastError,
	}
}

func (c *Coordinator) ActiveWatchers() int {
	return int(c.active.Load())
}

func (c *Coordinator) Close() {
	c.cancel()
	c.wg.Wait()
}

func (c *Coordinator) run(ctx context.Context, target Target, state *targetState) {
	defer func() {
		c.active.Add(-1)
		c.wg.Done()
		c.mu.Lock()
		state.running = false
		state.cancel = nil
		if len(state.waiters) == 0 {
			delete(c.targets, target)
		}
		c.mu.Unlock()
	}()

	for c.hasWaiters(state) {
		cycleCtx, cancel := context.WithTimeout(ctx, c.options.CycleTimeout)
		err := c.wakeUntilRequested(cycleCtx, target, state)
		if err == nil {
			err = c.readiness.WaitReady(cycleCtx, target)
		}
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return
			}
			c.recordError(state, err)
			if !sleepContext(ctx, c.options.Cooldown) {
				return
			}
			continue
		}

		waiters := c.eligibleWaiters(state)

		for _, waiter := range waiters {
			if !waiter.Active() {
				c.Remove(target, waiter.ID())
				continue
			}
			transferCtx, transfer, ok := c.beginTransfer(ctx, state, waiter.ID())
			if !ok {
				continue
			}
			err = waiter.Transfer(transferCtx)
			stillWaiting := c.endTransfer(state, waiter.ID(), transfer)
			if err != nil {
				if stillWaiting {
					c.recordTransferFailure(state, waiter.ID(), err)
				}
				continue
			}
			c.Complete(target, waiter.ID())
		}
		if c.hasWaiters(state) && !c.waitForEligibleRetry(ctx, state) {
			return
		}
	}
}

func newTargetState() *targetState {
	return &targetState{
		waiters:   make(map[string]Waiter),
		transfers: make(map[string]*inFlightTransfer),
		retries:   make(map[string]transferRetry),
		changed:   make(chan struct{}, 1),
	}
}

func notifyState(state *targetState) {
	select {
	case state.changed <- struct{}{}:
	default:
	}
}

func (c *Coordinator) eligibleWaiters(state *targetState) []Waiter {
	now := c.options.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	state.backendReady = true
	state.lastError = ""
	waiters := make([]Waiter, 0, len(state.waiters))
	for id, waiter := range state.waiters {
		retry, failed := state.retries[id]
		if !failed || !now.Before(retry.notBefore) {
			waiters = append(waiters, waiter)
		}
	}
	return waiters
}

func (c *Coordinator) beginTransfer(
	ctx context.Context,
	state *targetState,
	id string,
) (context.Context, *inFlightTransfer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, waiting := state.waiters[id]; !waiting {
		return nil, nil, false
	}
	transferCtx, cancel := context.WithTimeout(ctx, c.options.TransferTimeout)
	transfer := &inFlightTransfer{cancel: cancel}
	state.transfers[id] = transfer
	return transferCtx, transfer, true
}

func (c *Coordinator) endTransfer(state *targetState, id string, transfer *inFlightTransfer) bool {
	transfer.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if state.transfers[id] == transfer {
		delete(state.transfers, id)
	}
	_, stillWaiting := state.waiters[id]
	return stillWaiting
}

func (c *Coordinator) recordTransferFailure(state *targetState, id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, waiting := state.waiters[id]; !waiting {
		return
	}
	retry := state.retries[id]
	retry.notBefore = c.options.Now().Add(c.options.TransferRetryDelay(retry.failures))
	retry.failures++
	state.retries[id] = retry
	state.backendReady = false
	state.lastError = err.Error()
}

func (c *Coordinator) waitForEligibleRetry(ctx context.Context, state *targetState) bool {
	c.mu.Lock()
	select {
	case <-state.changed:
	default:
	}
	now := c.options.Now()
	var wait time.Duration
	haveWait := false
	for id := range state.waiters {
		retry, failed := state.retries[id]
		if !failed || !now.Before(retry.notBefore) {
			c.mu.Unlock()
			return true
		}
		delay := retry.notBefore.Sub(now)
		if !haveWait || delay < wait {
			wait = delay
			haveWait = true
		}
	}
	changed := state.changed
	c.mu.Unlock()
	if !haveWait {
		return true
	}
	return c.options.RetryWait(ctx, wait, changed)
}

func (c *Coordinator) wakeUntilRequested(ctx context.Context, target Target, state *targetState) error {
	for attempt := 0; ; attempt++ {
		err := c.waker.Wake(ctx, target)
		if err == nil {
			c.clearError(state)
			return nil
		}
		c.recordError(state, err)
		if !sleepContext(ctx, c.options.WakeRetryDelay(attempt)) {
			return err
		}
	}
}

func (c *Coordinator) hasWaiters(state *targetState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(state.waiters) != 0
}

func (c *Coordinator) recordError(state *targetState, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state.backendReady = false
	state.lastError = err.Error()
}

func (c *Coordinator) clearError(state *targetState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state.lastError = ""
}

func jitteredExponentialDelay(base, maximum time.Duration, attempt int) time.Duration {
	ceiling := base
	for i := 0; i < attempt && ceiling < maximum; i++ {
		if ceiling > maximum/2 {
			ceiling = maximum
			break
		}
		ceiling *= 2
	}
	if ceiling > maximum {
		ceiling = maximum
	}
	half := ceiling / 2
	return half + time.Duration(rand.Int64N(int64(ceiling-half)+1))
}

func defaultTransferRetryDelay(attempt int) time.Duration {
	return jitteredExponentialDelay(time.Second, 30*time.Second, attempt)
}

func waitContextOrChange(ctx context.Context, duration time.Duration, changed <-chan struct{}) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-changed:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
