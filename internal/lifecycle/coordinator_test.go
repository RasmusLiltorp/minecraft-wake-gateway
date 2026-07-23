package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWaker struct {
	calls    atomic.Int32
	failures atomic.Int32
}

func (w *fakeWaker) Wake(context.Context, Target) error {
	w.calls.Add(1)
	if w.failures.Add(-1) >= 0 {
		return errors.New("transient patch failure")
	}
	return nil
}

type fakeReadiness struct {
	results chan error
}

func (r *fakeReadiness) WaitReady(ctx context.Context, _ Target) error {
	select {
	case err := <-r.results:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeWaiter struct {
	id       string
	active   atomic.Bool
	fail     atomic.Bool
	attempts atomic.Int32
	done     chan struct{}
	once     sync.Once
}

type cancelAwareWaiter struct {
	id       string
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

type retryingWaiter struct {
	id        string
	failures  atomic.Int32
	attempts  atomic.Int32
	completed chan struct{}
	once      sync.Once
}

func newRetryingWaiter(id string, failures int32) *retryingWaiter {
	waiter := &retryingWaiter{id: id, completed: make(chan struct{})}
	waiter.failures.Store(failures)
	return waiter
}

func (w *retryingWaiter) ID() string   { return w.id }
func (w *retryingWaiter) Active() bool { return true }
func (w *retryingWaiter) Transfer(context.Context) error {
	w.attempts.Add(1)
	if w.failures.Add(-1) >= 0 {
		return errors.New("backend switch failed")
	}
	w.once.Do(func() { close(w.completed) })
	return nil
}

func newCancelAwareWaiter(id string) *cancelAwareWaiter {
	return &cancelAwareWaiter{
		id:       id,
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
}

func (w *cancelAwareWaiter) ID() string   { return w.id }
func (w *cancelAwareWaiter) Active() bool { return true }
func (w *cancelAwareWaiter) Transfer(ctx context.Context) error {
	w.once.Do(func() { close(w.started) })
	<-ctx.Done()
	close(w.canceled)
	return ctx.Err()
}

func newFakeWaiter(id string) *fakeWaiter {
	w := &fakeWaiter{id: id, done: make(chan struct{})}
	w.active.Store(true)
	return w
}

func (w *fakeWaiter) ID() string   { return w.id }
func (w *fakeWaiter) Active() bool { return w.active.Load() }
func (w *fakeWaiter) Transfer(context.Context) error {
	w.attempts.Add(1)
	if w.fail.Load() {
		return errors.New("backend unavailable")
	}
	w.once.Do(func() { close(w.done) })
	return nil
}

func testCoordinator(w Waker, r ReadinessSource) *Coordinator {
	return NewCoordinator(w, r, CoordinatorOptions{
		CycleTimeout:    time.Second,
		Cooldown:        time.Millisecond,
		TransferTimeout: time.Second,
		TransferRetryDelay: func(int) time.Duration {
			return time.Millisecond
		},
	})
}

func TestConcurrentWaitersShareWakeAndWatcher(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error)}
	c := testCoordinator(waker, readiness)
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Add(target, newFakeWaiter(time.Now().String()))
		}()
	}
	wg.Wait()

	eventually(t, func() bool { return waker.calls.Load() == 1 })
	if got := c.Snapshot(target).LobbySessions; got != 20 {
		t.Fatalf("lobby sessions = %d, want 20", got)
	}
	if got := c.ActiveWatchers(); got != 1 {
		t.Fatalf("active watchers = %d, want 1", got)
	}
}

func TestWakeRetriesTransientFailureBeforeWatchingReadiness(t *testing.T) {
	waker := &fakeWaker{}
	waker.failures.Store(1)
	readiness := &fakeReadiness{results: make(chan error, 1)}
	readiness.results <- nil
	c := NewCoordinator(waker, readiness, CoordinatorOptions{
		CycleTimeout:    time.Second,
		Cooldown:        time.Millisecond,
		TransferTimeout: time.Second,
		WakeRetryDelay:  func(int) time.Duration { return 0 },
		TransferRetryDelay: func(int) time.Duration {
			return time.Millisecond
		},
	})
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	waiter := newFakeWaiter("retry")

	c.Add(target, waiter)

	select {
	case <-waiter.done:
	case <-time.After(time.Second):
		t.Fatal("waiter was not transferred after transient wake failure")
	}
	if got := waker.calls.Load(); got != 2 {
		t.Fatalf("wake calls = %d, want failed attempt plus successful retry", got)
	}
}

func TestJitteredExponentialWakeDelayIsBounded(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		max  = 800 * time.Millisecond
	)
	for attempt := 0; attempt < 10; attempt++ {
		got := jitteredExponentialDelay(base, max, attempt)
		ceiling := base << min(attempt, 3)
		if got < ceiling/2 || got > ceiling {
			t.Fatalf("attempt %d delay = %s, want [%s,%s]", attempt, got, ceiling/2, ceiling)
		}
	}
}

func TestFailedTransferKeepsWaiterAndSuccessfulPeerLeaves(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error, 4)}
	c := testCoordinator(waker, readiness)
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	failing := newFakeWaiter("failing")
	failing.fail.Store(true)
	success := newFakeWaiter("success")

	c.Add(target, failing)
	c.Add(target, success)
	readiness.results <- nil

	select {
	case <-success.done:
	case <-time.After(time.Second):
		t.Fatal("successful waiter was not transferred")
	}
	eventually(t, func() bool { return c.Snapshot(target).LobbySessions == 1 })
	if failing.attempts.Load() == 0 {
		t.Fatal("failed waiter was not attempted")
	}

	failing.fail.Store(false)
	readiness.results <- nil
	select {
	case <-failing.done:
	case <-time.After(time.Second):
		t.Fatal("retained waiter was not retried")
	}
	eventually(t, func() bool { return c.Snapshot(target).LobbySessions == 0 })
}

func TestRemovingWaiterCancelsOnlyItsTransferWhilePeerContinues(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error, 1)}
	readiness.results <- nil
	c := testCoordinator(waker, readiness)
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	removed := newCancelAwareWaiter("removed")
	peer := newFakeWaiter("peer")

	c.Add(target, removed)
	c.Add(target, peer)

	select {
	case <-removed.started:
	case <-time.After(time.Second):
		t.Fatal("removed waiter's transfer did not start")
	}
	c.Remove(target, removed.ID())

	select {
	case <-removed.canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("removing one waiter did not cancel its in-flight transfer")
	}
	select {
	case <-peer.done:
	case <-time.After(time.Second):
		t.Fatal("canceling one waiter prevented its peer from transferring")
	}
	eventually(t, func() bool { return c.Snapshot(target).LobbySessions == 0 })
}

func TestFailedWaiterUsesIndependentExponentialScheduleWhilePeerContinues(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error, 8)}
	for i := 0; i < cap(readiness.results); i++ {
		readiness.results <- nil
	}
	now := time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	var delayAttempts []int
	var waits []time.Duration
	c := NewCoordinator(waker, readiness, CoordinatorOptions{
		CycleTimeout:    time.Second,
		Cooldown:        time.Millisecond,
		TransferTimeout: time.Second,
		WakeRetryDelay:  func(int) time.Duration { return 0 },
		TransferRetryDelay: func(attempt int) time.Duration {
			clockMu.Lock()
			defer clockMu.Unlock()
			delayAttempts = append(delayAttempts, attempt)
			return time.Second << attempt
		},
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		},
		RetryWait: func(_ context.Context, delay time.Duration, _ <-chan struct{}) bool {
			clockMu.Lock()
			defer clockMu.Unlock()
			waits = append(waits, delay)
			now = now.Add(delay)
			return true
		},
	})
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	failing := newRetryingWaiter("failing", 3)
	peer := newFakeWaiter("peer")

	c.Add(target, failing)
	c.Add(target, peer)

	select {
	case <-peer.done:
	case <-time.After(time.Second):
		t.Fatal("failing waiter's backoff blocked its peer")
	}
	select {
	case <-failing.completed:
	case <-time.After(time.Second):
		t.Fatal("failing waiter did not complete after scheduled retries")
	}

	clockMu.Lock()
	defer clockMu.Unlock()
	if got, want := delayAttempts, []int{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delay attempts = %v, want %v", got, want)
	}
	if got, want := waits, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry waits = %v, want %v", got, want)
	}
}

func TestTransferRetryScheduleResetsAfterSuccessfulRemoval(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error, 6)}
	for i := 0; i < cap(readiness.results); i++ {
		readiness.results <- nil
	}
	var mu sync.Mutex
	var delayAttempts []int
	c := NewCoordinator(waker, readiness, CoordinatorOptions{
		CycleTimeout:    time.Second,
		Cooldown:        time.Millisecond,
		TransferTimeout: time.Second,
		WakeRetryDelay:  func(int) time.Duration { return 0 },
		TransferRetryDelay: func(attempt int) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			delayAttempts = append(delayAttempts, attempt)
			return 0
		},
		RetryWait: func(context.Context, time.Duration, <-chan struct{}) bool { return true },
	})
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}

	first := newRetryingWaiter("same-player", 1)
	c.Add(target, first)
	select {
	case <-first.completed:
	case <-time.After(time.Second):
		t.Fatal("first connection did not complete")
	}
	eventually(t, func() bool { return c.ActiveWatchers() == 0 })

	second := newRetryingWaiter("same-player", 1)
	c.Add(target, second)
	select {
	case <-second.completed:
	case <-time.After(time.Second):
		t.Fatal("second connection did not complete")
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := delayAttempts, []int{0, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delay attempts after removal/re-add = %v, want %v", got, want)
	}
}

func TestDefaultTransferRetryDelayIsJitteredAndCapped(t *testing.T) {
	for attempt := 0; attempt < 10; attempt++ {
		got := defaultTransferRetryDelay(attempt)
		ceiling := time.Second << min(attempt, 5)
		if ceiling > 30*time.Second {
			ceiling = 30 * time.Second
		}
		if got < ceiling/2 || got > ceiling {
			t.Fatalf("attempt %d delay = %s, want [%s,%s]", attempt, got, ceiling/2, ceiling)
		}
	}
}

func TestRemovingLastWaiterCancelsWatcher(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error)}
	c := testCoordinator(waker, readiness)
	t.Cleanup(c.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	waiter := newFakeWaiter("leaving")

	c.Add(target, waiter)
	eventually(t, func() bool { return c.ActiveWatchers() == 1 })
	c.Remove(target, waiter.ID())
	eventually(t, func() bool { return c.ActiveWatchers() == 0 })
	if got := waker.calls.Load(); got > 1 {
		t.Fatalf("wake calls = %d, want at most one before cancellation", got)
	}
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
