package lifecycle

import (
	"testing"
	"time"
)

func TestWakeStartsOnlyAfterAuthenticatedPlayerReachesLimbo(t *testing.T) {
	waker := &fakeWaker{}
	readiness := &fakeReadiness{results: make(chan error)}
	coordinator := testCoordinator(waker, readiness)
	t.Cleanup(coordinator.Close)
	sessions := NewSessionStore(time.Now)
	target := Target{Namespace: "minecraft", Name: "survival"}
	logic := NewGatewayLogic(target, sessions, coordinator)
	player := newFakeWaiter("authenticated")

	logic.PostLogin(player.ID(), false)
	logic.BackendConnected(player, "limbo")
	if got := waker.calls.Load(); got != 0 {
		t.Fatalf("offline player triggered %d wakes, want 0", got)
	}

	logic.PostLogin(player.ID(), true)
	if got := waker.calls.Load(); got != 0 {
		t.Fatalf("wake started before lobby placement: %d", got)
	}
	logic.BackendConnected(player, "limbo")
	eventually(t, func() bool { return waker.calls.Load() == 1 })
}

func TestInitialBackendUsesOnlyCachedDoubleReadiness(t *testing.T) {
	coordinator := testCoordinator(&fakeWaker{}, &fakeReadiness{results: make(chan error)})
	t.Cleanup(coordinator.Close)
	target := Target{Namespace: "minecraft", Name: "survival"}
	logic := NewGatewayLogic(target, NewSessionStore(time.Now), coordinator)

	if got := logic.InitialBackend(true); got != "limbo" {
		t.Fatalf("initial backend = %q, want limbo while not ready", got)
	}
	coordinator.SetBackendReady(target, true)
	if got := logic.InitialBackend(true); got != "survival" {
		t.Fatalf("initial backend = %q, want survival while ready", got)
	}
	if got := logic.InitialBackend(false); got != "" {
		t.Fatalf("offline player initial backend = %q, want rejection", got)
	}
}

func TestBackendTransitionAndDisconnectUpdateState(t *testing.T) {
	coordinator := testCoordinator(&fakeWaker{}, &fakeReadiness{results: make(chan error)})
	t.Cleanup(coordinator.Close)
	sessions := NewSessionStore(time.Now)
	target := Target{Namespace: "minecraft", Name: "survival"}
	logic := NewGatewayLogic(target, sessions, coordinator)
	player := newFakeWaiter("player")

	logic.PostLogin(player.ID(), true)
	logic.BackendConnected(player, "limbo")
	eventually(t, func() bool { return coordinator.Snapshot(target).LobbySessions == 1 })
	logic.BackendConnected(player, "survival")
	if got := sessions.Snapshot(target); got.LobbySessions != 0 || got.BackendSessions != 1 {
		t.Fatalf("session after transfer = %#v", got)
	}
	eventually(t, func() bool { return coordinator.Snapshot(target).LobbySessions == 0 })
	logic.Disconnect(player.ID())
	if got := sessions.Snapshot(target); got.BackendSessions != 0 {
		t.Fatalf("session after disconnect = %#v", got)
	}
}

func TestDuplicateBackendPostLoginDoesNotResetSessionOrActivity(t *testing.T) {
	coordinator := testCoordinator(&fakeWaker{}, &fakeReadiness{results: make(chan error)})
	t.Cleanup(coordinator.Close)
	now := time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC)
	sessions := NewSessionStore(func() time.Time { return now })
	target := Target{Namespace: "minecraft", Name: "survival"}
	logic := NewGatewayLogic(target, sessions, coordinator)
	player := newFakeWaiter("modern-forge-player")

	logic.PostLogin(player.ID(), true)
	now = now.Add(time.Minute)
	logic.BackendConnected(player, "survival")
	afterTransition := sessions.Snapshot(target)

	now = now.Add(time.Minute)
	logic.PostLogin(player.ID(), true)
	afterDuplicate := sessions.Snapshot(target)

	if afterDuplicate.BackendSessions != 1 || afterDuplicate.LobbySessions != 0 {
		t.Fatalf("duplicate PostLogin reset location: %#v", afterDuplicate)
	}
	if !afterDuplicate.LastActivity.Equal(afterTransition.LastActivity) {
		t.Fatalf(
			"duplicate PostLogin advanced last activity from %s to %s",
			afterTransition.LastActivity,
			afterDuplicate.LastActivity,
		)
	}
}
