package lifecycle

import (
	"testing"
	"time"
)

func TestSessionStoreCountsOnlyAuthenticatedPlayers(t *testing.T) {
	now := time.Date(2026, 7, 23, 18, 42, 17, 0, time.UTC)
	s := NewSessionStore(func() time.Time { return now })
	target := Target{Namespace: "minecraft", Name: "survival"}

	s.Login("offline", target, false)
	s.Login("authenticated", target, true)
	s.SetLocation("authenticated", "limbo")
	got := s.Snapshot(target)
	if got.LobbySessions != 1 || got.BackendSessions != 0 {
		t.Fatalf("sessions = lobby %d backend %d, want 1/0", got.LobbySessions, got.BackendSessions)
	}
	if !got.LastActivity.Equal(now) {
		t.Fatalf("last activity = %v, want %v", got.LastActivity, now)
	}

	now = now.Add(time.Minute)
	s.SetLocation("authenticated", "survival")
	got = s.Snapshot(target)
	if got.LobbySessions != 0 || got.BackendSessions != 1 {
		t.Fatalf("sessions = lobby %d backend %d, want 0/1", got.LobbySessions, got.BackendSessions)
	}
	if !got.LastActivity.Equal(now) {
		t.Fatalf("last activity = %v, want %v", got.LastActivity, now)
	}

	now = now.Add(time.Minute)
	s.Disconnect("authenticated")
	got = s.Snapshot(target)
	if got.LobbySessions != 0 || got.BackendSessions != 0 {
		t.Fatalf("sessions after disconnect = lobby %d backend %d, want 0/0", got.LobbySessions, got.BackendSessions)
	}
	if !got.LastActivity.Equal(now) {
		t.Fatalf("disconnect did not advance last activity")
	}
}

func TestNonDemandEventsDoNotCreateSessions(t *testing.T) {
	s := NewSessionStore(time.Now)
	target := Target{Namespace: "minecraft", Name: "survival"}
	for _, event := range []ConnectionEvent{RawTCP, Handshake, StatusPing, PreLogin} {
		s.ObserveNonDemand(event)
	}
	got := s.Snapshot(target)
	if got.LobbySessions != 0 || got.BackendSessions != 0 || !got.LastActivity.IsZero() {
		t.Fatalf("non-demand events changed sessions: %#v", got)
	}
}
