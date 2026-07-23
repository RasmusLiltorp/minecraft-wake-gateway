package lifecycle

import (
	"sync"
	"time"
)

type ConnectionEvent string

const (
	RawTCP     ConnectionEvent = "raw-tcp"
	Handshake  ConnectionEvent = "handshake"
	StatusPing ConnectionEvent = "status-ping"
	PreLogin   ConnectionEvent = "pre-login"
)

type session struct {
	target   Target
	location string
}

type SessionSnapshot struct {
	LobbySessions   int
	BackendSessions int
	LastActivity    time.Time
}

// SessionStore contains authenticated play sessions only. ObserveNonDemand is
// intentionally a no-op so protocol probes cannot become player demand.
type SessionStore struct {
	mu           sync.RWMutex
	now          func() time.Time
	sessions     map[string]session
	lastActivity map[Target]time.Time
}

func NewSessionStore(now func() time.Time) *SessionStore {
	return &SessionStore{
		now:          now,
		sessions:     make(map[string]session),
		lastActivity: make(map[Target]time.Time),
	}
}

func (s *SessionStore) ObserveNonDemand(ConnectionEvent) {}

func (s *SessionStore) Login(id string, target Target, authenticated bool) {
	if !authenticated {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[id]; exists {
		return
	}
	s.sessions[id] = session{target: target}
	s.lastActivity[target] = s.now()
}

func (s *SessionStore) SetLocation(id, location string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[id]
	if !ok {
		return
	}
	current.location = location
	s.sessions[id] = current
	s.lastActivity[current.target] = s.now()
}

func (s *SessionStore) Disconnect(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[id]
	if !ok {
		return
	}
	delete(s.sessions, id)
	s.lastActivity[current.target] = s.now()
}

func (s *SessionStore) Snapshot(target Target) SessionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := SessionSnapshot{LastActivity: s.lastActivity[target]}
	for _, current := range s.sessions {
		if current.target != target {
			continue
		}
		switch current.location {
		case "limbo":
			snapshot.LobbySessions++
		case target.Name:
			snapshot.BackendSessions++
		}
	}
	return snapshot
}
