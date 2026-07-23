package lifecycle

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const StatusAPIVersion = "gateway.platform.liltorp.dev/v1alpha1"

type TargetStatus struct {
	APIVersion      string     `json:"apiVersion"`
	Target          string     `json:"target"`
	LobbySessions   int        `json:"lobbySessions"`
	BackendSessions int        `json:"backendSessions"`
	LastActivity    *time.Time `json:"lastActivity,omitempty"`
	WakeInFlight    bool       `json:"wakeInFlight"`
	BackendReady    bool       `json:"backendReady"`
	LastWakeError   string     `json:"lastWakeError"`
}

type statusHandler struct {
	token       []byte
	target      Target
	sessions    *SessionStore
	coordinator *Coordinator
	ready       func() bool
}

func NewStatusHandler(
	token string,
	target Target,
	sessions *SessionStore,
	coordinator *Coordinator,
	ready func() bool,
) http.Handler {
	return &statusHandler{
		token:       []byte(token),
		target:      target,
		sessions:    sessions,
		coordinator: coordinator,
		ready:       ready,
	}
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
	case "/readyz":
		if !h.ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	case "/v1/targets/" + h.target.String():
		if !h.authorized(r.Header.Get("Authorization")) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.writeStatus(w)
	default:
		http.NotFound(w, r)
	}
}

func (h *statusHandler) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) || len(h.token) == 0 {
		return false
	}
	provided := []byte(strings.TrimPrefix(header, prefix))
	if len(provided) != len(h.token) {
		return false
	}
	return subtle.ConstantTimeCompare(provided, h.token) == 1
}

func (h *statusHandler) writeStatus(w http.ResponseWriter) {
	sessions := h.sessions.Snapshot(h.target)
	coordination := h.coordinator.Snapshot(h.target)
	status := TargetStatus{
		APIVersion:      StatusAPIVersion,
		Target:          h.target.String(),
		LobbySessions:   sessions.LobbySessions,
		BackendSessions: sessions.BackendSessions,
		WakeInFlight:    coordination.WakeInFlight,
		BackendReady:    coordination.BackendReady,
		LastWakeError:   coordination.LastWakeError,
	}
	if !sessions.LastActivity.IsZero() {
		lastActivity := sessions.LastActivity.UTC()
		status.LastActivity = &lastActivity
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}
