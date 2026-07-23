package lifecycle

import "sync"

// GatewayLogic keeps Gate event ordering separate from the coordinator. A
// successful login records activity, but wake work starts only after Gate has
// established the real PicoLimbo play session.
type GatewayLogic struct {
	target      Target
	sessions    *SessionStore
	coordinator *Coordinator

	mu            sync.RWMutex
	authenticated map[string]struct{}
}

func NewGatewayLogic(target Target, sessions *SessionStore, coordinator *Coordinator) *GatewayLogic {
	return &GatewayLogic{
		target:        target,
		sessions:      sessions,
		coordinator:   coordinator,
		authenticated: make(map[string]struct{}),
	}
}

func (g *GatewayLogic) PostLogin(id string, authenticated bool) {
	g.sessions.Login(id, g.target, authenticated)
	if !authenticated {
		return
	}
	g.mu.Lock()
	g.authenticated[id] = struct{}{}
	g.mu.Unlock()
}

func (g *GatewayLogic) InitialBackend(authenticated bool) string {
	if !authenticated {
		return ""
	}
	if g.coordinator.BackendReady(g.target) {
		return g.target.Name
	}
	return "limbo"
}

func (g *GatewayLogic) BackendConnected(waiter Waiter, backend string) {
	if !g.isAuthenticated(waiter.ID()) {
		return
	}
	g.sessions.SetLocation(waiter.ID(), backend)
	if backend == "limbo" {
		g.coordinator.Add(g.target, waiter)
		return
	}
	if backend == g.target.Name {
		g.coordinator.Complete(g.target, waiter.ID())
	}
}

func (g *GatewayLogic) Disconnect(id string) {
	g.mu.Lock()
	delete(g.authenticated, id)
	g.mu.Unlock()
	g.coordinator.Remove(g.target, id)
	g.sessions.Disconnect(id)
}

func (g *GatewayLogic) isAuthenticated(id string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.authenticated[id]
	return ok
}
