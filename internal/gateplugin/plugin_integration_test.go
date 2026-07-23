package gateplugin

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/robinbraemer/event"
	jconfig "go.minekube.com/gate/pkg/edition/java/config"
	"go.minekube.com/gate/pkg/edition/java/proxy"
	"go.minekube.com/gate/pkg/util/uuid"

	"github.com/RasmusLiltorp/minecraft-wake-gateway/internal/lifecycle"
)

type integrationWaker struct {
	calls atomic.Int32
}

func (w *integrationWaker) Wake(context.Context, lifecycle.Target) error {
	w.calls.Add(1)
	return nil
}

type integrationReadiness struct {
	results chan error
}

func (r *integrationReadiness) WaitReady(ctx context.Context, _ lifecycle.Target) error {
	select {
	case err := <-r.results:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type integrationServerConnection struct {
	proxy.ServerConnection
	server proxy.RegisteredServer
	player proxy.Player
}

func (c *integrationServerConnection) Server() proxy.RegisteredServer { return c.server }
func (c *integrationServerConnection) Player() proxy.Player           { return c.player }

type integrationPlayer struct {
	proxy.Player

	id       uuid.UUID
	active   atomic.Bool
	current  *integrationServerConnection
	attempts atomic.Int32
	done     chan struct{}
	once     sync.Once
}

func newIntegrationPlayer() *integrationPlayer {
	player := &integrationPlayer{id: uuid.New(), done: make(chan struct{})}
	player.active.Store(true)
	return player
}

func (p *integrationPlayer) ID() uuid.UUID    { return p.id }
func (p *integrationPlayer) Active() bool     { return p.active.Load() }
func (p *integrationPlayer) OnlineMode() bool { return true }
func (p *integrationPlayer) CurrentServer() proxy.ServerConnection {
	if p.current == nil {
		return nil
	}
	return p.current
}
func (p *integrationPlayer) CreateConnectionRequest(server proxy.RegisteredServer) proxy.ConnectionRequest {
	return &integrationConnectionRequest{player: p, server: server}
}

type integrationConnectionRequest struct {
	proxy.ConnectionRequest
	player *integrationPlayer
	server proxy.RegisteredServer
}

func (r *integrationConnectionRequest) Server() proxy.RegisteredServer { return r.server }
func (r *integrationConnectionRequest) Connect(context.Context) (proxy.ConnectionResult, error) {
	if current := r.player.CurrentServer(); current == nil ||
		current.Server().ServerInfo().Name() != "limbo" {
		return nil, errors.New("same player connection did not remain in PicoLimbo")
	}
	if r.player.attempts.Add(1) == 1 {
		return fakeConnectionResult{status: proxy.ServerDisconnectedConnectionStatus}, nil
	}
	r.player.current = &integrationServerConnection{server: r.server, player: r.player}
	r.player.once.Do(func() { close(r.player.done) })
	return fakeConnectionResult{status: proxy.SuccessConnectionStatus}, nil
}
func (r *integrationConnectionRequest) ConnectWithIndication(ctx context.Context) bool {
	result, err := r.Connect(ctx)
	return err == nil && result.Status().Successful()
}

func TestGatePluginRealEventWiringRetainsLobbyAndSwitchesSamePlayer(t *testing.T) {
	eventManager := event.New(event.WithRecoverPanic(false))
	cfg := jconfig.DefaultConfig
	cfg.Quota.Connections.Enabled = false
	cfg.Quota.Logins.Enabled = false
	gateProxy, err := proxy.New(proxy.Options{Config: &cfg, EventMgr: eventManager})
	if err != nil {
		t.Fatal(err)
	}
	limbo, err := gateProxy.Register(proxy.NewServerInfo(
		"limbo",
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 25566},
	))
	if err != nil {
		t.Fatal(err)
	}
	survival, err := gateProxy.Register(proxy.NewServerInfo(
		"survival",
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 25567},
	))
	if err != nil {
		t.Fatal(err)
	}

	waker := &integrationWaker{}
	readiness := &integrationReadiness{results: make(chan error, 2)}
	coordinator := lifecycle.NewCoordinator(waker, readiness, lifecycle.CoordinatorOptions{
		CycleTimeout:    time.Second,
		Cooldown:        time.Millisecond,
		TransferTimeout: time.Second,
		WakeRetryDelay:  func(int) time.Duration { return 0 },
		TransferRetryDelay: func(int) time.Duration {
			return time.Millisecond
		},
	})
	t.Cleanup(coordinator.Close)
	target := lifecycle.Target{Namespace: "minecraft", Name: "survival"}
	sessions := lifecycle.NewSessionStore(time.Now)
	logic := lifecycle.NewGatewayLogic(target, sessions, coordinator)
	if err := New(logic, target).Init(context.Background(), gateProxy); err != nil {
		t.Fatal(err)
	}

	player := newIntegrationPlayer()
	postLogin := &proxy.PostLoginEvent{}
	setGateEventField(postLogin, "player", proxy.Player(player))
	eventManager.Fire(postLogin)

	choose := &proxy.PlayerChooseInitialServerEvent{}
	setGateEventField(choose, "player", proxy.Player(player))
	eventManager.Fire(choose)
	if !proxy.RegisteredServerEqual(choose.InitialServer(), limbo) {
		t.Fatalf("initial server = %v, want PicoLimbo", choose.InitialServer())
	}

	player.current = &integrationServerConnection{server: limbo, player: player}
	connected := &proxy.ServerPostConnectEvent{}
	setGateEventField(connected, "player", proxy.Player(player))
	eventManager.Fire(connected)
	eventuallyIntegration(t, func() bool { return waker.calls.Load() == 1 })

	kicked := &proxy.KickedFromServerEvent{}
	setGateEventField(kicked, "player", proxy.Player(player))
	setGateEventField(kicked, "server", survival)
	eventManager.Fire(kicked)
	redirect, ok := kicked.Result().(*proxy.RedirectPlayerKickResult)
	if !ok || !proxy.RegisteredServerEqual(redirect.Server, limbo) {
		t.Fatalf("kick result = %#v, want PicoLimbo redirect", kicked.Result())
	}
	if !proxy.RegisteredServerEqual(player.CurrentServer().Server(), limbo) {
		t.Fatal("failed survival attempt displaced the existing PicoLimbo connection")
	}

	readiness.results <- nil
	eventuallyIntegration(t, func() bool { return player.attempts.Load() == 1 })
	if !proxy.RegisteredServerEqual(player.CurrentServer().Server(), limbo) {
		t.Fatal("failed transfer did not retain PicoLimbo")
	}
	readiness.results <- nil
	select {
	case <-player.done:
	case <-time.After(time.Second):
		t.Fatal("same player was not switched after backend readiness recovered")
	}
	if !proxy.RegisteredServerEqual(player.CurrentServer().Server(), survival) {
		t.Fatal("successful transfer did not switch the same player to survival")
	}

	eventManager.Fire(connected)
	status := sessions.Snapshot(target)
	if status.LobbySessions != 0 || status.BackendSessions != 1 {
		t.Fatalf("session status after switch = %#v", status)
	}
}

func setGateEventField(target any, field string, value any) {
	structValue := reflect.ValueOf(target).Elem()
	fieldValue := structValue.FieldByName(field)
	writable := reflect.NewAt(fieldValue.Type(), unsafe.Pointer(fieldValue.UnsafeAddr())).Elem()
	writable.Set(reflect.ValueOf(value))
}

func eventuallyIntegration(t *testing.T, predicate func() bool) {
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
