package gateplugin

import (
	"context"
	"errors"
	"fmt"

	"github.com/robinbraemer/event"
	"go.minekube.com/common/minecraft/component"
	"go.minekube.com/gate/pkg/edition/java/proxy"

	"github.com/RasmusLiltorp/minecraft-wake-gateway/internal/lifecycle"
)

// New returns the in-process Gate plugin. It deliberately subscribes only to
// authenticated/play-state events: no connection, handshake, ping, or pre-login
// event has a path to the wake coordinator.
func New(logic *lifecycle.GatewayLogic, target lifecycle.Target) proxy.Plugin {
	return proxy.Plugin{
		Name: "minecraft-wake",
		Init: func(_ context.Context, gateProxy *proxy.Proxy) error {
			limbo := gateProxy.Server("limbo")
			backend := gateProxy.Server(target.Name)
			if limbo == nil || backend == nil {
				return errors.New("limbo and survival backends must be registered")
			}

			event.Subscribe(gateProxy.Event(), 0, func(e *proxy.PostLoginEvent) {
				player := e.Player()
				logic.PostLogin(player.ID().String(), player.OnlineMode())
			})
			event.Subscribe(gateProxy.Event(), 0, func(e *proxy.PlayerChooseInitialServerEvent) {
				switch logic.InitialBackend(e.Player().OnlineMode()) {
				case "limbo":
					e.SetInitialServer(limbo)
				case target.Name:
					e.SetInitialServer(backend)
				default:
					e.SetInitialServer(nil)
				}
			})
			event.Subscribe(gateProxy.Event(), 0, func(e *proxy.ServerPostConnectEvent) {
				player := e.Player()
				current := player.CurrentServer()
				if current == nil {
					return
				}
				name := current.Server().ServerInfo().Name()
				logic.BackendConnected(&gateWaiter{player: player, backend: backend}, name)
			})
			event.Subscribe(gateProxy.Event(), 0, func(e *proxy.KickedFromServerEvent) {
				if !e.Player().OnlineMode() ||
					e.Server().ServerInfo().Name() != target.Name {
					return
				}
				e.SetResult(&proxy.RedirectPlayerKickResult{
					Server:  limbo,
					Message: &component.Text{Content: "Server is starting; waiting in the lobby."},
				})
			})
			event.Subscribe(gateProxy.Event(), 0, func(e *proxy.DisconnectEvent) {
				logic.Disconnect(e.Player().ID().String())
			})
			return nil
		},
	}
}

type gateWaiter struct {
	player  proxy.Player
	backend proxy.RegisteredServer
}

func (w *gateWaiter) ID() string   { return w.player.ID().String() }
func (w *gateWaiter) Active() bool { return w.player.Active() }
func (w *gateWaiter) Transfer(ctx context.Context) error {
	result, err := w.player.CreateConnectionRequest(w.backend).Connect(ctx)
	return transferResult(result, err)
}

func transferResult(result proxy.ConnectionResult, err error) error {
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("Gate returned no connection result")
	}
	if result.Status().Successful() {
		return nil
	}
	return fmt.Errorf("Gate connection status %d", result.Status())
}
