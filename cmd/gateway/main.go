package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.minekube.com/gate/pkg/edition/java/config"
	"go.minekube.com/gate/pkg/edition/java/proxy"
	"go.minekube.com/gate/pkg/gate"
	gateconfig "go.minekube.com/gate/pkg/gate/config"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/RasmusLiltorp/minecraft-wake-gateway/internal/gateplugin"
	"github.com/RasmusLiltorp/minecraft-wake-gateway/internal/lifecycle"
)

var target = lifecycle.Target{Namespace: "minecraft", Name: "survival"}

type readinessChecker interface {
	Check(context.Context, lifecycle.Target) (bool, error)
}

func main() {
	if err := run(); err != nil {
		log.Printf("minecraft gateway exited: %v", err)
		os.Exit(1)
	}
}

func run() error {
	forwardingSecret := os.Getenv("FORWARDING_SECRET")
	statusToken := os.Getenv("STATUS_TOKEN")
	if forwardingSecret == "" || statusToken == "" {
		return errors.New("FORWARDING_SECRET and STATUS_TOKEN must be set")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("create in-cluster config: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes clientset: %w", err)
	}

	readiness := &lifecycle.KubernetesReadiness{Dynamic: dynamicClient, Client: clientset}
	coordinator := lifecycle.NewCoordinator(
		lifecycle.KubernetesWaker{Dynamic: dynamicClient},
		readiness,
		lifecycle.CoordinatorOptions{
			CycleTimeout:    180 * time.Second,
			Cooldown:        30 * time.Second,
			TransferTimeout: 10 * time.Second,
		},
	)
	defer coordinator.Close()
	sessions := lifecycle.NewSessionStore(time.Now)
	logic := lifecycle.NewGatewayLogic(target, sessions, coordinator)

	initialReadDone := make(chan struct{})
	go func() {
		defer close(initialReadDone)
		_ = retryInitialRead(ctx, readiness, target, func(ready bool) {
			coordinator.SetBackendReady(target, ready)
		}, initialReadRetryDelay)
	}()
	defer func() {
		cancel()
		<-initialReadDone
	}()

	proxy.Plugins = append(proxy.Plugins, gateplugin.New(logic, target))
	cfg := gateConfig(forwardingSecret)
	status := lifecycle.NewStatusHandler(
		statusToken,
		target,
		sessions,
		coordinator,
		func() bool {
			return readiness.Initialized() &&
				tcpReachable("127.0.0.1:25565") &&
				tcpReachable("picolimbo.minecraft.svc.cluster.local:25565")
		},
	)
	httpServer := &http.Server{
		Addr:              "0.0.0.0:8080",
		Handler:           status,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		errs <- httpServer.ListenAndServe()
	}()
	go func() {
		errs <- gate.Start(ctx, gate.WithConfig(cfg), gate.WithAutoShutdownOnSignal(false))
	}()

	err = <-errs
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func retryInitialRead(
	ctx context.Context,
	checker readinessChecker,
	target lifecycle.Target,
	setReady func(bool),
	retryDelay func(attempt int) time.Duration,
) error {
	for attempt := 0; ; attempt++ {
		checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
		ready, err := checker.Check(checkCtx, target)
		checkCancel()
		if err == nil {
			setReady(ready)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(retryDelay(attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func initialReadRetryDelay(attempt int) time.Duration {
	delay := time.Second << min(attempt, 3)
	return min(delay, 10*time.Second)
}

func gateConfig(forwardingSecret string) gateconfig.Config {
	cfg := gateconfig.DefaultConfig
	cfg.Config.Bind = "0.0.0.0:25565"
	cfg.Config.OnlineMode = true
	cfg.Config.Forwarding.Mode = config.VelocityForwardingMode
	cfg.Config.Forwarding.VelocitySecret = forwardingSecret
	cfg.Config.Servers = map[string]string{
		"limbo":    "picolimbo.minecraft.svc.cluster.local:25565",
		"survival": "survival.minecraft.svc.cluster.local:25565",
	}
	cfg.Config.Try = []string{"limbo"}
	cfg.Config.ConnectionTimeout = 10_000
	cfg.Config.BuiltinCommands = false
	cfg.API.Enabled = false
	cfg.HealthService.Enabled = false
	cfg.NoAutoReload = true
	return cfg
}

func tcpReachable(address string) bool {
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
