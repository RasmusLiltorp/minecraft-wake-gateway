package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	jconfig "go.minekube.com/gate/pkg/edition/java/config"

	"github.com/RasmusLiltorp/minecraft-wake-gateway/internal/lifecycle"
)

type checkResult struct {
	ready bool
	err   error
}

type fakeInitialReadChecker struct {
	calls   atomic.Int32
	results chan checkResult
}

func (f *fakeInitialReadChecker) Check(ctx context.Context, _ lifecycle.Target) (bool, error) {
	f.calls.Add(1)
	select {
	case result := <-f.results:
		return result.ready, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestGateConfigEnforcesOnlineAuthAndVelocityForwarding(t *testing.T) {
	cfg := gateConfig("forwarding-secret")
	if !cfg.Config.OnlineMode {
		t.Fatal("Gate online mode is disabled")
	}
	if cfg.Config.Forwarding.Mode != jconfig.VelocityForwardingMode {
		t.Fatalf("forwarding mode = %q, want velocity", cfg.Config.Forwarding.Mode)
	}
	if cfg.Config.Forwarding.VelocitySecret != "forwarding-secret" {
		t.Fatal("forwarding secret was not configured")
	}
	if cfg.Config.Servers["limbo"] != "picolimbo.minecraft.svc.cluster.local:25565" ||
		cfg.Config.Servers["survival"] != "survival.minecraft.svc.cluster.local:25565" {
		t.Fatalf("unexpected backends: %#v", cfg.Config.Servers)
	}
	if cfg.Config.Bind != "0.0.0.0:25565" {
		t.Fatalf("bind = %q", cfg.Config.Bind)
	}
}

func TestInitialReadRetriesUntilKubernetesRecovers(t *testing.T) {
	checker := &fakeInitialReadChecker{results: make(chan checkResult, 3)}
	checker.results <- checkResult{err: errors.New("API unavailable")}
	checker.results <- checkResult{err: errors.New("API unavailable")}
	checker.results <- checkResult{ready: true}
	var gotReady atomic.Bool

	err := retryInitialRead(
		context.Background(),
		checker,
		target,
		gotReady.Store,
		func(int) time.Duration { return 0 },
	)

	if err != nil {
		t.Fatal(err)
	}
	if calls := checker.calls.Load(); calls != 3 {
		t.Fatalf("checks = %d, want 3", calls)
	}
	if !gotReady.Load() {
		t.Fatal("successful retry did not publish backend readiness")
	}
}

func TestInitialReadRetryStopsOnShutdown(t *testing.T) {
	checker := &fakeInitialReadChecker{results: make(chan checkResult)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := retryInitialRead(
		ctx,
		checker,
		target,
		func(bool) { t.Fatal("canceled check published readiness") },
		func(int) time.Duration { return 0 },
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if calls := checker.calls.Load(); calls != 1 {
		t.Fatalf("checks = %d, want one canceled check", calls)
	}
}
