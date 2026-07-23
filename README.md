# Minecraft Wake Gateway

A small Kubernetes-aware Minecraft Java proxy that lets an authenticated player wait in [PicoLimbo](https://github.com/Quozul/PicoLimbo) while an on-demand server starts, then moves that same player to the backend when it is ready.

Built as an in-process plugin for [Gate](https://github.com/minekube/gate), the extensible Minecraft proxy from Minekube.

## Features

- Uses online mode and Velocity forwarding.
- Wakes one `MinecraftServer` target only when an authenticated player has reached PicoLimbo.
- Deduplicates concurrent wake requests and retries failed transfers without disconnecting waiting players.
- Waits for both `MinecraftServer.status.phase=Running` and a ready Kubernetes EndpointSlice before transfer.
- Redirects an authenticated player kicked by the backend back to PicoLimbo instead of dropping the connection.
- Exposes liveness, readiness, and token-protected operational status endpoints.

## How it works

```text
Minecraft client (online-mode)
        |
        v
Gate gateway :25565
  | backend ready --------------------------> survival service
  |
  | not ready
  v
PicoLimbo -- authenticated player connected --> patch MinecraftServer replicas=1
                                                   |
                                                   v
                                  wait for Running + ready EndpointSlice
                                                   |
                                                   v
                                            transfer active player
```

The target is currently fixed to `minecraft/survival`. Initial routing goes to `survival` only when it is already known ready; otherwise it goes to PicoLimbo. A wake begins only after an authenticated player has actually connected to PicoLimbo. Status pings, handshakes, pre-login traffic, and unauthenticated players do not wake the server.

This is intentionally a no-kick flow: a player waits in PicoLimbo while the backend starts, and an authenticated player kicked by `survival` is sent back to PicoLimbo with a waiting message.

## Project status

Early Kubernetes implementation for one target (`minecraft/survival`). The automated suite covers routing, wake coordination, readiness watching, transfers, and status handling. End-to-end acceptance with a live Mojang-authenticated account that remains in the same session still needs confirmation.

## Requirements

- Go 1.26.5 to build from source.
- Kubernetes: the binary uses in-cluster authentication.
- A compatible `MinecraftServer` resource at `platform.liltorp.dev/v1alpha1`.
- [Gate](https://github.com/minekube/gate), [PicoLimbo](https://github.com/Quozul/PicoLimbo), and the `survival` backend reachable at the configured in-cluster DNS names:
  - `picolimbo.minecraft.svc.cluster.local:25565`
  - `survival.minecraft.svc.cluster.local:25565`

## Quick start

```sh
go build ./...
go vet ./...
go test ./...
docker build -t minecraft-wake-gateway .
```

The Dockerfile expects the repository root as its build context and produces a static `linux/amd64` image.

## Configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `FORWARDING_SECRET` | Yes | Velocity forwarding secret shared with the backend. |
| `STATUS_TOKEN` | Yes | Bearer token for the operational status endpoint. |

Supply both through your deployment secret mechanism; never commit their values.

## Kubernetes contract and RBAC

The gateway reads and patches only resources in the `minecraft` namespace:

- `platform.liltorp.dev/v1alpha1` `minecraftservers`: `get`, `list`, `watch`, `patch`.
- `discovery.k8s.io/v1` `endpointslices`: `get`, `list`, `watch`.

It requests a wake by patching `platform.liltorp.dev/replicas: "1"` on `minecraft/survival`. It does not create, delete, or directly manage workloads; the `MinecraftServer` controller owns that lifecycle.

## Status API

The HTTP server listens on `:8080`:

- `GET /healthz` returns `200` while the process is alive.
- `GET /readyz` returns `200` after an initial Kubernetes readiness read succeeds and both local Gate and PicoLimbo are reachable; otherwise `503`.
- `GET /v1/targets/minecraft/survival` returns session counts, last activity, wake state, backend readiness, and the latest wake error. Send `Authorization: Bearer $STATUS_TOKEN`; otherwise it returns `401`.

## Security and limitations

Keep the forwarding secret and status token in a secret store, and do not expose the status API publicly. This implementation assumes online-mode authentication and a trusted Kubernetes network path to PicoLimbo and the backend.

It is not a high-availability deployment: a single node or gateway failure disconnects players, and node loss can leave the service unavailable until recovery. The target namespace, name, DNS endpoints, timings, and the `MinecraftServer` API group are currently compiled into the binary. It is not a general multi-server control plane or a claim of universal Minecraft-version compatibility.

## Contributing and development

Run `go build ./...`, `go vet ./...`, and `go test ./...` before proposing a change. Keep the wake boundary strict: only an authenticated PicoLimbo session may start a backend, while probes and pre-login traffic must remain side-effect free.
