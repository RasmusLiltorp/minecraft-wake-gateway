# minecraft-wake-gateway

An in-process [Gate](https://github.com/minekube/gate) Java Edition proxy plugin that holds authenticated players in PicoLimbo while a Kubernetes-managed Minecraft server wakes, then transfers them to the ready backend.

## Architecture

The gateway is deliberately scoped to the `minecraft/survival` `MinecraftServer` resource. Gate listens on `:25565` in online mode with Velocity forwarding. The status server listens on `:8080`.

Only authenticated, play-state events enter the wake coordinator. On login, a player goes to PicoLimbo unless the `MinecraftServer` reports `status.phase=Running` and the `survival` Service has a ready EndpointSlice endpoint. A waiting player causes one deduplicated wake loop, which patches `platform.liltorp.dev/replicas: "1"` on the `MinecraftServer`, watches readiness, and transfers active players to `survival`.

This is a no-kick flow: PicoLimbo is the initial holding backend, and a player kicked by `survival` is redirected back to PicoLimbo with a waiting message. Status pings, handshakes, pre-login events, and unauthenticated players cannot wake the backend.

## Requirements and configuration

The binary uses Kubernetes in-cluster authentication and therefore must run in a Pod with network access to:

- `picolimbo.minecraft.svc.cluster.local:25565`
- `survival.minecraft.svc.cluster.local:25565`
- the Kubernetes API

Required environment variables:

| Variable | Purpose |
| --- | --- |
| `FORWARDING_SECRET` | Velocity forwarding secret shared with the Minecraft backend. |
| `STATUS_TOKEN` | Bearer token required by the authenticated status endpoint. |

Both values are secrets and must be supplied by the deployment environment; do not place them in this repository.

## Kubernetes RBAC contract

Grant the gateway only the permissions it needs in the `minecraft` namespace:

- `platform.liltorp.dev/v1alpha1`, resource `minecraftservers`: `get`, `list`, `watch`, `patch`.
- `discovery.k8s.io/v1`, resource `endpointslices`: read access (`get`, `list`, `watch`).

The gateway never creates or deletes workloads. It requests wake-up solely by patching the replicas annotation, and treats the `MinecraftServer` controller as the owner of actual lifecycle decisions.

## Status endpoints

- `GET /healthz` returns `200` while the process is alive.
- `GET /readyz` returns `200` only after the first Kubernetes readiness snapshot succeeds and both local Gate (`127.0.0.1:25565`) and PicoLimbo are reachable; otherwise `503`.
- `GET /v1/targets/minecraft/survival` returns JSON containing session counts, last activity, wake state, backend readiness, and the last wake error. It requires `Authorization: Bearer $STATUS_TOKEN`; unauthorized requests return `401`.

## Build and test

```sh
go build ./...
go vet ./...
go test ./...
docker build -t minecraft-wake-gateway .
```

The Dockerfile uses the repository root as its build context and produces a static `linux/amd64` image.

## Security and operations

The proxy uses online mode and Velocity forwarding; protect the forwarding secret and status token as Kubernetes secrets. The status API exposes operational state, so it should not be internet-facing. This gateway is intentionally single-target and its Kubernetes dependency means it is not a standalone local server process. A single-node deployment has no gateway or backend redundancy: node loss disconnects players and can make the service unavailable until recovery.

The surrounding deployment manifests, encrypted-secret handling, and cluster-specific credentials remain in the private infrastructure repository. This repository contains only the reviewed gateway source and its container build.
