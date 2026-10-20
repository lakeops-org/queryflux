---
sidebar_position: 4
sidebar_label: Kubernetes (Helm)
title: Install on Kubernetes with Helm
description: Install QueryFlux with the Helm chart — starter release, configuration, secrets, Postgres persistence, production values, ADBC drivers, and worker mode.
image: img/queryflux-hero-banner.png
---

# Kubernetes (Helm)

The chart lives in the repository at [`charts/queryflux`](https://github.com/lakeops-org/queryflux/tree/main/charts/queryflux). It is provider-neutral: storage, ingress, monitoring, and network policy are opt-in through values, not tied to one Kubernetes distribution.

## Prerequisites

- Kubernetes and `kubectl` access to the target namespace
- [Helm 3](https://helm.sh/docs/intro/install/)
- A clone of the repository — the chart is installed from the local path:

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux
```

## Quick start

```bash
helm install queryflux ./charts/queryflux
kubectl port-forward svc/queryflux 3000:3000 8080:8080 9000:9000
```

The default release creates a starter **DuckDB**-backed configuration with `inMemory` persistence and generates an admin password in a Secret:

```bash
kubectl get secret queryflux-admin \
  -o jsonpath='{.data.QUERYFLUX_ADMIN_PASSWORD}' | base64 --decode
```

Then open Studio at http://localhost:3000 and send a query to the Trino HTTP frontend:

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

:::tip Server-only image
The default image tag includes **Studio** on port `3000`. For a server-only deployment, set `image.tag` to a tag ending in `-slim`.
:::

## Configure QueryFlux

The chart renders `config.data` into a ConfigMap and mounts it as `config.yaml`. It takes the same YAML shape as the **[YAML reference](/docs/configuration)**:

```yaml title="my-values.yaml"
config:
  data:
    queryflux:
      externalAddress: https://queryflux.example.com
      frontends:
        trinoHttp:
          enabled: true
          port: 8080
      persistence:
        type: inMemory   # single pod; for Postgres, see "Keep credentials out of ConfigMaps"
      adminApi:
        port: 9000
    clusters:
      trino-1:
        engine: trino
        enabled: true
        endpoint: http://trino:8080
    clusterGroups:
      trino-default:
        maxRunningQueries: 100
        members: [trino-1]
    routers:
      - type: protocolBased
        trinoHttp: trino-default
    routingFallback: trino-default
```

```bash
helm upgrade --install queryflux ./charts/queryflux -f my-values.yaml
```

### Keep credentials out of ConfigMaps

QueryFlux mounts `config.yaml` verbatim and **does not expand environment variables** in it. A Postgres URL with a password in `config.data` ends up in plaintext in a ConfigMap. To avoid that, store the whole file in a Secret:

```bash
kubectl create secret generic queryflux-config --from-file=config.yaml=./config.yaml
```

```yaml
config:
  create: false
  existingSecret: queryflux-config   # Secret with a config.yaml key
```

`config.existingSecret` takes precedence over `config.existingConfigMap` and `config.create`. Use `config.existingConfigMap` instead if your config holds no secrets.

### Admin credentials

By default the chart generates `QUERYFLUX_ADMIN_USER` / `QUERYFLUX_ADMIN_PASSWORD`. In production, create the Secret yourself and reference it:

```bash
kubectl create secret generic queryflux-admin \
  --from-literal=QUERYFLUX_ADMIN_USER=admin \
  --from-literal=QUERYFLUX_ADMIN_PASSWORD='<strong-password>'
```

```yaml
existingSecret:
  name: queryflux-admin
  usernameKey: QUERYFLUX_ADMIN_USER
  passwordKey: QUERYFLUX_ADMIN_PASSWORD
```

## Run multiple replicas

Multiple replicas need exactly two things:

1. **A shared Postgres** that every pod connects to. The chart does not deploy one — use a managed database (RDS, Cloud SQL, …) or a Postgres chart in the cluster.
2. **More than one pod** — `replicaCount > 1`, or `autoscaling.enabled` with `autoscaling.minReplicas` of 2 or more (the chart default is 1).

When all pods use the same Postgres, QueryFlux runs in distributed mode: config changes reach every pod, `maxRunningQueries` is one limit across all pods (not per pod), each queued query is dispatched by exactly one pod, and each pod derives its own instance ID.

**1. Put the config in a Secret** — it contains the Postgres password:

```yaml title="config.yaml"
queryflux:
  externalAddress: https://queryflux.example.com
  frontends:
    trinoHttp: { enabled: true, port: 8080 }
  persistence:
    type: postgres          # shared state — required for more than one pod
    url: postgres://queryflux:PASSWORD@my-postgres:5432/queryflux
  adminApi: { port: 9000 }
clusters:
  trino-1: { engine: trino, enabled: true, endpoint: http://trino:8080 }
clusterGroups:
  trino: { maxRunningQueries: 100, members: [trino-1] }
routers:
  - { type: protocolBased, trinoHttp: trino }
routingFallback: trino
```

```bash
kubectl create secret generic queryflux-config --from-file=config.yaml
```

**2. Write the values** — point at the Secret and run more than one pod:

```yaml title="ha-values.yaml"
config:
  create: false
  existingSecret: queryflux-config
replicaCount: 3              # or: autoscaling: { enabled: true, minReplicas: 2, maxReplicas: 6 }
pdb:
  enabled: true              # keep pods available during node drains and upgrades
  maxUnavailable: 1
```

**3. Install:**

```bash
helm install queryflux ./charts/queryflux -f ha-values.yaml
```

Schema migrations run automatically on first start. To check that the pods share state, run a query and confirm it appears in Studio's query history no matter which pod served it.

:::warning Common mistakes
- **`inMemory` with several pods.** Each pod keeps its own queue, history, and config, and they drift apart. The install notes warn about this, but the install still succeeds.
- **The Postgres URL in `config.data`.** That stores the password in plaintext in a ConfigMap — use `config.existingSecret` as above.
- **Snowflake HTTP without sticky sessions.** Snowflake sessions live in pod memory, so every request in a session must reach the same pod: enable session affinity on the load balancer or ingress. To make it explicit, set `queryflux.enforceSnowflakeHttpSessionAffinity: true` and `frontends.snowflakeHttp.sessionAffinityAcknowledged: true`. Trino HTTP, MySQL/PostgreSQL wire, and Flight SQL don't need affinity.
:::

For production, [`examples/production-values.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/production-values.yaml) builds on this with an HPA (2–6 pods), TLS ingress, ServiceMonitor, NetworkPolicy, and pod anti-affinity — see [Production install](#production-install). For capacity leases, queue claims, draining, and crash recovery, see **[Multi-replica operations](../operations/multi-replica.md)**.

## Production install

[`examples/production-values.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/production-values.yaml) is a production checklist in values form: two replicas, TLS ingress, HPA, PDB, ServiceMonitor, NetworkPolicy, topology spread, config from a Secret, and a pre-created admin Secret.

```bash
# 1. Admin credentials (see above)
kubectl create secret generic queryflux-admin \
  --from-literal=QUERYFLUX_ADMIN_USER=admin \
  --from-literal=QUERYFLUX_ADMIN_PASSWORD='<strong-password>'

# 2. Config: copy the template, fill in the Postgres URL and OIDC/LDAP, then store it
cp charts/queryflux/examples/production-config.yaml ./production-config.yaml
kubectl create secret generic queryflux-config \
  --from-file=config.yaml=./production-config.yaml

# 3. A TLS secret for the ingress (named queryflux-tls in the example)

# 4. Install
helm install queryflux ./charts/queryflux \
  -f ./charts/queryflux/examples/production-values.yaml
```

Pin `image.tag` to a release tag in real deployments rather than relying on the default.

## Optional features

| Value | What it does |
| --- | --- |
| `ingress.enabled` | Expose the Trino HTTP frontend through an ingress controller. |
| `autoscaling.enabled` | Create an HPA (requires Postgres persistence). |
| `pdb.enabled` | Create a PodDisruptionBudget for rolling upgrades. |
| `networkPolicy.enabled` | Create a NetworkPolicy. Empty `ingress`/`egress` rules would deny all traffic, so the chart **fails the install** in that case — start from `examples/networkpolicy-values.yaml`. |
| `serviceMonitor.enabled` | Prometheus Operator ServiceMonitor for `/metrics` on the admin port. |
| `startupProbe` | HTTP check on `/readyz` so liveness doesn't kill slow startups. |
| `terminationGracePeriodSeconds` | Default `45`. Keep it ≥ `queryflux.shutdownDrainTimeoutSecs` (default `30`) plus a buffer so pods aren't killed mid-drain. |

The chart also accepts `env`, `envFrom`, `extraVolumes`, `extraVolumeMounts`, `extraContainers`, `nodeSelector`, `tolerations`, `affinity`, and `topologySpreadConstraints`.

## ADBC drivers

The image ships **no ADBC drivers**. List the ones your `engine: adbc` clusters need, pinned to exact versions:

```yaml
adbcDrivers:
  install: ["trino=0.6.0", "snowflake=1.14.0"]
  onFailure: fail          # fail | warn
  mode: initContainer      # initContainer | entrypoint
  volume:
    emptyDir: {}           # or a persistentVolumeClaim to keep drivers across restarts
```

With `mode: initContainer` (default) an init container installs the drivers and the server mounts them read-only, so the server needs no registry egress. If you use NetworkPolicy egress rules, allow HTTPS to `dbc-cdn.columnar.tech` and `dbc-cdn-private.columnar.tech`. Full reference: **[ADBC drivers](/docs/operations/adbc-drivers)**.

## Worker mode

The same chart deploys a **control plane** or a pool of **workers** — one release each:

```bash
# Control plane: Postgres, Studio, admin API, worker API
helm install queryflux-control ./charts/queryflux \
  -f ./charts/queryflux/examples/control-plane-values.yaml

# A pool of shared-service workers, scaled on load
helm install queryflux-workers ./charts/queryflux \
  -f ./charts/queryflux/examples/worker-values.yaml \
  -f ./charts/queryflux/examples/worker-autoscaling-values.yaml
```

Other shapes: one worker per node with `examples/worker-daemonset-values.yaml`, or a worker as a native sidecar with `examples/worker-sidecar-manifest.yaml`. Design and operations: **[Worker mode](../operations/worker-mode.md)** and **[Add a worker as a sidecar](../operations/worker-sidecar.md)**.

## Example values files

All under [`charts/queryflux/examples/`](https://github.com/lakeops-org/queryflux/tree/main/charts/queryflux/examples):

| File | Use it for |
| --- | --- |
| `production-values.yaml` + `production-config.yaml` | Production checklist (TLS, HPA, PDB, ServiceMonitor, NetworkPolicy, secrets) |
| `external-config-values.yaml` | Pre-created ConfigMap and Secret, server-only image |
| `adbc-drivers-values.yaml` | Pinned ADBC drivers via init container |
| `networkpolicy-values.yaml` | Starter NetworkPolicy — tighten selectors before production |
| `control-plane-values.yaml` + `control-plane-config.yaml` | Worker-mode control plane |
| `worker-values.yaml` + `worker-config.yaml` | Shared-service workers |
| `worker-autoscaling-values.yaml` | Scale workers on load |
| `worker-daemonset-values.yaml` | One worker per node |
| `worker-sidecar-manifest.yaml` | Worker as a native sidecar (plain manifest, not values) |

## Validate the chart

From the repository root, lint and render the chart against the default values and every example:

```bash
make helm-check
```

## Upgrade and uninstall

```bash
helm upgrade queryflux ./charts/queryflux -f my-values.yaml
helm uninstall queryflux
```

With Postgres persistence, QueryFlux applies pending schema migrations at startup (`persistence.autoMigrate`, default `true`). To run them as a separate step instead — for example from a Job before the rollout — set `autoMigrate: false` and run `queryflux migrate --config /etc/queryflux/config.yaml`.
