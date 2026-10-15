# QueryFlux Helm Chart

This chart installs the QueryFlux server on Kubernetes. It is provider-neutral: storage, ingress, monitoring, and network policy are opt-in and configured through values instead of being tied to one Kubernetes distribution.

## Quick Start

```bash
helm install queryflux ./charts/queryflux
kubectl port-forward svc/queryflux 3000:3000 8080:8080 9000:9000
```

The default release creates a starter DuckDB-backed QueryFlux configuration and generates the admin password in a Kubernetes Secret.
The default image tag includes QueryFlux Studio on port `3000`; use an image tag ending in `-slim` for server-only deployments.

```bash
kubectl get secret queryflux-admin \
  -o jsonpath='{.data.QUERYFLUX_ADMIN_PASSWORD}' | base64 --decode
```

## Configuration

Override `config.data` with the same YAML shape used by `config.example.yaml`:

```yaml
config:
  data:
    queryflux:
      externalAddress: https://queryflux.example.com
      frontends:
        trinoHttp:
          enabled: true
          port: 8080
      persistence:
        type: postgres
        url: postgres://queryflux:queryflux@postgres:5432/queryflux
      adminApi:
        port: 9000
    clusterGroups:
      trino-default:
        engine: trino
        maxRunningQueries: 100
        clusters:
          - name: trino-1
            endpoint: http://trino:8080
    routers:
      - type: protocolBased
        trinoHttp: trino-default
    routingFallback: trino-default
```

To manage config outside Helm, set `config.create=false` and `config.existingConfigMap` to a ConfigMap containing `config.yaml`.

QueryFlux mounts `config.yaml` verbatim and does **not** interpolate environment variables into it, so any secret in the config (for example a Postgres URL with a password) ends up in plaintext when stored in a ConfigMap. To keep such values out of a ConfigMap, put the full `config.yaml` in a Secret and set `config.existingSecret`, which takes precedence over `config.existingConfigMap` and `config.create`:

```yaml
config:
  create: false
  existingSecret: queryflux-config   # Secret with a config.yaml key
```

### Persistence and replicas

The default config uses `persistence.type: inMemory`, which is per-pod. Running more than one replica (`replicaCount > 1` or `autoscaling.enabled`) with in-memory persistence causes state to diverge across pods. For multi-replica deployments, configure Postgres persistence under `config.data.queryflux.persistence`.

With Postgres persistence, QueryFlux runs in distributed mode: config changes propagate to all replicas, `maxRunningQueries` is enforced cluster-wide rather than per-pod, and each queued query is dispatched by exactly one replica. Each pod derives a unique instance ID automatically; set the `QUERYFLUX_INSTANCE_ID` env var only if you need to override it.

Operator runbook (capacity leases, queue claims, affinity, crash recovery): [Multi-replica operations](https://queryflux.dev/docs/operations/multi-replica).

### Snowflake HTTP requires session affinity

Snowflake HTTP sessions are held in pod-local memory. With more than one replica, every request of a Snowflake session must reach the same pod — configure sticky sessions (session affinity) on the load balancer or ingress in front of the Snowflake port. The other frontends (Trino HTTP, MySQL/PostgreSQL wire, Flight SQL) keep their state in Postgres or on the connection itself and do not need affinity.

To make the requirement explicit, set `config.data.queryflux.enforceSnowflakeHttpSessionAffinity: true`; QueryFlux will then refuse to start unless `frontends.snowflakeHttp.sessionAffinityAcknowledged: true` is also set, so the affinity decision is recorded in config.

## Secrets

By default the chart creates a Secret for `QUERYFLUX_ADMIN_USER` and `QUERYFLUX_ADMIN_PASSWORD`. For production, provide a password explicitly or reference a pre-created Secret:

```yaml
existingSecret:
  name: queryflux-admin
  usernameKey: QUERYFLUX_ADMIN_USER
  passwordKey: QUERYFLUX_ADMIN_PASSWORD
```

## Optional Features

- `ingress.enabled`: expose the Trino HTTP frontend through an ingress controller.
- `autoscaling.enabled`: create an HPA.
- `pdb.enabled`: create a PodDisruptionBudget.
- `networkPolicy.enabled`: create a NetworkPolicy. Defaults to `false`. Enabling with empty `ingress`/`egress` while `policyTypes` lists Ingress/Egress **denies all traffic**; the chart **fails install** in that case. Copy and tighten rules from `examples/networkpolicy-values.yaml` or `examples/production-values.yaml`.
- `serviceMonitor.enabled`: create a Prometheus Operator ServiceMonitor for `/metrics` on the admin port.
- `startupProbe`: HTTP check on `/readyz` (admin port) so liveness does not kill slow startups.
- `terminationGracePeriodSeconds`: default `45`. Keep this ≥ `queryflux.shutdownDrainTimeoutSecs` (default `30`) plus a buffer so Kubernetes does not SIGKILL mid-drain.

The chart also supports `env`, `envFrom`, `extraVolumes`, `extraVolumeMounts`, `extraContainers`, `nodeSelector`, `tolerations`, `affinity`, and `topologySpreadConstraints` for platform-specific integration.

## ADBC drivers

The image ships no ADBC drivers. List the ones your `engine: adbc` clusters use:

```yaml
adbcDrivers:
  install: ["trino=0.6.0", "snowflake=1.14.0"]   # `dbc install` specs; pin exact versions
  # or: dbcToml / dbcLock — a dbc driver list rendered into a ConfigMap (takes precedence)
  onFailure: fail          # fail | warn
  mode: initContainer      # initContainer | entrypoint
  volume:
    emptyDir: {}           # or a persistentVolumeClaim to keep drivers across restarts
```

- `mode: initContainer` (default): an init container installs the drivers into `volume`, and the server mounts it read-only with `ADBC_DRIVER_PATH` set. The server container needs no registry egress.
- `mode: entrypoint`: the server container installs at startup into `volume`, mounted read-write.
- Changing `dbcToml`/`dbcLock` rolls the pods (checksum annotation). Changing `install` changes the pod spec directly.
- With `networkPolicy` egress rules, allow HTTPS (443) to `dbc-cdn.columnar.tech` and `dbc-cdn-private.columnar.tech`.
- Nothing is rendered when `install` and `dbcToml` are both empty.
- Drivers you supply yourself: mount them with `extraVolumes`/`extraVolumeMounts` and set `ADBC_DRIVER_PATH` in `env`.

See [ADBC drivers](https://queryflux.dev/docs/operations/adbc-drivers) for the full reference.

## Examples

- `examples/external-config-values.yaml`: use a pre-created ConfigMap and Secret, and run the server-only image.
- `examples/production-values.yaml`: production checklist — TLS ingress, HPA, PDB, ServiceMonitor, NetworkPolicy, topology spread, `config.existingSecret`, and pre-created admin Secret (no default password in values).
- `examples/production-config.yaml`: template for the config Secret referenced by production-values (`auth.required: true`, OIDC, Postgres URL). Create with `kubectl create secret generic queryflux-config --from-file=config.yaml=...`.
- `examples/adbc-drivers-values.yaml`: install pinned ADBC drivers with an init container.
- `examples/networkpolicy-values.yaml`: starter NetworkPolicy allowing clients, DNS, Postgres, and engines (tighten selectors before production).
- `examples/control-plane-values.yaml` + `examples/control-plane-config.yaml`: a worker-mode control plane (Postgres, `workerApi`).
- `examples/worker-values.yaml` + `examples/worker-config.yaml`: shared-service workers that pull config from the control plane.
- `examples/worker-sidecar-manifest.yaml`: a worker as a native sidecar (Kubernetes 1.29+) next to an application container. This is a plain manifest, not Helm values.

## Worker mode

The same chart deploys a control plane or workers. The role comes from the config, not from the chart. See [Worker mode](https://github.com/lakeops-org/queryflux/blob/main/website/docs/operations/worker-mode.md) for the design.

- **Control plane:** `role: controlPlane`, Postgres, `workerApi.enabled: true`. Workers connect to its admin port (`9000`), so put TLS in front of that port. Snapshots carry cluster credentials.
- **Workers:** `role: worker`, `controlPlane.url` and the shared secret. A worker never connects to Postgres, serves no Studio, and its admin port serves only `/health`, `/readyz`, `/metrics` and `/worker/status`.
- **Probes:** `/readyz` returns 200 once the worker's first config snapshot is active. The startup probe keeps a new pod out of the Service until then. In a native sidecar, it also holds the application container back until then.
- **Session affinity:** a running query lives in the memory of the worker that started it: Trino result polls, Snowflake and MCP sessions. Shared-service workers with more than one replica therefore need `service.sessionAffinity: ClientIP`, which `examples/worker-values.yaml` sets. An external load balancer in front needs sticky sessions too. Sidecar workers are not affected.
- **Secrets:** QueryFlux does not expand environment variables in its config file, so put the worker config, which holds the shared secret, in a Secret (`config.existingSecret`).

## Validation

Run the repository chart check:

```bash
make helm-check
# or directly:
scripts/check-helm-chart.sh
```

The script requires `helm` and runs `helm lint` and `helm template` against the
default values and every file under `examples/`.
