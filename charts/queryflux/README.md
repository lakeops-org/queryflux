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

## Examples

- `examples/external-config-values.yaml`: use a pre-created ConfigMap and Secret, and run the server-only image.
- `examples/production-values.yaml`: production checklist — TLS ingress, HPA, PDB, ServiceMonitor, NetworkPolicy, topology spread, `config.existingSecret`, and pre-created admin Secret (no default password in values).
- `examples/production-config.yaml`: template for the config Secret referenced by production-values (`auth.required: true`, OIDC, Postgres URL). Create with `kubectl create secret generic queryflux-config --from-file=config.yaml=...`.
- `examples/networkpolicy-values.yaml`: starter NetworkPolicy allowing clients, DNS, Postgres, and engines (tighten selectors before production).

## Validation

Run the repository chart check:

```bash
make helm-check
# or directly:
scripts/check-helm-chart.sh
```

The script requires `helm` and runs `helm lint` and `helm template` against the
default values and every file under `examples/`.
