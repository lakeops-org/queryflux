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
- `networkPolicy.enabled`: create a NetworkPolicy. The default policy body is empty so operators can define provider-specific ingress and egress rules.
- `serviceMonitor.enabled`: create a Prometheus Operator ServiceMonitor for `/metrics` on the admin port.

The chart also supports `env`, `envFrom`, `extraVolumes`, `extraVolumeMounts`, `extraContainers`, `nodeSelector`, `tolerations`, `affinity`, and `topologySpreadConstraints` for platform-specific integration.

## Examples

- `examples/external-config-values.yaml`: use a pre-created ConfigMap and Secret, and run the server-only image.
- `examples/production-values.yaml`: shows ingress, TLS, HPA, PDB, ServiceMonitor, NetworkPolicy, resource requests, and topology spread settings.

## Validation

Run the repository chart check:

```bash
make helm-check
# or directly:
scripts/check-helm-chart.sh
```

The script requires `helm` and runs `helm lint` and `helm template` against the
default values and every file under `examples/`.
