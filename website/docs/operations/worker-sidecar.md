---
title: Add a worker as a sidecar
description: Run a QueryFlux worker inside each application pod, so the application connects to localhost and never holds engine endpoints or credentials.
image: img/queryflux-hero-banner.png
---

# Add a worker as a sidecar

A sidecar worker runs inside your application's pod. The application connects to `localhost`, and the worker gets everything else from the control plane: engine endpoints, credentials, routing, limits and access rules. The application's database config shrinks to one fixed local address.

This guide assumes you've read [Worker mode](./worker-mode) and have a control plane running with the worker API enabled.

## When to use a sidecar

| Pattern | Workers | Use it when |
|---|---|---|
| **Sidecar** (this page) | one per application pod | a service needs its own worker: isolation, per-pod labels, nothing shared with other apps |
| Shared pool ([Helm `worker-values.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/worker-values.yaml)) | a few, behind a Service | most teams; fewest workers |
| One per node ([Helm `worker-daemonset-values.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/worker-daemonset-values.yaml)) | one per node | many small apps; apps reach their node's worker through one Service address |

Every sidecar is a worker of its own. It keeps its own long-poll and heartbeat with the control plane, and takes one capacity lease per query. That's fine for tens to a few hundred pods. For thousands, prefer one worker per node.

## How it works

The worker runs as a **native sidecar**: an init container with `restartPolicy: Always`. This needs Kubernetes 1.29 or later.

- **Start:** Kubernetes starts the worker first and waits for its startup probe (`/readyz`). `/readyz` turns `200` once the worker has a config from the control plane, so the application never starts against a worker that can't run queries yet.
- **Run:** the application connects to the worker on `localhost`. The worker routes, translates and sends each query to the engine, takes capacity leases from the control plane, and sends it query history.
- **Stop:** Kubernetes stops the application first and the worker last, so queries in flight during shutdown still complete.

## 1. Create the worker config

Every pod shares one worker config. Store it in a Secret: it holds the shared secret, and QueryFlux doesn't expand environment variables in its config.

```yaml title="worker-config.yaml"
queryflux:
  role: worker
  controlPlane:
    url: https://queryflux-control.queryflux.svc:9000
    auth:
      mode: sharedSecret
      sharedSecret: <the control plane's workerApi.sharedSecret>
  # Report this pod's own labels (mounted below), e.g. app=checkout, team=payments.
  workerLabelsFile: /etc/podinfo/labels
  coordination:
    mode: controlPlane       # share maxRunningQueries with every other worker
  # Enable only the protocols your application speaks. The worker shares the pod's
  # network with the application: pick ports the application does not use itself.
  frontends:
    trinoHttp:
      enabled: true
      port: 18080
    postgresWire:
      enabled: true
      port: 15432
  adminApi:
    port: 19000              # ops listener: /health, /readyz, /metrics, /worker/status
```

```bash
kubectl create secret generic queryflux-worker-config --from-file=config.yaml=worker-config.yaml
```

The worker's name defaults to the pod name, so every pod reports itself separately. Leave `workerName` unset.

## 2. Add the sidecar to your Deployment

Add one init container and two volumes to the pod template. Your application container stays as it is.

```yaml title="deployment.yaml (pod template)"
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 45     # at least shutdownDrainTimeoutSecs (30) + buffer
      initContainers:
        - name: queryflux
          image: ghcr.io/lakeops-org/queryflux:<release>
          restartPolicy: Always             # native sidecar
          args: ["--config", "/etc/queryflux/config.yaml"]
          startupProbe:                     # the app starts once this passes
            httpGet: { path: /readyz, port: 19000 }
            periodSeconds: 2
            failureThreshold: 150
          readinessProbe:
            httpGet: { path: /readyz, port: 19000 }
          livenessProbe:
            httpGet: { path: /health, port: 19000 }
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits: { memory: 1Gi }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 1000
            capabilities: { drop: [ALL] }
          env:
            - { name: TMPDIR, value: /tmp }
          volumeMounts:
            - { name: queryflux-config, mountPath: /etc/queryflux, readOnly: true }
            - { name: podinfo, mountPath: /etc/podinfo, readOnly: true }
            - { name: tmp, mountPath: /tmp }
      containers:
        - name: app
          # ... unchanged, plus the connection settings from step 3
      volumes:
        - name: queryflux-config
          secret: { secretName: queryflux-worker-config }
        - name: podinfo                      # this pod's labels, for workerLabelsFile
          downwardAPI:
            items:
              - path: labels
                fieldRef: { fieldPath: metadata.labels }
        - name: tmp
          emptyDir: {}
```

A complete manifest is in [`charts/queryflux/examples/worker-sidecar-manifest.yaml`](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/examples/worker-sidecar-manifest.yaml).

## 3. Point the application at localhost

The application connects to the worker with the client it already uses. It needs no engine host and no engine credentials:

| Protocol | Connect to | Notes |
|---|---|---|
| Trino HTTP | `http://localhost:18080` | Trino JDBC: `jdbc:trino://localhost:18080`. Any Trino client works. |
| PostgreSQL wire | `localhost:15432` | Clients built on libpq (psql, psycopg, most Postgres drivers) read `PGHOST=localhost` and `PGPORT=15432` from the environment, so setting those two variables is often the only change. |
| MySQL wire | `localhost:<mysqlWire.port>` | |
| Arrow Flight SQL | `grpc://localhost:<flightSql.port>` | |

For example, a libpq application needs only:

```yaml
containers:
  - name: app
    env:
      - { name: PGHOST, value: localhost }
      - { name: PGPORT, value: "15432" }
```

The client still sends a user name, which QueryFlux uses for routing, authorization and query history. If the control plane enables authentication, the client authenticates to the worker the same way it would to a central proxy.

## 4. Check that it works

```bash
kubectl port-forward deploy/checkout 19000:19000
curl -s localhost:19000/worker/status
```

`activeRevision` should match the revision the control plane serves, with `configSource: controlPlane` and `stale: false`. In Studio, the **Workers** page lists one worker per pod with the pod's labels, and query history shows `via <pod name>` for its queries.

## Production checklist

- **Ports:** the worker's frontend and ops ports must not collide with ports the application listens on, because the containers share the pod's network.
- **Network exposure:** the worker listens on the pod IP too, not only `localhost`. Add a NetworkPolicy that admits ingress only on the application's own ports, so other pods can't use the worker directly. Traffic over `localhost` isn't affected by NetworkPolicies.
- **TLS to the control plane:** configs carry engine credentials. Keep `allowInsecureHttp` off and serve the control plane's admin port over TLS.
- **Shutdown:** set `terminationGracePeriodSeconds` to at least the worker's `shutdownDrainTimeoutSecs` (default 30) plus a buffer.
- **Resources:** start at 100m CPU and 256Mi memory per worker, and raise them for large result sets or heavy translation.
- **Control-plane outages:** a running pod keeps serving with the config it has. A new pod waits on its startup probe until it gets one. To let new pods start during an outage, enable [peer bootstrap](./worker-mode#starting-while-the-control-plane-is-down):
  1. Add a headless Service selecting your application's pods on the worker's ops port (19000 here).
  2. Set `controlPlane.peerBootstrap.address` to it.

  A new pod then copies the config from a running pod of the same application.
- **Kubernetes older than 1.29:** native sidecars aren't available. Run the worker as a regular second container. Kubernetes then doesn't order startup or shutdown, so make the application retry its first connection, and accept that the worker may stop before the application on shutdown.

## Troubleshooting

| Symptom | Check |
|---|---|
| Pod stuck in `Init` | The worker can't get a config. `kubectl logs <pod> -c queryflux` shows the sync errors: wrong control plane URL or secret, TLS, or the control plane can't build its config. |
| Queries get `503` or "not configured yet" | Same as above. The worker is up but hasn't activated a config. |
| Queries wait, then time out | The shared limit is full (`queryflux_capacity_lease_denied_total` rises), or the control plane is unreachable and `coordination.onUnavailable` is `failClosed`. |
| Worker missing in Studio | It heartbeats to the control plane every 10s. Check its logs for "Registered with the control plane". |
| Wrong labels in Studio | `workerLabelsFile` must point at the mounted downward API file, and the `podinfo` volume must be mounted. |
