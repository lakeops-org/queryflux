---
sidebar_position: 2
sidebar_label: QueryFlux Studio
title: QueryFlux Studio
description: Web UI for clusters, query history, routing, and admin security. Connects to the QueryFlux Admin API on port 9000.
image: img/queryflux-hero-banner.png
---
# QueryFlux Studio

QueryFlux Studio is the built-in web management UI. It connects to the **Admin REST API** (default port `9000`) and lets you monitor clusters, browse query history, manage routing rules, cluster groups, and security settings.

## Accessing Studio

Studio is a Next.js application served on port `3000` (Docker images) or via `pnpm dev` locally. It talks to the Admin API through a same-origin proxy — no CORS configuration required.

| Service | Default URL |
|---|---|
| Studio | http://localhost:3000 |
| Admin API | http://localhost:9000 |

## Authentication

The Admin API is protected by **HTTP Basic authentication**. Studio presents a login dialog on first visit and stores the session in a browser cookie for the duration of the tab session.

### Default credentials

| Username | Password |
|---|---|
| `admin` | `admin` |

:::warning Change the default password

The default `admin`/`admin` credentials are intentionally simple for first boot. **Change the password immediately** after your first login using the Security page. Once changed, the new bcrypt-hashed password is stored in the database and the bootstrap credentials are no longer used.

:::

## Changing the admin password

1. Sign in with your current credentials.
2. Go to **Security** in the left sidebar.
3. If the default password is still active, an amber warning banner appears at the top — click **Change password** there.
4. Enter your current password, a new password (minimum 8 characters), and confirm it.
5. Click **Change password**. The new password is stored as a bcrypt hash (cost 12) in the `proxy_settings` table.

After the change:
- The YAML / environment variable bootstrap credentials are **ignored** — the database record takes precedence.
- Password changes survive process restarts (requires Postgres persistence; see note below).

:::note In-memory persistence

When QueryFlux runs with `persistence.type: inMemory`, the password change takes effect for the current process lifetime but is **lost on restart**. Use `persistence.type: postgres` to make changes permanent.

:::

## Configuration

### YAML

```yaml
queryflux:
  adminApi:
    port: 9000            # default
    username: admin       # bootstrap username (default: admin)
    password: admin       # bootstrap password (default: admin)
```

### Environment variables

Environment variables take precedence over YAML:

| Variable | Description | Default |
|---|---|---|
| `QUERYFLUX_ADMIN_USER` | Bootstrap admin username | `admin` |
| `QUERYFLUX_ADMIN_PASSWORD` | Bootstrap admin password | `admin` |

### Credential priority

| Source | Active when |
|---|---|
| Database (bcrypt hash) | Password has been changed via the UI at least once |
| YAML / env vars | No DB record exists yet (first boot) |

Once a DB record exists it is always used, regardless of what the YAML or env vars say. To reset to bootstrap credentials you must delete the `admin_credentials` key from the `proxy_settings` table.

:::caution Emergency use only

This resets authentication to the default `admin`/`admin` credentials. Only run this if you have lost access and cannot recover the password through other means. Change the password immediately after regaining access.

:::

```sql
DELETE FROM proxy_settings WHERE key = 'admin_credentials';
```

## Admin API endpoints

The Admin API is a plain HTTP service — you can call it directly with any HTTP client:

```bash
# Using default credentials
curl -u admin:admin http://localhost:9000/admin/auth/status

# Check if a DB password override is active
# Response: {"db_override": false}  ← still using bootstrap creds

# Change password via curl
curl -u admin:admin -X POST http://localhost:9000/admin/auth/change-password \
  -H "Content-Type: application/json" \
  -d '{"current_password": "admin", "new_password": "my-secure-pass"}'
```

The full OpenAPI spec is available at `http://localhost:9000/openapi.json` and a Swagger UI at `http://localhost:9000/docs`.

## Managing clusters

The **Clusters** page lists runtime cluster state (health, running queries, capacity) and lets you add or edit persisted cluster configs when Postgres persistence is enabled.

In **distributed mode**, the **running** count shown reflects backend ground truth from the reconcile sweep (for example Snowflake warehouse `running`, BigQuery in-flight jobs). **Capacity** / admission limits use QueryFlux fleet-wide leases separately — a warehouse can report many running queries while QueryFlux only holds a few admission slots.

### Add cluster

1. Click **Add cluster** and choose an engine (ADBC drivers are listed individually — Snowflake, Databricks, BigQuery, etc.).
2. Step 2: set cluster name, routing limits, and connection fields.
3. For **SaaS ADBC** drivers (Snowflake, Databricks, BigQuery, Redshift), configure **Warehouses & health**:
   - Add one or more warehouse rows (each expands to `cluster-name::variant-name` at runtime).
   - Optionally override **Health check query** and **Reconcile query** (leave empty for built-in driver introspection).
4. For **other ADBC** drivers, the same optional health/reconcile fields appear under **Health & reconcile** (no structured warehouse editor on create).
5. **Save cluster** persists to Postgres; the proxy hot-reloads adapters without restart.

### Edit cluster

Expand a cluster row to edit connection settings, enable/disable, max concurrent queries, warehouses (SaaS ADBC), variants JSON (other ADBC), and optional health/reconcile SQL overrides.

Password and bearer token fields can be left blank to keep values already stored in Postgres.

### Health and reconcile fields (ADBC)

| Field | Config key | When to set |
|---|---|---|
| Health check query | `healthCheckQuery` | Override built-in probe (for example custom `SHOW WAREHOUSES` with `{{sub_resource}}`) |
| Reconcile query | `reconcileQuery` | Override running-query ground truth; must return a single integer |

Empty fields mean **use built-in behavior** — Databricks REST, Snowflake `SHOW WAREHOUSES`, BigQuery `JOBS_BY_PROJECT`, etc. See **[Cluster variants, health checks & reconciliation](./architecture/cluster-variants-and-health)**.

### Cluster groups and variants

When a cluster has warehouse variants, add **expanded** member names to groups (for example `my-snowflake::analytics`), not just the base cluster name.

## Studio in production

For production deployments:

- Set `QUERYFLUX_ADMIN_USER` and `QUERYFLUX_ADMIN_PASSWORD` to non-default values in your deployment environment, **or** change the password via the UI immediately after first boot.
- Run Studio behind a reverse proxy (nginx, Caddy, …) with TLS — the Admin API cookie is `SameSite=Strict` but is not `Secure`-flagged by default.
- The Admin API/Studio UI login does not yet support OIDC/SSO — it uses HTTP Basic authentication only. Note that QueryFlux's *query-path* authentication already supports OIDC providers (see [Auth & Authorization Design](./architecture/auth-authz-design.md)); this limitation applies only to the Studio management interface. Future releases will add pluggable auth providers for Studio as well.
