---
title: Query Tags
description: Attach key/value metadata to queries for routing, Prometheus metrics, observability, and backend forwarding.
image: img/queryflux-hero-banner.png
---
# Query tags

Query tags are key/value metadata that clients attach to their sessions. QueryFlux reads them to make routing decisions, records them in query history, emits them as Prometheus metrics, and forwards them to backend engines.

---

## Tag format

Tags are a map of `string → string | null`:

- **Key-value tag** — `team:eng` → key `team`, value `"eng"`
- **Key-only tag** — `batch` → key `batch`, value `null`

Two wire formats are accepted wherever a raw tag string appears:

| Format | Example |
|--------|---------|
| Comma-separated k:v | `team:eng,cost_center:701,batch` |
| JSON object | `{"team":"eng","cost_center":"701","batch":null}` |

Validation rules (soft — invalid entries are silently dropped):

- Maximum **20 tags** per query.
- Keys and values each max **128 characters**.
- Keys must match `[a-zA-Z0-9_-]+`.

---

## Setting tags — by frontend

### Trino HTTP

Use the standard `X-Trino-Client-Tags` header. Each comma-separated element is treated as a **key-only tag** (value = `null`):

```http
X-Trino-Client-Tags: batch,premium
```

For key-value tags, include them in the `X-Trino-Session` header as the `query_tag` or `query_tags` property (k:v or JSON format). Trino’s HTTP client treats the header as **comma-separated** `name=value` session properties and uses **percent-encoding** for values (same rules as `application/x-www-form-urlencoded`), so a **single** `query_tags` value that itself contains commas must be encoded — otherwise the comma would start a new property. Example for one property whose value is the k:v list `team:eng,cost_center:701`:

```http
X-Trino-Session: query_tags=team%3Aeng%2Ccost_center%3A701
```

QueryFlux decodes that value when reading the header, and when you use `SET SESSION` below it emits `X-Trino-Set-Session` with the same encoding so the CLI round-trips safely.

Or from the Trino CLI / JDBC, issue a `SET SESSION` statement before your query. QueryFlux intercepts it locally and echoes the property back via `X-Trino-Set-Session` so the client carries it in subsequent requests:

```sql
SET SESSION query_tags = 'team:eng,cost_center:701,batch';
SELECT * FROM orders;
```

Both sources are merged: `X-Trino-Client-Tags` contributes key-only tags; `query_tag` / `query_tags` in `X-Trino-Session` contributes key-value tags.

### MySQL wire

Issue a `SET` statement before your query. QueryFlux intercepts it and updates the session — no round-trip to the backend occurs:

```sql
SET query_tags = 'team:eng,cost_center:701,batch';
SELECT * FROM orders;
```

Both `SET query_tags` and `SET SESSION query_tags` are accepted, as is the singular `query_tag` spelling. JSON format also works:

```sql
SET query_tags = '{"team":"eng","cost_center":"701","batch":null}';
```

Tags persist for the lifetime of the connection and are re-applied to every subsequent query on that session.

### Postgres wire

Pass `query_tags` (or `query_tag`) as a **startup parameter** in the connection string. Most clients support extra parameters via the `options` field or named parameters:

```
postgresql://host:5432/db?options=-c%20query_tags%3Dteam%3Aeng%2Ccost_center%3A701
```

Or with psql:

```bash
psql "host=localhost port=5432 dbname=mydb query_tags=team:eng,cost_center:701"
```

Tags are extracted once at connection time.

### ClickHouse HTTP

Pass the `X-QueryFlux-Tags` header on each request:

```http
X-QueryFlux-Tags: team:eng,cost_center:701
```

If the header is absent, QueryFlux falls back to the `query_tags` URL query parameter:

```
http://localhost:8123/?query=SELECT+1&query_tags=team:eng
```

---

## Group default tags

Each cluster group can declare **default tags** that are merged with every query routed to it. Client tags override group defaults on the same key:

```yaml
clusterGroups:
  analytics:
    members: [trino-1]
    defaultTags:
      env: prod
      team: analytics
```

If a client sends `team:eng`, the effective tags become `{env: prod, team: eng}` — the client value wins.

---

## Tag-based routing

Use the `tags` router type to route queries based on session tags. See [Tags router](./routing-and-clusters#tags-router-tags) for the full config reference.

Quick example:

```yaml
routers:
  - type: tags
    rules:
      - tags:
          team: eng
          env: prod
        targetGroup: prod-trino
      - tags:
          batch: null         # any client that sets the "batch" key goes here
        targetGroup: batch-trino
```

---

## Backend forwarding

When QueryFlux dispatches a query to a backend engine, effective tags (client tags merged with group defaults) are forwarded in an engine-native way:

| Engine | How tags are forwarded |
|--------|----------------------|
| **Trino** | All tags sent as `X-Trino-Client-Tags`. Key-only tags: `batch`. Key-value tags: `team:eng`. Trino records them in query info/history. |
| **StarRocks** | `SET @query_tag = '{"team":"eng","batch":null}'` executed before the query on the same connection. |
| **DuckDB** | Tags are recorded in QueryFlux query history but not forwarded (DuckDB has no tag mechanism). |
| **Athena** | Tags are recorded in QueryFlux query history but not forwarded to Athena execution. |

---

## Observability

### Prometheus

Tags emit a dedicated counter:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `queryflux_query_tags_total` | Counter | `tag_key`, `tag_value`, `cluster_group` | Incremented once per tag per query. Use this to track which teams/workloads are driving load on each group. |

Example query — queries by team per cluster group over the last hour:

```promql
sum by (tag_value, cluster_group) (
  increase(queryflux_query_tags_total{tag_key="team"}[1h])
)
```

### Tags deny list

High-cardinality tag values (e.g. `query_id`, `request_id`) can pollute Prometheus label space. Add them to the deny list in config to suppress their counter emission while still routing and recording them in history:

```yaml
queryflux:
  metrics:
    tagsDenyList:
      - request_id
      - trace_id
```

Tags in the deny list are still available for routing and stored in query history — only the Prometheus counter is suppressed.

### Query history

Tags are stored with each query record in Postgres. The Studio **Queries** page shows them alongside SQL, status, duration, and routing trace — useful for debugging which workload produced a given query.
