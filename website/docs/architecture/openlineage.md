---
title: OpenLineage
description: Send an OpenLineage event for every query, with table and column lineage across every engine QueryFlux routes to.
---
# OpenLineage

QueryFlux can send an [OpenLineage](https://openlineage.io) event for every query that reads or writes a table. Because every engine sits behind QueryFlux, the events form one lineage graph across Trino, Snowflake, StarRocks, DuckDB, Athena and the rest. Any consumer of the OpenLineage HTTP transport works: Marquez, DataHub, OpenMetadata.

## Turn it on

```yaml
queryflux:
  openLineage:
    url: https://marquez.example.com   # events go to {url}{endpoint}
    endpoint: /api/v1/lineage          # default
    namespace: queryflux               # job namespace (default)
    sql: maskLiterals                  # full | maskLiterals (default) | metadata
```

The API key, when the consumer needs one, is sent as `Authorization: Bearer`. Set it with `apiKey`, or leave that out and set the `OPENLINEAGE_API_KEY` environment variable to keep it out of the config file.

Configure it on a standalone QueryFlux or on the control plane. In [worker mode](../operations/worker-mode), workers work out each query's tables and send them upstream with their query history; the control plane sends the events. A worker's own `openLineage` setting is ignored.

## What an event contains

One event per query, sent when the query finishes: `COMPLETE`, `FAIL` or `ABORT`. Queries that touch no table (`SELECT 1`), statements that move no data (`DROP`, `ALTER`, `GRANT`), queries blocked by a guard, and queries QueryFlux can't parse send nothing.

| Part | Value |
|---|---|
| Run ID | The QueryFlux query ID |
| Job | `namespace` above; name from the `openlineage.job` query tag, else `query.<hash>` of the query with its literals removed, so repeats of one query are one job |
| Inputs and outputs | The tables read and the table written |
| `sql` job facet | The query, as allowed by `sql`: `full`, `maskLiterals` (literals replaced with `?`), or `metadata` (no SQL). Error messages follow the same setting. |
| `queryflux` run facet | Cluster group, cluster, engine, dialects, whether the query was translated or rewritten by access control, cache hit, user, agent, timings |
| `queryflux_columnsRead` input facet | The columns read from each table, when the [catalog](./catalog-integration) gives QueryFlux the schema |
| `columnLineage` output facet | For writes (`INSERT … SELECT`, `CREATE TABLE … AS`, `UPDATE`): each written column's input columns and how they were used, plus the columns that filter, join, group or order the rows. An input column masked by access control for that query is marked `masking: true`. |

Column-level details need a configured [catalog](./catalog-integration). Without a schema, `SELECT *` can't be expanded and an unqualified column in a join can't be tied to a table, so those columns are left out, never guessed.

## Linking to the caller's own lineage

A tool that runs queries through QueryFlux can attach them to its own runs with [query tags](./query-tags):

| Tag | Effect |
|---|---|
| `openlineage.job` | The job name |
| `openlineage.parent_run_id`, `openlineage.parent_job_namespace`, `openlineage.parent_job_name` | The `parent` run facet (all three needed) |

Without these tags, a query from an AI agent gets its conversation as the parent (job `agent/<agent id>` in namespace `queryflux-agents`).

## Naming datasets across engines

The catalog name in SQL belongs to the engine: Trino might call a Polaris catalog `iceberg`, while Snowflake calls the same catalog `ANALYTICS`. Named after the engine, one table would become two unrelated datasets. Map each cluster's catalog names to the catalog behind them:

```yaml
clusters:
  trino-prod:
    lineageCatalogs:
      iceberg: iceberg+rest://polaris.example.com/api/catalog
  snowflake-prod:
    lineageCatalogs:
      ANALYTICS: iceberg+rest://polaris.example.com/api/catalog
```

A mapped catalog's tables are named `schema.table` under the mapped namespace, so both clusters report the same dataset. If Spark or Airflow also send lineage for these tables, use the namespace their integrations emit, or the graphs won't join.

An unmapped catalog is named after the engine: `<engine>://<host:port>` from the cluster's endpoint (`awsathena://athena.<region>.amazonaws.com` for Athena, `bigquery` for BigQuery), with the name `catalog.schema.table`. A table without a catalog or schema in the SQL takes the session's.

For clusters stored in Postgres, `lineageCatalogs` is a key in the cluster's `config` object in the admin API.

## Delivery

Events are queued in memory and sent in the background, never on the query path. While the consumer is unreachable, up to `spoolMaxEvents` (default 10,000) wait and the oldest beyond that are dropped; delivery retries with backoff and honours `Retry-After`. An event the consumer rejects with a 4xx (other than 408 and 429) is dropped and logged rather than retried. Queued events are sent once more at shutdown.

Because the run ID is the query ID, an event delivered twice is still one run to the consumer.

## What's next

QueryFlux will store lineage itself, taking events from other producers too, and show it in Studio as a graph of jobs, datasets and columns. See the **[Roadmap](../roadmap#built-in-lineage-store-and-studio-lineage-graph)**.
