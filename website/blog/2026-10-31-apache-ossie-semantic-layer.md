---
slug: apache-ossie-semantic-layer
title: "Teach agents your metrics, not your joins"
description: Apache Ossie gives metrics one vendor-neutral definition. ossiec compiles them into correct SQL for each engine, and QueryFlux is where those queries get routed, guarded and recorded.
authors: [amit]
tags: [Vision]
keywords:
  - Apache Ossie
  - semantic layer
  - semantic model
  - metrics layer
  - AI agents
  - MCP
  - QueryFlux
  - ossiec
  - fan trap
  - text-to-SQL
---

Ask an agent for "units in stock by customer region" over an ordinary star schema, and it will write something like this:

```sql
SELECT c.region, SUM(p.stock)
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products  p ON o.product_id  = p.product_id
GROUP BY c.region
```

It runs and it looks right, but the numbers are wrong. Stock lives on `products`, and the join repeats each product once per order:

| region | agent's SQL | correct |
|---|---:|---:|
| EU | 250 | 150 |
| US | 60 | 30 |

Nothing errors and nothing warns, so the dashboard or the agent's answer ships with a number that is off by 2×.

> **An agent that can write SQL still doesn't know what your metrics mean.** Something has to hold that meaning, and something has to turn it into SQL that is correct on the engine that runs it.

This post covers Apache Ossie, the open standard for the first part, ossiec, our compiler for the second, and why the query path in QueryFlux is the right place to connect them.

<!-- truncate -->

## Apache Ossie: one definition of a metric

[Apache Ossie](https://ossie.apache.org/) is a vendor-neutral specification for semantic models, currently incubating at the Apache Software Foundation. It was previously called Open Semantic Interchange (OSI). A model is plain YAML:

- **datasets:** tables or queries, with their primary keys;
- **fields:** columns and expressions;
- **relationships:** how datasets join;
- **metrics:** aggregates such as `SUM(orders.revenue)`.

Each element can also carry `ai_context` (synonyms and descriptions) for agents.

```yaml
datasets:
  - name: orders
    source: orders
    primary_key: [id]
    fields:
      - { name: id, expression: { sql: order_id } }
      - { name: product_id, expression: { sql: product_id } }
  - name: products
    source: products
    primary_key: [id]
    fields:
      - { name: id, expression: { sql: product_id } }
      - { name: stock, expression: { sql: stock } }

relationships:
  - { name: orders_to_products, from: orders, to: products,
      from_columns: [product_id], to_columns: [id] }

metrics:
  - name: total_stock
    expression: { sql: SUM(products.stock) }
    description: Units in stock
```

The point is that the definition belongs to you and not to a tool. You write `total_revenue` once, and every BI tool, notebook and agent that reads Ossie gets the same definition, instead of each one keeping its own copy that slowly drifts.

## A spec is not a query

Ossie defines what a metric *means*. By design, it does not say how to turn "`total_stock` by `customers.region`" into SQL. That is the hard part. The agent's query above used exactly the right tables and columns, and it was still wrong.

So we built [**ossiec**](https://github.com/lakeops-org/ossiec), an open-source (Apache-2.0) compiler for Ossie models. It takes a model and a request and returns one SQL statement:

```json
{"metrics": ["total_stock", "total_revenue"], "dimensions": ["customers.region"]}
```

The rules that make the answer correct live in the compiler instead of in each prompt:

- **No double counting.** Joins only go from many to one, starting at a single root. A measure that reads the "one" side of a join, like `SUM(products.stock)` from orders, is computed over rows de-duplicated on that dataset's primary key, as the Ossie spec describes. That is how 250 becomes 150.
- **Separate facts stay separate.** Store sales and web sales share `item` and `date` but not each other. ossiec computes each fact on its own and stacks the results, instead of joining two fact tables and multiplying both.
- **Ambiguity is an error, not a guess.** If a date can be reached by "first purchase" and "first shipment", ossiec refuses and names the choices, so the caller can pick one (`customer_first_sales.d_year`).
- **Errors speak the model's language.** `unknown metric 'revenue'; available: total_revenue, …` is a message an agent can act on and correct itself.

The output is engine-native SQL for Trino, StarRocks, ClickHouse, Postgres, MySQL, Snowflake, BigQuery and more. Engines differ in subtle ways, so every test case runs on six real engines (DuckDB, Postgres, Trino, StarRocks, ClickHouse and MySQL) and must return the same rows on each. The tests include official TPC-DS queries over generated TPC-DS data.

That testing paid off. On ClickHouse, `lag` returns `0` instead of NULL for the first row. MySQL silently ignores `NULLS LAST`. One ClickHouse release line drops columns from a `DISTINCT` inside a `UNION ALL`, so equal prices quietly merge into one row. Each of these returns plausible-looking rows that are wrong. ossiec has a small, tested fix for each one, so the request means the same thing on every engine.

## Why the query path is the right place

A compiler produces SQL; it doesn't run it. Someone still has to decide which engine runs it, what the caller is allowed to see, and keep a record. QueryFlux already does all three for every other query, which is why semantic queries belong in QueryFlux.

<figure class="blog-figure">
  <img src="/img/blog/ossie-semantic-path.svg" alt="Planned flow: an agent calls query_semantic_model; QueryFlux looks up the Ossie model, routes to an engine, compiles with ossiec for that engine, and applies guardrails, row filters and impersonation before the engine runs it" />
  <figcaption>The planned flow. Routing comes first, so ossiec compiles for the engine that will actually run the query.</figcaption>
</figure>

**Route first, then compile.** QueryFlux picks the engine with the same routing rules it uses for any other query, then asks ossiec for SQL in that engine's own dialect. That way the engine-specific rules apply, and the query isn't translated a second time.

**Semantic queries get no special path.** Once compiled, the SQL goes through the same pipeline as anything else:

- [guardrails](/docs/architecture/guardrails);
- [row filters and column masks](/blog/row-filters-above-the-engines);
- impersonation, so the engine sees the real user;
- query history with [agent context](/docs/agentic/overview).

A metric can't be used to read rows a user isn't allowed to see. A row filter on `customers` applies whether the agent wrote raw SQL or asked for `total_revenue by region`.

**Agents get tools built for metrics.** Alongside the existing [MCP tools](/docs/quickstart/ai-agent-mcp), agents get:

- `list_semantic_models` and `describe_semantic_model`, to find metrics and dimensions, including `ai_context` synonyms;
- `get_dimension_values`, to filter on values that actually exist;
- `query_semantic_model`, to run a request;
- `compile_semantic_query`, to see the SQL without running it.

**Raw SQL becomes your choice.** Some questions fall outside any model. Each deployment decides whether agents may still write raw SQL: `allow`, `warn` or `deny`. Access policies can tighten that setting per user or group, but never loosen it.

## What you can use today, and what's next

**Available now:** [ossiec](https://github.com/lakeops-org/ossiec), as a Rust library and CLI. It reads the current Ossie spec and supports:

- window metrics (running totals, rankings, period-over-period);
- role-playing dimensions;
- independent facts;
- including dimension rows with no matching facts.

You can point it at your own Ossie model and compare its SQL with what you write by hand.

**Next in QueryFlux:**

1. A model registry with the discovery tools above.
2. `query_semantic_model`, compiled for the routed engine and run through the full pipeline.
3. Later, a SQL surface for BI tools: one virtual table per model (`SELECT "region", "total_revenue" FROM semantic.sales`) for tools like Superset.

The goal is the same as the rest of QueryFlux. Agents get one endpoint and the answers they need. You keep control of where queries run, what they may see, and what each metric means.

<div class="blog-cta">

Read the [Apache Ossie spec](https://github.com/apache/ossie/blob/main/core-spec/spec.md), try [ossiec](https://github.com/lakeops-org/ossiec) on your own model, and see how QueryFlux already handles [agents](/blog/agents-sql-not-keys) and [row filters](/blog/row-filters-above-the-engines).

</div>
