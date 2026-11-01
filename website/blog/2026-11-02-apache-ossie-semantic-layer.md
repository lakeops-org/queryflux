---
slug: apache-ossie-semantic-layer
title: "Teach agents your metrics, not your joins"
description: Agent SQL can run, look right and be off by 2x. How ossiec compiles Apache Ossie metrics into correct SQL on six engines, and why QueryFlux is where they run.
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
image: img/blog/ossie-hero.jpg
---

:::tip New to Apache Ossie?

This is part two. [Part one](/blog/apache-ossie-why-metrics-need-a-standard) explains what Ossie is, why metrics need an open standard, and where it fits next to Iceberg and QueryFlux. It's a five-minute read.

:::

Ask an agent for "units in stock by customer region" and you'll get something like this:

```sql
SELECT c.region, SUM(p.stock)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products  p ON o.product_id  = p.product_id
GROUP BY c.region
```

Every table is right. Every join is right. It runs, and it returns this:

| region | agent's SQL | correct |
|---|---:|---:|
| EU | 350 | 150 |
| US | 60 | 30 |

Stock belongs to a product, but the join repeats each product once per *order*. A product ordered three times gets its stock counted three times. No error, no warning, and the number is more than double the truth.

A better prompt won't fix this, and neither will a bigger model. Nothing in the schema says that `total_stock` has to be counted once per product *before* it's grouped through orders, so the join multiplies it.

> **An agent that can write SQL still doesn't know what your metrics mean.**

<!-- truncate -->

## The model knows what the schema doesn't

An [Apache Ossie](https://ossie.apache.org/) model is a YAML (or JSON) document that describes your datasets, how they join, and what each metric means. Two small details in it fix the query above:

```yaml
datasets:
  - name: products
    primary_key: [product_id]    # what "one product" means
relationships:
  - from: orders                 # many orders...
    to: products                 # ...point at one product
metrics:
  - name: total_stock            # SUM(products.stock)
```

The primary key says what counts as one product. The relationship direction says which joins can multiply rows. The agent had neither, so it couldn't have known its answer was wrong.

But Ossie is a definition, not a query engine. It says what `total_stock` means, not how to turn "`total_stock` by region" into SQL that is correct. That gap is the hard part.

## ossiec: from metric to correct SQL

So we built [**ossiec**](https://github.com/lakeops-org/ossiec), an open-source (Apache-2.0) compiler for Ossie models, written in Rust. An agent no longer writes joins. It sends a request:

```json
{"metrics": ["total_stock"], "dimensions": ["customers.region"]}
```

and ossiec returns one SQL statement for the target engine, with the correctness rules built in:

- **No double counting.** When a metric reads the "one" side of a join, like stock from products while grouping through orders, ossiec first reduces it to one row per product, then sums. That's how 350 becomes 150.
- **Separate facts stay separate.** Store sales and web sales are computed on their own and combined, never joined to each other.
- **Ambiguity is an error, not a guess.** ossiec follows the most direct join path. If two paths are equally direct, say a date reached through "first purchase" or through "first shipment", it refuses and names both, and the caller picks one by role (`customer_first_sales.d_year`).
- **Errors an agent can fix.** `unknown metric 'revenue'; available: total_revenue, …` gets fixed in one retry.

<figure class="blog-figure">
  <img src="/img/blog/ossie-fan-trap.svg" alt="EU has five orders over two products. Summing stock over the joined rows counts product 10 twice and product 11 three times, giving 350. ossiec sums each product once, by primary key, giving 150." />
  <figcaption>The join repeats each product once per order. ossiec counts each product once: 100 + 50 = 150.</figcaption>
</figure>

## Same request, same answer, on every engine

ossiec emits native SQL for Trino, StarRocks, ClickHouse, Postgres, MySQL, Snowflake, BigQuery and more. Every test runs on six real engines and must return the same rows on each, including official TPC-DS queries.

That testing caught bugs that never raise an error:

- **ClickHouse** returns `0` instead of `NULL` for the first row of `LAG`, so growth for the first period is computed against zero.
- **MySQL** has no `NULLS LAST`. Translators quietly drop it, and your "top 10" starts with unknowns.
- **ClickHouse 25.8–26.4** can drop a column from a `SELECT DISTINCT` inside a `UNION ALL`, so rows that should stay distinct collapse. That's exactly the shape double-count protection produces ([fixed in 26.5](https://github.com/ClickHouse/ClickHouse/pull/104114)).

ossiec handles each of these, so the same request means the same thing everywhere.

## Why QueryFlux runs it

A compiler produces SQL; it doesn't run it. Someone still has to pick the engine, check what the caller may see, and keep a record. QueryFlux already does that for every query, so that's where semantic queries go. This is the design we're building now:

<figure class="blog-figure">
  <img src="/img/blog/ossie-semantic-path.svg" alt="Planned flow: an agent calls query_semantic_model; QueryFlux looks up the Ossie model, routes to an engine, compiles with ossiec for that engine, and applies guardrails, row filters and impersonation before the engine runs it" />
  <figcaption>The planned flow. QueryFlux picks the engine first, so ossiec compiles for the engine that will actually run the query.</figcaption>
</figure>

- **Route, then compile.** QueryFlux picks the engine, then ossiec writes SQL in that engine's own dialect, with the fixes above intact.
- **No side door.** The SQL goes through the same [guardrails](/docs/architecture/guardrails), [row filters](/blog/row-filters-above-the-engines) and audit as any other query. With access policies configured, a metric can't show rows a user isn't allowed to see.
- **Tools built for metrics.** Agents get MCP tools to list models, look up real dimension values (`'EU'`, not a guessed `'Europe'`) and run `query_semantic_model`.
- **Raw SQL is your call.** Each deployment chooses whether agents may still write their own SQL: `allow`, `warn` or `deny`.

The agent's job shrinks from "write correct SQL over a schema you've never seen" to "pick metrics from a list". That's a job agents can get right.

## Try it

**Available now:** [ossiec](https://github.com/lakeops-org/ossiec), as a Rust library and CLI. Point it at your own Ossie model and compare its SQL with what you'd write by hand. If you find a request where they disagree, open an issue. We want to see it.

**Coming to QueryFlux:** a model registry, metric tools for agents over MCP, and later a SQL surface so BI tools like Superset can query metrics directly.

<div class="blog-cta">

New to Ossie? Start with [part one](/blog/apache-ossie-why-metrics-need-a-standard). Then read the [Apache Ossie spec](https://github.com/apache/ossie/blob/main/core-spec/spec.md), try [ossiec](https://github.com/lakeops-org/ossiec), and see how QueryFlux already handles [agents](/blog/agents-sql-not-keys) and [row filters](/blog/row-filters-above-the-engines).

</div>
