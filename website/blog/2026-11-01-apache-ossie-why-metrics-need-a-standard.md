---
slug: apache-ossie-why-metrics-need-a-standard
title: "Revenue has three definitions. Your agent picked a fourth."
description: Apache Ossie is an open specification for metric definitions. Why BI tools, semantic layers and AI agents need it, and where it fits next to Iceberg and QueryFlux.
authors: [amit]
tags: [Vision]
keywords:
  - Apache Ossie
  - Open Semantic Interchange
  - OSI
  - semantic layer
  - metrics layer
  - AI agents
  - Apache Iceberg
  - Apache Polaris
  - QueryFlux
  - ossiec
image: img/blog/ossie-why-hero.jpg
---

Ask three tools for last quarter's revenue:

| Where you ask | Q3 revenue | What it actually computed |
|---|---:|---|
| Finance dashboard | $4.12M | net of refunds, by invoice date, in UTC |
| dbt metric | $4.37M | gross, by order date |
| Sales BI dashboard | $4.29M | booked orders, including pending, in local time |

All three queries are correct. Each one faithfully computes *a* definition of revenue. They just aren't the same definition, and nothing in the stack knows that.

Now ask an agent. It finds an `orders` table, writes `SUM(order_total)` and answers **$4.61M**. That includes tax and shipping, which is a fourth definition nobody ever agreed on.

This is an old problem. For years it was a slow one: someone spots two numbers that don't match, opens a ticket, and the team reconciles them in a meeting. **AI agents make it fast.** An agent answers hundreds of questions a day, never asks which definition you meant, and doesn't show up to the reconciliation meeting. When no shared definition exists, it does what anyone would do with a schema and a deadline: it makes one up.

> **The fix isn't a smarter agent. It's one definition of revenue that every tool, and every agent, reads.**

That's what [Apache Ossie](https://ossie.apache.org/) is for. This post covers why it exists, where it sits in the data stack, what it deliberately leaves out, and how QueryFlux fills that gap. [Part two](/blog/apache-ossie-semantic-layer) gets into the compiler and the SQL.

<!-- truncate -->

## Semantic layers already exist. That's the problem.

The industry's answer to metric drift was the semantic layer: define `revenue` once, centrally, and have tools query the definition instead of the tables. It works, and that success is how we ended up here.

There's now a semantic layer in nearly every product: the dbt Semantic Layer (MetricFlow), Snowflake semantic views, Databricks metric views, LookML, Cube, Tableau, GoodData and more. Each one is a good place to define a metric. Each one stores it in its own format, and only that tool and its partners can read it.

So "define it once" quietly turned into "define it once per tool". Your finance team's revenue lives in the BI tool, the analytics engineers' revenue lives in dbt, and the agent your platform team shipped last month reads neither. Moving off a tool means rewriting its metrics. The meaning of your business ends up locked inside your vendors.

## The lakehouse already solved this once

If that sounds familiar, it should.

Ten years ago tables had the same problem. A table belonged to the engine that wrote it: Hive tables for Hive, warehouse tables for the warehouse. Using a different engine meant copying the data. Then open table formats, Apache Iceberg above all, separated *the table* from *the engine*. The data stays put, and Trino, Spark, StarRocks, Snowflake and DuckDB all read the same table.

Ossie applies the same move one layer up. It separates *the meaning* from *the tool*:

| Layer | Locked to | Open format | What it decouples |
|---|---|---|---|
| Files | — | Parquet | bytes from the writer |
| Tables | the engine | Apache Iceberg | tables from the engine |
| Catalog | the warehouse | Apache Polaris, Iceberg REST | discovery from the engine |
| **Meaning** | **the BI tool / semantic layer** | **Apache Ossie** | **metrics from the tool** |

The first three rows are mostly settled. The fourth is where the lock-in has moved.

## What Apache Ossie is

Ossie started in 2025 as **Open Semantic Interchange (OSI)**, launched by Snowflake with 17 partners. Its repository opened in November 2025. In 2026 it entered the Apache Incubator as **Apache Ossie (Incubating)**; the rename avoids a clash with other projects called OSI, and the specification itself didn't change. The coalition now counts more than 50 organizations. Contributors include Snowflake, Databricks, Salesforce, dbt Labs, Dremio, RelationalAI, GoodData and Honeydew, and spec changes go through the usual Apache process of public discussion and votes.

Technically, an Ossie model is one YAML (or JSON) document that describes:

- **datasets:** the tables or queries a model reads, optionally with a **primary key** (a compiler needs it to avoid double counting);
- **fields:** columns and expressions, some marked as dimensions or time dimensions;
- **relationships:** which columns join which datasets, and in which direction (many orders to one customer);
- **metrics:** aggregate expressions over those fields, such as `SUM(orders.amount)`;
- **`ai_context`:** descriptions, synonyms and instructions aimed at agents, so "sales" and "turnover" both resolve to `total_revenue`.

Two of those details do more work than they appear to. Primary keys and relationship direction are exactly what a schema doesn't tell you and an agent can't guess. They are the difference between a correct number and a confidently wrong one, as [part two](/blog/apache-ossie-semantic-layer) shows with a 2x error.

Beyond the core spec, the project includes an ontology specification for broader business concepts, a compliance suite, a Python SDK for loading and validating models, and [reference converters](https://github.com/apache/ossie-converters) between Ossie and existing formats such as dbt, Snowflake semantic models, Salesforce, GoodData and Polaris. Working groups cover the metric language, catalog integration and the ontology.

<figure class="blog-figure">
  <img src="/img/blog/ossie-ecosystem.svg" alt="Where Apache Ossie fits: metric definitions from dbt, Snowflake, Salesforce, GoodData and BI tools convert into one Ossie model. In the planned QueryFlux flow, agents, BI tools and notebooks will query through QueryFlux, which will compile the model with ossiec, route, govern and run it on Trino, StarRocks, ClickHouse, Snowflake and other engines over Iceberg tables." />
  <figcaption>Ossie is the interchange point. Converters bring existing definitions in; something on the query path has to turn them into SQL and run it. The QueryFlux part of this picture is planned.</figcaption>
</figure>

## What Ossie deliberately doesn't do

This part is easy to miss, and it's the most important for anyone building on Ossie.

**Ossie defines what a metric means, and what a correct answer is. It doesn't run anything.** It isn't a query engine or a server, and it doesn't generate SQL. Its draft [Relational Query Interface](https://github.com/apache/ossie/blob/main/core-spec/relational_semantics.md) goes a long way on semantics: it requires that aggregates never double count, and it sketches a reference strategy for getting there. But the project ships a loader and validator, not an engine, and its converters translate one format into another. As of today, none of them turns "`total_revenue` by `region`" into a query.

That's the right call for a specification. Iceberg doesn't execute queries either; it lets every engine agree on what a table is. But it leaves a real gap. Between "here is the definition" and "here are the rows", someone has to:

1. **Plan the query.** Pick the joins, avoid double counting across one-to-many relationships, keep separate fact tables separate, and refuse ambiguous requests instead of guessing.
2. **Speak the engine's dialect.** Not just syntax: the same SQL can return different rows on ClickHouse and Postgres.
3. **Pick the engine.** On a lakehouse, the same Iceberg table can be read by Trino, StarRocks or DuckDB, and the right one depends on the query.
4. **Enforce access.** A metric must not become a side door around row-level security.
5. **Keep the record.** Which agent asked for which metric, which SQL ran, and on which engine.

The spec covers *what* item 1 must produce; a separate Wide Table Interface proposal covers how BI tools might ask. Every item on the list still needs an implementation.

## Where QueryFlux fits

QueryFlux is a SQL proxy that already sits between every client and every engine: dashboards, notebooks, applications and [AI agents over MCP](/blog/agents-sql-not-keys) on one side; Trino, StarRocks, ClickHouse, Snowflake, DuckDB and others on the other. It already handles items 3 to 5 for the queries that pass through it: [routing](/docs/architecture/routing-and-clusters), [guardrails](/docs/architecture/guardrails), [row filters and column masks](/blog/row-filters-above-the-engines) where access policies are configured, impersonation and query history.

So the missing pieces are items 1 and 2: turning an Ossie request into a query that is correct on a specific engine. For that we built [**ossiec**](https://github.com/lakeops-org/ossiec), an open-source (Apache-2.0) compiler for Ossie models, written in Rust and tested against six real engines. It's available today.

The plan for QueryFlux, which isn't shipped yet, connects the two:

- **One registry.** Ossie models live in QueryFlux, next to the routing and access policies.
- **Tools built for metrics.** Agents list models, look up metrics and real dimension values, and ask for "`total_revenue` by `region`" instead of writing joins.
- **Route, then compile.** QueryFlux picks the engine first, then asks ossiec for SQL in that engine's own dialect.
- **No special path.** The compiled SQL goes through the same guardrails, row filters and audit as any other query, with whatever policies the deployment has configured.
- **Raw SQL as a policy.** Each deployment chooses whether agents may still write their own SQL: `allow`, `warn` or `deny`.

The result is a split of responsibilities that mirrors the lakehouse. Your metric definitions live in an open format you own. Converters bring in what you already have. The compiler turns them into correct SQL for each supported engine, tested on six of them, and the query path decides where they run and who sees what.

## What's next

[Part two, *Teach agents your metrics, not your joins*](/blog/apache-ossie-semantic-layer), is the technical half. It walks through a fan trap that silently doubles a number, how ossiec avoids it, and three engine bugs that return plausible, wrong rows without raising an error.

<div class="blog-cta">

Read the [Apache Ossie spec](https://github.com/apache/ossie/blob/main/core-spec/spec.md), browse the [converters](https://github.com/apache/ossie-converters), try [ossiec](https://github.com/lakeops-org/ossiec) on your own model, and see how QueryFlux already puts [agents](/blog/agents-sql-not-keys) and [row filters](/blog/row-filters-above-the-engines) on one query path.

</div>
