---
slug: ossie-iceberg-playbook
title: "Iceberg didn't win because it was a file format"
description: Apache Ossie is retracing Iceberg's path to an open standard. Iceberg won on defined behavior and many implementations. Here's why we built ossiec.
authors: [amit]
tags: [Vision]
keywords:
  - Apache Ossie
  - Apache Iceberg
  - Open Semantic Interchange
  - open standards
  - semantic layer
  - vendor lock-in
  - Power BI
  - ossiec
  - QueryFlux
image: img/blog/ossie-iceberg-playbook-hero.jpg
---

In October, Microsoft and Google [joined Apache Ossie](https://www.infoworld.com/article/4229785/), the open specification for metric definitions. Microsoft engineers contributed a two-way converter between Power BI semantic models and Ossie. Snowflake, Databricks, Salesforce, dbt Labs, Oracle and Starburst are already on the project's [ecosystem page](https://ossie.apache.org/ecosystem/), along with about eighty other organizations.

Companies that compete on every deal agreeing to share one format is rare in data. The last time it happened was with tables, and the format was Apache Iceberg.

So it's tempting to say Ossie is "Iceberg for metrics" and stop there. We think the comparison is right, and that it's more useful as a checklist than as a slogan. Iceberg didn't win just because it was an open format. It won because its spec said what every engine must do with a table, and because anyone could implement that spec, and many did.

> **A standard lets definitions move between tools. It only removes lock-in when every tool gives the same answer.**

That second part is why we built [ossiec](https://github.com/lakeops-org/ossiec).

<!-- truncate -->

## What Iceberg actually fixed

Before Iceberg, a Hive table was a directory. The table was whatever files happened to be in it, and its behavior was whatever the engine reading it did. Rename a column, and some engines matched columns by name and others by position, so the same query could return different data depending on which engine ran it. Writes to object storage weren't atomic, so a reader could see half of a commit. The alternative was a warehouse's own storage, which only that warehouse could read.

Netflix started Iceberg in 2017 to fix the first problem, and in doing so fixed the second. Three things made it last:

**1. A spec, not a product.** Iceberg is defined by a document that says what a table is and what readers and writers must do. Columns are tracked by ID, so a rename is safe in every engine that follows the spec. Commits are atomic. Row-level deletes have defined semantics. "What does this table contain?" has one answer, written down.

**2. A home that no single vendor owns.** Iceberg entered the Apache Incubator in 2018 and became a top-level project in 2020. During the table-format years, with Delta Lake and Apache Hudi as the alternatives, that neutrality counted. By 2024 the two biggest rivals in the market had both backed it: Databricks bought Tabular, the company founded by Iceberg's creators, and Snowflake open-sourced Polaris, an Iceberg catalog, which then went to Apache too.

**3. Many independent implementations.** There's the Java reference implementation, plus PyIceberg, iceberg-rust, iceberg-go and iceberg-cpp, and every major engine has its own reader. Because they're independent, the spec can't quietly mean "whatever the reference code does". When two implementations disagree, someone finds out, and either the spec or one of the implementations gets fixed.

The first point gets the most attention. The other two are why buyers trusted it.

## Metrics are where tables were in 2015

Today a metric definition belongs to the tool that holds it. `revenue` lives in LookML, a dbt semantic model, a Snowflake semantic view, a Databricks metric view or a Power BI dataset. Each is a good place to define it, and each stores it in its own format. [Part one of our Ossie series](/blog/apache-ossie-why-metrics-need-a-standard) went through what that costs, and AI agents make it worse, because they answer far more questions than people do.

Ossie is following Iceberg's path closely:

| Step | Apache Iceberg | Apache Ossie |
|---|---|---|
| The problem | Hive directories, warehouse-only storage | LookML, dbt, semantic views, metric views, Power BI datasets |
| Started | Netflix, 2017 | Snowflake and 16 partners, as Open Semantic Interchange, September 2025 |
| Neutral home | Apache Incubator 2018, top-level project 2020 | Apache Incubator, July 2026 |
| Rivals sign on | Databricks and Snowflake, 2024 | Databricks, Salesforce, dbt Labs; Microsoft and Google, October 2026 |
| In the catalog | Iceberg REST catalog, Apache Polaris | Polaris [Semantic Model API](https://iceberglakehouse.com/posts/apache-ossie-polaris-semantic-models/), beta and off by default |
| Behavior defined | The spec says what readers must do | In progress: a query interface [merged on November 4](https://github.com/apache/ossie/pull/653), with more in draft |
| Independent implementations | Five languages, plus every engine | A handful |

Ossie's first four rows took about a year. For Iceberg they took about seven years. That's the coalition working.

The last two rows are where Ossie still has to catch up.

## Portable definitions, locked behavior

Most of Ossie's ecosystem today is **converters**. Export a Snowflake semantic view to Ossie. Import Ossie into Power BI as TMDL. Convert a dbt model. These are useful, because they make definitions portable, and portability is the first step.

But follow what happens when someone asks a question. To get a number from an Ossie model, you usually convert it into a vendor's semantic layer and let that vendor answer. The definition is open, but the thing that computes the result usually isn't. Analysts covering the Microsoft and Google news [said the same thing](https://www.infoworld.com/article/4229785/). Stephanie Walter of HyperFrame Research: *"A portable structure does not guarantee equivalent behavior."* Ashish Chaturvedi of HFS Research expects new forms of lock-in to emerge, because each engine will keep its own behavior.

If Iceberg had shipped this way, it would have been a format you convert into Delta or into Snowflake's storage before you could read it. The convenience is real, but the lock-in is still there, one step later in the process.

Here's what "behavior" means for a metric. Take one Ossie model with `orders`, `customers` and `products`, and ask for units in stock by customer region:

- A tool that joins the tables and sums `stock` returns **350** for EU, because each product is counted once per order.
- A tool that takes the joins as written and counts each product once, using the model's primary keys, returns **150**. In the spec, that's a Layer 2 query: SQL with `AGG(measure)`.
- A tool that takes the question as a list of metrics and dimensions, the spec's Layer 3 interface, refuses: stock belongs to products, and there's no many-to-one path from products to customers.

All three read the same YAML file, and all three can say they support Ossie. [Part two](/blog/apache-ossie-semantic-layer) walks through this case in detail. Underneath that, engines differ in smaller ways too. ClickHouse's `lag` returns `0` where Postgres returns `NULL`, and MySQL has no `NULLS LAST`. So the same SQL can return different rows on different engines.

The Ossie community is working on exactly this. A Layer 2 relational interface (SQL with `AGG(measure)`, based on Julian Hyde and John Fremlin's [*Measures in SQL*](https://arxiv.org/pdf/2406.00251)) is in draft. A Layer 3 interface for requests that list metrics and dimensions was merged on November 4. That's the equivalent of Iceberg's read semantics being written. The other thing Iceberg had, many independent implementations testing the spec, has to come from people building them.

## Why we built ossiec

We started from a simple position. QueryFlux sits between every client and every engine, and we wanted it to understand metrics. We didn't want to invent a metric format, and we didn't want QueryFlux to depend on one vendor's semantic layer. That's the same reason we route to Trino, StarRocks, Snowflake and DuckDB over Iceberg tables, rather than picking one engine. Ossie was the obvious standard to adopt.

But adopting a standard means running it. We needed something that takes an Ossie model and a request and returns SQL that's correct on whichever engine QueryFlux picked. It had to be a library we could embed, not a server, and it had to work across engines. A few Ossie query engines already exist, such as duckdb-ossie, Saiku and Datus, and that's a good sign. Each is built around one engine or as its own service, and we needed a library that works with all of QueryFlux's engines. So we wrote one and published it under Apache-2.0. We applied Iceberg's lessons directly:

**Follow the spec, not our own ideas.** ossiec reads Ossie models as the spec defines them, and every Ossie example and compliance fixture loads. It supports both query interfaces: Layer 3 requests that list metrics and dimensions, and Layer 2 SQL with `AGG(measure)`. When the spec says a request must be rejected, ossiec rejects it with the spec's own error code, such as `E_NO_PATH`, rather than guessing. A wrong number that looks plausible is the worst thing a metric tool can return.

**The same answer on every engine.** This is Iceberg's main promise, applied to metrics. Every correctness test in ossiec runs on six real engines (DuckDB, Postgres, Trino, StarRocks, ClickHouse and MySQL), and they must all return the same rows. When an engine differs, a small dialect rule fixes it, and a test fails if the rule stops working. The same model, request and options always produce the same SQL, byte for byte. Two limits remain. Snowflake, BigQuery and Redshift are only compile-checked so far, not run. And truncating a time zone-aware timestamp uses each engine's session time zone, so sessions set to different zones can put rows in different groups.

**A library anyone can embed.** iceberg-rust lets an engine read Iceberg without running a service. ossiec does the same for Ossie. It's a Rust crate and a CLI with five dependencies, no runtime and no Python. QueryFlux is one place to embed it. An engine, a BI tool or an agent framework could embed it too.

**Another independent implementation.** ossiec is useful even if you never run QueryFlux, because it's one more implementation checking the spec. Each one that gets built, by us or anyone else, makes it harder for the spec to drift towards "whatever one product does". When implementing ossiec raises a question the spec doesn't answer, the answer belongs upstream in Ossie, not in our code.

## Where QueryFlux fits

Iceberg let any engine read the same table. QueryFlux lets any client reach any engine through one endpoint, with routing, guardrails, [row filters](/blog/row-filters-above-the-engines) and query history. With Ossie models and ossiec in the query path, the plan is for an agent, a notebook and eventually a BI tool to ask for `total_revenue` by `region` and get the same answer, compiled for whichever engine runs it. That integration is being built. The compiler is available today.

There's also something the spec deliberately leaves out. Ossie [doesn't cover row-level security](https://www.infoworld.com/article/4229785/), and it probably shouldn't, any more than Iceberg's spec covers who may read a table. That job belongs in the query path, and the query path is what QueryFlux is.

## What could still go wrong

Being honest about this is part of the argument:

- **The spec is young.** Ossie hasn't made a release yet; the first is planned as `0.3.0-incubating`. The query semantics are partly merged and partly in draft. Anything built on it today, ossiec included, will change with it.
- **Converters can lose meaning.** A Power BI model converted to Ossie keeps features Ossie can't represent in a separate, tool-specific section, which other tools ignore. A converted model can look complete and still be missing pieces.
- **Vendor languages can carry vendor behavior.** Ossie now recognizes DAX for Power BI measures. That's good for getting Microsoft's models in, but a metric written in DAX only gives the same answer in a tool that evaluates DAX the same way.
- **Standards take time.** Iceberg took seven years to go from Netflix to having Snowflake and Databricks both on board. Ossie is fourteen months old.

None of this argues against Ossie. It says where the work is. Iceberg got its read semantics written down, and then implementations kept everyone to them. Ossie's semantics are being written now. Implementations are what turn them into a guarantee rather than an intention.

<div class="blog-cta">

Read the [Apache Ossie spec](https://github.com/apache/ossie/blob/main/core-spec/spec.md) and join the discussion on the [mailing lists](https://ossie.apache.org/). Try [ossiec](https://github.com/lakeops-org/ossiec) on your own model, and if it gives an answer that doesn't match the spec, [open an issue](https://github.com/lakeops-org/ossiec/issues). That's how independent implementations are supposed to work.

</div>
