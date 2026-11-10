---
slug: your-llm-speaks-postgres
title: "Your LLM speaks Postgres. Your engines don't."
description: We ran 20 SQL idioms models love on Trino, DuckDB, ClickHouse and StarRocks. Translation fixed most errors and added new wrong answers that raise no error.
authors: [amit]
tags: [Vision]
keywords:
  - AI agents
  - text-to-SQL
  - LLM SQL
  - SQL dialects
  - sqlglot
  - SQL translation
  - MCP
  - QueryFlux
image: img/blog/llm-speaks-postgres-hero.jpg
---

Ask a model for SQL and you'll usually get Postgres. Sometimes Snowflake. It writes `created_at::date`, `ILIKE`, `DISTINCT ON`, `IFF`, `QUALIFY`, because that's what most of the SQL on the internet looks like.

Your agent's queries, though, run on whatever is behind the endpoint: Trino over the lake, DuckDB on a laptop, ClickHouse for events, StarRocks for dashboards. A proxy that translates between dialects sounds like the fix, and QueryFlux has one. So we measured what it fixes, and what it doesn't.

> **An error costs the agent a retry. A wrong answer costs someone a decision, and nothing tells them it's wrong.**

Translation got rid of most of the errors. It also produced more wrong answers than sending the SQL unchanged did, and none of them raised an error.

<!-- truncate -->

## How we ran it

We wrote 20 queries in the style models tend to use: 13 in Postgres and 7 in Snowflake. Each one is a common idiom, such as a cast, a date bucket, a median, a top row per group or a ratio. We ran them against a 24-row `orders` table loaded into four engines:

- Trino 484
- DuckDB 2.0.0
- ClickHouse 26.8
- StarRocks 4.1.6

Each query ran four ways:

1. **As written.** This is what the [MCP frontend](/docs/architecture/frontends/mcp#the-dialect-parameter) does when the agent doesn't pass `dialect`: QueryFlux sends the SQL as it is.
2. **Translated.** The agent passes `dialect`, and no catalog is configured, so sqlglot translates on syntax alone.
3. **Translated with schema.** A catalog is configured, so sqlglot also knows the column types.
4. **Strict.** The same as 3, with `translation.mode: strict`.

We made the same sqlglot calls QueryFlux makes, with the version we pin (30.24.0). We also ran a sample through a real QueryFlux MCP endpoint with embedded DuckDB, and it matched.

We checked every result against the answer the source dialect gives. For Postgres queries, that's the same SQL run on Postgres 16. We had no Snowflake account, so for Snowflake queries we wrote the equivalent query in Postgres from Snowflake's documented behaviour. Numbers are compared to four decimal places.

**We chose the idioms, so this doesn't measure how often models write them.** It shows what happens when they do.

<figure class="blog-figure">
  <img src="/img/blog/llm-dialects-scoreboard.svg" alt="Stacked bars of 80 runs each. As written: 33 right, 38 errors, 9 wrong answers. Translated without a catalog: 59 right, 3 order lost, 5 errors, 13 wrong. With schema: 65 right, 3 order lost, 5 errors, 7 wrong. Strict: 61 right, 3 order lost, 5 errors, 7 rejected, 4 wrong." />
  <figcaption>Each row is the same 80 runs: 20 queries on 4 engines. "Order lost" means the right rows came back, but not in the order the query asked for.</figcaption>
</figure>

## As written: lots of errors, and that's the good case

Sending model-written SQL unchanged failed 38 times out of 80. Trino doesn't know `::`. StarRocks doesn't know `ILIKE`. Nobody but Snowflake knows `IFF`. That looks bad, but an error is something an agent can recover from. It reads the message, rewrites and tries again.

The 9 that ran and returned the wrong answer are the problem. The clearest one was on StarRocks:

```sql
SELECT customer || ' (' || region || ')' AS label FROM orders
```

In Postgres that builds `globex (EU)`. In StarRocks, as in MySQL, `||` is logical OR by default, so every row came back `NULL`. There was no error, just a column of nulls for the agent to explain.

## Translation fixes the loud failures and creates quiet ones

Declaring the dialect turned 24 errors into right answers, and 3 more into the right rows in the wrong order. sqlglot handles the common cases well: `::date` becomes `CAST(... AS DATE)`, `ILIKE` becomes `LOWER(...) LIKE LOWER(...)` on Trino and StarRocks, `||` becomes `CONCAT`, and `QUALIFY` becomes a subquery.

But wrong answers went **up**, from 9 to 13. Six queries that used to fail now returned wrong results, and only two wrong answers were fixed. Fewer errors meant fewer chances for anyone to notice. Here are the ones that surprised us.

**A date format that isn't one.** The model wrote Snowflake:

```sql
SELECT TO_VARCHAR(created_at, 'YYYY-MM') AS ym, COUNT(*) AS n FROM orders GROUP BY 1
```

For Trino, the translation was `DATE_FORMAT(created_at, 'YYYY-MM')`. Trino's `DATE_FORMAT` expects `%Y-%m`, and a pattern with no `%` is printed as it is. The monthly breakdown came back as one row:

```text
YYYY-MM	24
```

On DuckDB, ClickHouse and StarRocks the format was dropped and the call became a plain `CAST`, so "per month" turned into one group per timestamp. The same idea written in Postgres (`to_char(created_at, 'YYYY-MM')`) translated correctly to `'%Y-%m'` on all four engines. The intent was the same, but the source dialect decided the outcome.

**45 days became 10,971.** The Postgres idiom for the span between two timestamps is:

```sql
SELECT round(extract(epoch FROM max(created_at) - min(created_at)) / 86400) FROM orders
```

The StarRocks translation is `UNIX_TIMESTAMP(MAX(created_at) - MIN(created_at))`. StarRocks subtracts two `DATETIME`s as if they were numbers, so the result was `20261021155600 − 20260906132200 = 115023400`. That number then went through `UNIX_TIMESTAMP` and came out as **10,971 days**. On Trino the same translation failed with a cast error. That's the better failure.

**A median that's an estimate.** `percentile_cont(0.5) WITHIN GROUP (ORDER BY amount)` became `APPROX_PERCENTILE(amount, 0.5)` on Trino, which has no exact equivalent. The true median is 107.015, and Trino returned 109.19. The result is close, plausible and wrong, and nothing in it says "approximately". Even DuckDB, which runs the original as written, returned 107.01, because its median of a `DECIMAL(10,2)` column keeps the column's two decimal places.

## Same SQL, two meanings

This query is valid, unchanged, in Postgres, Snowflake, Trino, DuckDB, ClickHouse and StarRocks:

```sql
SELECT SUM(CASE WHEN status = 'Delayed' THEN 1 ELSE 0 END) / COUNT(*) AS late_rate FROM orders
```

It doesn't mean the same thing in all of them. Postgres and Trino divide integers as integers and return **0**. Snowflake, DuckDB, ClickHouse and StarRocks return **0.1667**. No parser can spot the difference, because the text is identical.

So `dialect` isn't really about syntax. It tells QueryFlux what the query *means*:

| Model wrote | `dialect` passed | Engine | Catalog | Result | Source dialect says |
| --- | --- | --- | --- | --- | --- |
| Postgres | `postgres` | DuckDB, ClickHouse, StarRocks | no | 0.1667 | 0 ✗ |
| Postgres | `postgres` | DuckDB, ClickHouse, StarRocks | yes | 0, via `CAST(TRUNC(...) AS BIGINT)` and similar | 0 ✓ |
| Snowflake | none | Trino | n/a | 0 | 0.1667 ✗ |
| Snowflake | `snowflake` | Trino | either | 0.1667, via `CAST(... AS DOUBLE)` | 0.1667 ✓ |

Without column types, sqlglot can't tell that `SUM(...)` is an integer, so the Postgres-to-DuckDB translation left `/` alone. With a catalog, it knew the types and kept Postgres's integer division. This is the main reason the schema-aware row in the chart has 7 wrong answers and not 13.

Look at what "correct" means here, though. With a catalog, QueryFlux faithfully reproduced the Postgres answer of **0**, but the model almost certainly wanted a late rate of 16.7%. **Translation is faithful to the dialect, not to what the model meant.** If a model writes Postgres out of habit while thinking in Snowflake, a correct translator carries that mistake into every engine.

## Strict mode filters; it doesn't verify

Strict mode tells sqlglot to raise an error instead of skipping a construct it can't translate. It brought silent wrong answers down to 4, and it refused 7 queries before they reached an engine. Here's what the refusals were:

- **3 real catches.** `TO_VARCHAR` with a format, aimed at DuckDB, ClickHouse and StarRocks: `Argument 'format' is not supported for expression 'ToChar' when targeting DuckDB.`
- **4 false alarms.** These were ordinary queries like `GROUP BY 1 ORDER BY 1` aimed at StarRocks. Postgres sorts `NULL` last by default, and sqlglot can't express that next to a positional `ORDER BY`, so it refused queries that worked perfectly well in best-effort mode.

The 4 wrong answers that got through were the Trino `DATE_FORMAT`, the Trino approximate median, the StarRocks epoch and DuckDB's rounded median. Strict mode catches what sqlglot *knows* it can't translate. It can't catch what sqlglot gets wrong without realising it.

## What the run caught in our own stack

As in [our last post](/blog/busiest-sql-user-has-amnesia), following the queries turned up places where the behaviour doesn't match what we'd assumed:

| What we found | Evidence | What to do for now |
| --- | --- | --- |
| **Snowflake-dialect SQL didn't get schema-aware translation.** sqlglot's optimizer upper-cases unquoted Snowflake identifiers, and our schema used lowercase names, as Trino and Iceberg catalogs report them. | All 7 Snowflake queries failed the optimizer with errors like `Column 'AMOUNT' could not be resolved`, and fell back to translation without types. | Look for `translation.reason = optimize_error` in query history, and `queryflux_translation_fallback_dialect_only_total` in metrics. |
| **`DISTINCT ON` and `QUALIFY` lose their final ordering on Trino.** The rewrite into a `ROW_NUMBER()` subquery puts the `ORDER BY` inside the window and not on the outer query. | The right 6 rows, in no particular order. | If order matters, ask agents for `ROW_NUMBER()` in a subquery with an explicit outer `ORDER BY`. |
| **Strict mode refuses fine queries headed for StarRocks.** | 4 of 7 strict refusals were queries that returned correct results in best-effort mode. | Use strict mode per environment, and check what it refuses before turning it on everywhere. |

These are sqlglot behaviours, not QueryFlux bugs, but QueryFlux is the one sending the result to your engine, so we treat them as ours.

## Make the dialect a contract

None of this is a reason to stop translating. Going from 33 right answers to 65 is a big improvement. It's a reason to treat the dialect as part of the contract between the model and the proxy:

1. **Choose one dialect for your agents, and say it twice.** Name it in the system prompt ("write PostgreSQL") and pass the same value as `dialect` on every `execute_query`. A declaration the model doesn't actually follow is worse than none.
2. **Configure a catalog.** Types are what turn `/` into the right kind of division. Without them, translation only works on syntax.
3. **Watch for fallbacks.** Query history records `translation.status` and `reason` on every query. A sudden increase in `fallback` means the agents are writing something your setup can't fully translate.
4. **Turn known silent failures into loud ones.** A [fixup script](/docs/architecture/query-translation#python-transform-scripts) that raises an exception sends the original SQL through in best-effort mode, where Trino rejects `TO_VARCHAR`, and rejects the query outright in strict mode. Both outcomes beat `YYYY-MM | 24`:

   ```python
   import sqlglot
   import sqlglot.expressions as exp

   def transform(sql: str, src: str, dst: str) -> str:
       # In Trino, a DATE_FORMAT pattern with no % comes back as a literal.
       if dst != "trino":
           return sql
       for fn in sqlglot.parse_one(sql, dialect=dst).find_all(exp.TimeToStr):
           fmt = fn.args.get("format")
           if isinstance(fmt, exp.Literal) and "%" not in fmt.this:
               raise ValueError(f"date format {fmt.this!r} has no % directives")
       return sql
   ```

5. **Keep a set of golden questions.** Choose ten questions whose answers you already know, ask them through the proxy on every engine an agent can reach, and run them again whenever you upgrade an engine, sqlglot or QueryFlux. That's all this post did.

Models will keep writing the SQL they've seen most often. The proxy is the one place that knows both what the model wrote and where it's going to run, so that's where to check that the answer still means the same thing.

<div class="blog-cta">

Read about [query translation](/docs/architecture/query-translation) and the MCP [`dialect` parameter](/docs/architecture/frontends/mcp#the-dialect-parameter), then try it with the [MCP quickstart](/docs/quickstart/ai-agent-mcp).

</div>
