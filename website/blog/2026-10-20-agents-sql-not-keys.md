---
slug: agents-sql-not-keys
title: "Give agents SQL, not the keys to every engine"
description: AI agents need live data, but a warehouse password and an open SQL prompt is the wrong interface. How to put agents behind QueryFlux, with routing, guardrails, identity and a record of every query, in one config file.
authors: [amit]
tags: [Guides]
image: img/blog/agents-sql-hero.jpg
---

The fastest way to give an AI agent data is also the worst one: hand it a warehouse connection string and let it write SQL. It works in a demo. In production it means one shared credential for every agent, no record of which agent ran what, and nothing between a generated `SELECT *` and your largest table.

> **Agents shouldn't choose engines, and they shouldn't hold warehouse passwords.** Something in the query path should decide where each query runs, what it may do, and keep the record.

This post shows how to set that up with QueryFlux using features that ship today.

<!-- truncate -->

<figure class="blog-figure">
  <img src="/img/blog/agent-query-path.svg" alt="Flow diagram: agent connects via MCP to QueryFlux, which applies guardrails and impersonation before Trino, with query history in Studio" />
  <figcaption>Agents see one endpoint. Operators keep routing, limits, guardrails, identity, and audit in the proxy.</figcaption>
</figure>

## The setup in one file

One QueryFlux config gives agents their own entry point, capacity pool, guardrails, and per-user identity on the engine:

<figure class="blog-figure">
  <img src="/img/blog/agents-mcp-config.svg" alt="QueryFlux YAML: MCP frontend on port 8811, trino cluster with impersonation, separate analysts and agents cluster groups, protocol router sending MCP to agents group, read_only row_limit and require_predicate guardrails" />
  <figcaption>Full example — adjust endpoints and limits for your environment. See <a href="/docs/configuration">configuration</a> and <a href="/docs/quickstart/ai-agent-mcp">MCP quickstart</a>.</figcaption>
</figure>

The sections below map directly to that file.

## 1. One endpoint, any agent

Agents connect over MCP (streamable HTTP):

<figure class="blog-figure">
  <img src="/img/blog/mcp-connect.svg" alt="Terminal command: claude mcp add with HTTP transport to localhost QueryFlux MCP endpoint" />
  <figcaption>Cursor and Claude Desktop use the same MCP URL on port 8811.</figcaption>
</figure>

The agent gets tools to run a query, list schemas, describe a table, read the engine's plan, and check or cancel what it started.

Agents that prefer a SQL driver can connect over Trino, PostgreSQL, or MySQL instead — routing, guardrails, impersonation, and audit work the same because they live in the query path, not in the MCP layer.

## 2. Agents get their own lane

The `protocolBased` router sends every MCP query to the `agents` group, which has its own `maxRunningQueries`. A burst of exploratory agent queries queues in its own lane instead of starving dashboards in `analysts`.

With Postgres persistence, the limit holds across every QueryFlux replica and worker, and waiting queries are served first come, first served.

## 3. Guardrails in the query path

The `agents` group gets three built-in guards on top of any global ones:

| Guard | Effect |
| --- | --- |
| `read_only` | Blocks anything that isn't `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, or `EXPLAIN` |
| `row_limit` | Denies queries whose outer `LIMIT` exceeds 5,000 rows; warns when there is no `LIMIT` |
| `require_predicate` | Rejects scans of fact/event tables with no `WHERE` |

A blocked query returns an error naming the rule that failed, so the agent can rewrite and retry. When the built-ins aren't enough, add a Python guard or call your own webhook — both receive the agent's context.

:::tip Why guardrails belong in QueryFlux
A rule in an MCP wrapper only holds while the agent uses that wrapper. A rule in the **data path** holds for MCP, SQL drivers, and anything else that speaks to the proxy.
:::

## 4. The engine sees the person, not a shared account

With `queryAuth: impersonate`, QueryFlux authenticates to Trino with its own service account but runs each query as the user who sent it. When an agent connects with a person's token, the engine applies that person's permissions and writes that person into its audit log. There is no shared password for an agent to leak or overreach with.

Trino and ClickHouse support impersonation. For Snowflake and Trino, QueryFlux can instead exchange the caller's OAuth token for one the engine accepts, or pass the caller's credential through.

For finer control, add row filters and column masks from OPA or Cerbos — they apply to agents exactly as they apply to people.

## 5. Every query has an owner

Each query QueryFlux runs is stored with the agent, conversation, step, and tool call that produced it. Over MCP these fields are filled automatically from the authenticated user and MCP session. Over Trino HTTP, send `X-Agent-Id` and `X-Conversation-Id` headers.

In Studio, the **Agents** and **Conversations** pages group history by agent and session so you can replay a conversation step by step: every query, where it ran, how long it took, and which guardrail stopped it.

## What this adds up to

An agent gets one endpoint and the SQL it needs. You keep control of:

- which engine runs each query;
- how much of the cluster agents can use;
- what agents may do;
- whose permissions apply on the engine;
- a complete record of what happened.

It works the same over MCP and over a SQL driver, on the engines you already run.

<div class="blog-cta">

Start with the [MCP quickstart](/docs/quickstart/ai-agent-mcp), then read [guardrails](/docs/architecture/guardrails), [agent context](/docs/agentic/overview), and [authentication](/docs/authentication).

</div>
