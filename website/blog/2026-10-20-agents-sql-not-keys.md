---
slug: agents-sql-not-keys
title: "Give agents SQL, not the keys to every engine"
description: AI agents need live data, but a warehouse password and an open SQL prompt is the wrong interface. How to put agents behind QueryFlux, with routing, guardrails, identity and a record of every query, in one config file.
authors: [amit]
tags: [agents, mcp, guardrails, security]
image: img/queryflux-hero-banner.png
---

The fastest way to give an AI agent data is also the worst one: hand it a warehouse connection string and let it write SQL. It works in a demo. In production it means one shared credential for every agent, no record of which agent ran what, and nothing between a generated `SELECT *` and your largest table.

Agents shouldn't choose engines, and they shouldn't hold warehouse passwords. Something in the query path should decide where each query runs, what it may do, and keep the record. This post shows how to set that up with QueryFlux, using features that ship today.

<!-- truncate -->

## The setup

One QueryFlux config gives agents their own entry point, their own capacity, their own guardrails, and their user's identity on the engine:

```yaml title="config.yaml"
queryflux:
  frontends:
    mcp:
      enabled: true
      port: 8811

clusters:
  trino-1:
    engine: trino
    endpoint: https://trino.internal:8443
    auth:                  # QueryFlux's own service credential
      type: basic
      username: qf_svc
      password: "..."
    queryAuth:             # the identity each query runs as
      type: impersonate

clusterGroups:
  analysts: { maxRunningQueries: 40, members: [trino-1] }
  agents:   { maxRunningQueries: 8,  members: [trino-1] }

routers:
  - type: protocolBased
    mcp: agents            # everything that arrives over MCP
routingFallback: analysts

guardrails:
  groups:
    agents:
      - kind: built_in
        name: read_only
      - kind: built_in
        name: row_limit
        max_rows: 5000
      - kind: built_in
        name: require_predicate
        applies_to: ["fct_*", "events.*"]
```

Here is what each part does.

## 1. One endpoint, any agent

Agents connect over MCP (streamable HTTP):

```bash
claude mcp add --transport http queryflux http://localhost:8811/mcp
```

Cursor and Claude Desktop take the same URL. The agent gets tools to run a query, list schemas, describe a table, read the engine's plan, and check or cancel what it started. Agents that prefer a SQL driver can connect over Trino, PostgreSQL or MySQL instead; everything below applies to them too, because none of it lives in the MCP layer.

## 2. Agents get their own lane

The `protocolBased` router sends every MCP query to the `agents` group, which has its own `maxRunningQueries`. A burst of exploratory agent queries queues in its own lane instead of starving the dashboards in `analysts`. With Postgres persistence, the limit holds across every QueryFlux replica and worker, and waiting queries are served first come, first served.

## 3. Guardrails in the query path

The `agents` group gets three built-in guards on top of any global ones:

- `read_only` blocks anything that isn't a `SELECT`, `WITH`, `SHOW`, `DESCRIBE` or `EXPLAIN`.
- `row_limit` denies queries whose outer `LIMIT` exceeds 5,000 rows, and records a warning when there is no `LIMIT` at all.
- `require_predicate` rejects queries on fact and event tables that have no `WHERE` clause.

A blocked query gets an error explaining which rule it broke, so the agent can rewrite and retry. When the built-ins aren't enough, add a Python guard or call your own webhook; both receive the agent's context.

These run in QueryFlux itself, before the query reaches an engine. That is the point: a guardrail that lives in an MCP wrapper only holds as long as the agent uses that wrapper. One in the data path holds however the agent connects.

## 4. The engine sees the person, not a shared account

With `queryAuth: impersonate`, QueryFlux authenticates to Trino with its own service account but runs each query as the user who sent it. When an agent connects with a person's token, the engine applies that person's permissions and writes that person into its own audit log. There is no shared password for an agent to leak or overreach with.

Trino and ClickHouse support impersonation. For Snowflake and Trino, QueryFlux can instead exchange the caller's OAuth token for one the engine accepts, or pass the caller's own credential straight through.

For finer control, add row filters and column masks from OPA or Cerbos. They apply to agents exactly as they apply to people.

## 5. Every query has an owner

Each query QueryFlux runs is stored with the agent, conversation, step and tool call that produced it. Over MCP these are filled in for you: the agent from the authenticated user, the conversation from the MCP session. Over Trino HTTP, the agent sends `X-Agent-Id` and `X-Conversation-Id` headers.

In Studio, the **Agents** and **Conversations** pages group history by agent and session, so you can replay a conversation step by step: every query, where it ran, how long it took, and which guardrail stopped it.

## What this adds up to

An agent gets one endpoint and the SQL it needs. You keep control of:

- which engine runs each query;
- how much of the cluster agents can use;
- what agents may do;
- whose permissions apply on the engine;
- a complete record of what happened.

It works the same over MCP and over a SQL driver, on the engines you already run.

Start with the [MCP quickstart](/docs/quickstart/ai-agent-mcp), then read [guardrails](/docs/architecture/guardrails), [agent context](/docs/agentic/overview) and [authentication](/docs/authentication).
