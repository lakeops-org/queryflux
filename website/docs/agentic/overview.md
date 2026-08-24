---
title: Agentic AI
sidebar_label: Overview
description: How QueryFlux supports AI agent workloads — attaching agent identity to queries, replaying sessions, and applying guardrails to agent traffic.
image: img/queryflux-hero-banner.png
---
# Agentic AI

When an AI agent queries through QueryFlux, it can identify itself and attach conversation state to every query. QueryFlux persists this context alongside every query record so you can replay an agent's full session, correlate queries across steps, and audit what the agent tried — including any guardrail decisions.

This section covers:

- **[Setting agent context](agent-context)** — how to attach agent identity to a query, depending on which frontend the agent uses: HTTP headers, SQL session params, or [MCP](../architecture/frontends/mcp) tool parameters.
- **[Session replay and guardrails](session-replay)** — what gets persisted, how to reconstruct an agent's full session from query history, and how guardrails integrate with agentic traffic.

## The three propagation mechanisms

Agentic context can be set in three ways depending on which frontend protocol the agent uses. All three use the same underlying fields and produce identical records in query history.

| Mechanism | Used by |
|-----------|---------|
| **HTTP headers** | Trino HTTP, Snowflake HTTP, ClickHouse HTTP, and [MCP](../architecture/frontends/mcp) frontends. |
| **SQL session params** | MySQL wire and PostgreSQL wire frontends, where HTTP headers are not available. |
| **MCP tool parameters** | The [MCP](../architecture/frontends/mcp) frontend additionally accepts these fields as explicit tool-call arguments, since not every MCP client lets you set custom headers per call. |

See [Setting agent context](agent-context) for the full reference on each.

## Also relevant

- **[MCP Frontend](../architecture/frontends/mcp)** — lets any MCP-compatible agent query QueryFlux directly, with agent context, routing, and guardrails all reused from the rest of QueryFlux.
- **[Guardrails](../architecture/guardrails)** — the general-purpose SQL safety layer that applies to agent traffic exactly like any other client.
