# QueryFlux + MCP

The MCP (Model Context Protocol) frontend, backed by the embedded DuckDB engine — no
Trino, Postgres, or any other external service. The fastest way to go from zero to
"an AI agent is running SQL through QueryFlux."

This example runs with `auth.provider: none` for simplicity. See
**[Authentication](../../website/docs/authentication.md)** for real deployments
(`oidc`, `static`, `ldap`), and
**[`examples/with-keycloak-oidc`](../with-keycloak-oidc/)** for a runnable OIDC setup —
the same pattern applies to the MCP frontend, just point an MCP client's `Authorization`
header at a token from your IdP instead of `none`.

## What's included

| Service | URL |
|---------|-----|
| MCP endpoint (streamable HTTP) | `http://localhost:8811/mcp` |
| Admin API | `http://localhost:9000` |
| Studio | `http://localhost:3000` |

There's no bearer token to configure — `auth.provider: none` accepts any (or no)
`Authorization` header.

## Start

```bash
docker compose up -d --wait
```

## Connect a client

Full setup instructions (Cursor, Claude Code, Claude Desktop, MCP Inspector, generic
`mcpServers` config) are in
**[the MCP frontend docs](../../website/docs/architecture/frontends/mcp.md#connecting-a-client)**.
Point any of them at `http://localhost:8811/mcp` — since this example has no auth, you
can omit the `Authorization` header entirely, or set it to any placeholder value.

Quickest check, with the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
npx @modelcontextprotocol/inspector
# Transport: Streamable HTTP
# URL: http://localhost:8811/mcp
```

## Try it

Once connected, ask your agent to run something like:

> Create a table called `orders` with a few sample rows, describe its columns, then
> show me everything in it.

That exercises `execute_query` (CREATE + INSERT), `describe_table`, and `execute_query`
again (SELECT) in sequence — a good first look at all six tools working together. Or run
the same steps directly against `execute_query` yourself via the Inspector:

```sql
CREATE TABLE orders AS
SELECT * FROM (VALUES (1, 'widget', 9.99), (2, 'gadget', 19.99)) AS t(id, name, price);
```

```sql
SELECT * FROM orders;
```

DuckDB here is in-memory and process-local — data doesn't survive `docker compose down`,
and (since `persistence.type: inMemory` too) query history and routing config live only
in the running process, same as every `minimal-inmemory`-style example.

## Explore query history in Studio

Open **http://localhost:3000** — MCP-originated queries show up on the **Queries** page
like any other frontend's traffic, and (once you set `agent_id`/`conversation_id`, either
via tool parameters or letting them default) on the **Agents** and **Conversations**
pages too. See
**[Session replay and guardrails](../../website/docs/agentic/session-replay.md)**
for what gets persisted.

## Add guardrails

Nothing here restricts what an agent can run — no read-only enforcement, no row caps.
That's deliberate: guardrails are configured the same way for MCP as for every other
frontend, not baked into the frontend itself. See
**[Guardrails](../../website/docs/architecture/guardrails.md)** for `read_only`,
`row_limit`, and other guard types, and add a `guardrails:` block to `config.yaml` to
try one against this stack.

## Stop

```bash
docker compose down
```
