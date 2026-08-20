/**
 * Starter bodies for user_scripts rows. Must match runtime contracts:
 * - translation_fixup: sqlglot AST transform
 * - routing: PythonScriptRouter `route(query, ctx)` (see website/docs/architecture/routing-and-clusters.md)
 * - guard: GuardChain `check(ctx)` returns {"action": "allow"|"warn"|"deny", ...}
 * - clusterStrategy: PythonScriptStrategy `select_cluster(candidates)` (see website/docs/architecture/routing-and-clusters.md)
 */

export const TRANSLATION_FIXUP_TEMPLATE = `# Do not remove the imports below — they are required by the proxy.
from sqlglot.expressions import Expression
import sqlglot.expressions as exp


def transform(ast: Expression, src: str, dst: str) -> None:
    # src: source dialect (e.g. "trino"), dst: target dialect (e.g. "athena")
    pass
`;

export const GUARD_SCRIPT_TEMPLATE = `def check(ctx: dict) -> dict:
    """Inspect a query and return an allow / warn / deny verdict.

    ctx keys:
      sql            - original SQL string
      translated_sql - SQL after dialect translation
      engine_type    - e.g. "trino", "duckdb"
      cluster_group  - destination group name
      user           - authenticated username (may be None)
      agent_id       - agent identity header (may be None)
      conversation_id
      query_intent   - "schema_exploration" | "aggregation" | "lookup" | "mutation" | "unknown"
      query_tags     - dict[str, str | None]

    Return shapes:
      {"action": "allow"}
      {"action": "allow", "metadata": {"matched_rule": "...", "estimated_rows": "~50k"}}
      {"action": "warn",  "reason": "scans large table without partition filter"}
      {"action": "deny",  "reason": "missing WHERE on fct_events", "code": "MISSING_PREDICATE"}
    """
    sql = ctx.get("sql", "").upper()

    # Example: block mutations
    # for kw in ("INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE"):
    #     if kw in sql:
    #         return {"action": "deny", "reason": f"{kw} not allowed", "code": "READ_ONLY"}

    return {"action": "allow"}
`;

export const ROUTING_SCRIPT_TEMPLATE = `def route(query: str, ctx: dict) -> str | None:
    """Pick a cluster group name, or None to let the next router decide.

    ctx always includes:
      - protocol: "trinoHttp" | "postgresWire" | "mysqlWire" | "clickHouseHttp" | "flightSql"
      - headers: dict[str, str] (empty {} for Postgres/MySQL wire; Trino uses lowercase keys)

    When authenticated, ctx may include:
      - auth: {"user": str, "groups": [str, ...], "roles": [str, ...]}

    Postgres wire also: database, user, sessionParams.
    MySQL wire: schema, user, sessionVars.
    ClickHouse HTTP: queryParams.
    """
    # Example:
    # if ctx.get("protocol") == "trinoHttp":
    #     user = (ctx.get("headers") or {}).get("x-trino-user")
    #     if user == "batch":
    #         return "my-heavy-group"
    return None
`;

export const CLUSTER_STRATEGY_SCRIPT_TEMPLATE = `def select_cluster(candidates: list[dict]) -> str | None:
    """Pick a member cluster from this group, or None to fall back to the first candidate.

    Called only with eligible candidates: already enabled, healthy, and under
    max_running_queries. Each candidate dict has:
      - name: cluster name (str)
      - engineType: e.g. "trino", "duckDb", "starRocks"
      - runningQueries: current in-flight query count (int)
      - maxRunningQueries: per-cluster capacity limit (int)
    """
    # Example: prefer the least-loaded Trino cluster, otherwise fall back.
    # trino = [c for c in candidates if c["engineType"] == "trino"]
    # if trino:
    #     return min(trino, key=lambda c: c["runningQueries"])["name"]
    return None
`;
