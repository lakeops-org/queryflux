//! `OpaAccessGuard` — the data-level access-control decision, expressed as a `Guard` in the
//! pre-translation guardrail chain. Denies short-circuit the query; allow-with-policy
//! returns `GuardResult::Rewrite` carrying the row-filtered + column-masked **source** SQL.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use polyglot_sql::expressions::Expression;
use queryflux_access_control::{
    AccessController, AccessResource, Columns, Identity, Operation, RequestContext,
};
use queryflux_core::access_config::OnMissingSchema;
use queryflux_core::access_model::{AccessDecision, AccessRequest};
use queryflux_core::query::EngineType;
use queryflux_core::query::{ClusterGroupName, SqlDialect};
use queryflux_core::schema_context::SchemaContext;
use queryflux_core::session::SessionContext;
use queryflux_core::tags::QueryTags;
use queryflux_guardrails::built_in::Guard;
use queryflux_guardrails::context::{GuardContext, GuardLayer, GuardResult};
use queryflux_guardrails::result_to_action;
use queryflux_persistence::GuardAction;
use queryflux_translation::{
    apply_write_filters, render_mask, rewrite_table_scans, ExtractedResource, TablePolicy,
};

use queryflux_auth::AuthContext;

/// Outcome of the pre-translation access-control stage.
pub enum AccessStageOutcome {
    /// Query may proceed unchanged. `action` is recorded for audit.
    Allowed { action: GuardAction },
    /// A guard rewrote the query. `sql` is source-dialect; run `maybe_translate` on it next.
    Rewritten { sql: String, action: GuardAction },
    /// The query is denied.
    Denied {
        reason: String,
        code: Option<String>,
        action: GuardAction,
    },
}

/// Run the access-control guard on the **source** SQL, before dialect translation.
/// Returns `None` when no guard is configured (caller proceeds unchanged).
#[allow(clippy::too_many_arguments)]
pub async fn run_access_control_stage(
    guard: &OpaAccessGuard,
    sql: &str,
    src_dialect: &SqlDialect,
    engine_type: &EngineType,
    group: &ClusterGroupName,
    auth_ctx: &AuthContext,
    session: &SessionContext,
    schema: Option<&SchemaContext>,
    tags: &QueryTags,
) -> AccessStageOutcome {
    let parse =
        queryflux_core::sql_classify::SqlParseCache::new(sql.to_string(), src_dialect.clone());
    let ctx = GuardContext {
        sql,
        original_sql: None,
        dialect: src_dialect,
        engine_type,
        cluster_group: group,
        user: session.user(),
        groups: &auth_ctx.groups,
        roles: &auth_ctx.roles,
        attributes: &auth_ctx.attributes,
        agent_context: None,
        query_tags: tags,
        session_extra: &session.extra,
        schema,
        sql_parse: Some(&parse),
    };
    let result = guard.check(&ctx).await;
    let action = result_to_action(guard.name(), &result);
    match result {
        GuardResult::Deny { reason, code } => AccessStageOutcome::Denied {
            reason,
            code,
            action,
        },
        GuardResult::Rewrite { sql, .. } => AccessStageOutcome::Rewritten { sql, action },
        GuardResult::Allow { .. } | GuardResult::Warn { .. } => {
            AccessStageOutcome::Allowed { action }
        }
    }
}

/// Everything `check()` needs that can vary **per connection** — a named entry under
/// `accessControl.connections`. Every cluster group resolves to exactly one of these (see
/// [`OpaAccessGuard::connection_for_group`]).
struct ConnectionRuntime {
    controller: Arc<AccessController>,
    session_param_keys: Vec<String>,
    on_missing_schema: OnMissingSchema,
}

pub struct OpaAccessGuard {
    /// Keyed by connection name. No name is reserved or required to be present.
    connections: HashMap<String, ConnectionRuntime>,
    /// The validated config this guard was built from — `enabled_for_group` /
    /// `connection_name_for_group` delegate to it directly rather than re-deriving their
    /// own copy of the same group-override-else-default resolution rule, so that rule
    /// lives in exactly one place ([`queryflux_core::access_config::AccessControlConfig`]).
    config: Arc<queryflux_core::access_config::AccessControlConfig>,
}

impl OpaAccessGuard {
    /// Whether access control is administratively enabled for `group`. Does **not** by
    /// itself mean access control runs for it — see [`Self::connection_name_for_group`].
    pub fn enabled_for_group(&self, group: &str) -> bool {
        self.config.enabled_for_group(group)
    }

    /// The named connection `group` resolves to: its own override, else
    /// `default_connection`. `None` means access control does not apply to this group.
    pub fn connection_name_for_group<'a>(&'a self, group: &str) -> Option<&'a str> {
        self.config.connection_name_for_group(group)
    }

    fn connection_for_group(&self, group: &str) -> Option<&ConnectionRuntime> {
        self.connection_name_for_group(group)
            .and_then(|name| self.connections.get(name))
    }

    /// Build the rewriting guard from validated `accessControl` config. One
    /// [`AccessController`] (its own HTTP client, cache, fail-open policy) is built per named
    /// connection.
    pub fn try_from_config(
        cfg: &queryflux_core::access_config::AccessControlConfig,
    ) -> Result<Arc<Self>, String> {
        let metrics: Arc<dyn queryflux_access_control::AccessMetricsSink> =
            Arc::new(queryflux_access_control::NoopMetrics);
        let mut controllers = queryflux_access_control::build_controllers(cfg, metrics)?;
        let mut connections = HashMap::new();
        for (name, conn_cfg) in &cfg.connections {
            let controller = controllers.remove(name).ok_or_else(|| {
                format!("internal error: no controller built for connection {name:?}")
            })?;
            connections.insert(
                name.clone(),
                ConnectionRuntime {
                    controller: Arc::new(controller),
                    session_param_keys: conn_cfg.session_param_keys.clone(),
                    on_missing_schema: conn_cfg.on_missing_schema,
                },
            );
        }
        Ok(Arc::new(Self {
            connections,
            config: Arc::new(cfg.clone()),
        }))
    }
}

#[async_trait]
impl Guard for OpaAccessGuard {
    fn name(&self) -> &'static str {
        "opa_access"
    }

    fn layer(&self) -> GuardLayer {
        GuardLayer::Plan
    }

    async fn check(&self, ctx: &GuardContext<'_>) -> GuardResult {
        // Administratively disabled (globally or for this group): never call the provider,
        // whatever the caller did before reaching us.
        if !self.enabled_for_group(&ctx.cluster_group.0) {
            return GuardResult::allow();
        }

        // No connection resolves for this group (no explicit override and no
        // `defaultConnection`) — access control simply does not apply here.
        let Some(conn) = self.connection_for_group(&ctx.cluster_group.0) else {
            return GuardResult::allow();
        };

        let stmts = match ctx.sql_parse {
            Some(cache) => cache.statements_async().await.map(<[_]>::to_vec),
            None => None,
        };
        // `classify_operation` and `extract_resources` each only look at the first parsed
        // statement — a batch such as `SELECT 1; DELETE FROM orders` (the PostgreSQL simple
        // query protocol allows a semicolon-separated batch in one message, and the wire
        // frontend forwards it unsplit) would classify as the harmless first statement and
        // extract no tables from it, so nothing here would evaluate the DELETE at all, and
        // the whole batch would reach the engine unchecked. Deny outright instead of
        // silently only judging the first statement.
        if stmts.as_ref().is_some_and(|s| s.len() > 1) {
            return GuardResult::deny(
                "access control: multi-statement queries are not supported",
                "ACCESS_MULTI_STATEMENT",
            );
        }
        let operation = classify_operation(stmts.as_deref(), ctx.sql);

        // A non-SELECT statement can still *read* policied tables (`INSERT … SELECT`,
        // `CREATE TABLE … AS`, `UPDATE … WHERE x IN (SELECT …)`), so those reads are
        // evaluated as `table.select` whenever that operation is enabled — independent of
        // whether the statement's own operation is. Its write target is evaluated separately
        // under the statement's own operation, only when that is enabled.
        let evaluates_reads = conn.controller.evaluates(&Operation::table_select());
        // Whether the statement replaces an object is only known after extraction, so the
        // early gate assumes it might.
        let may_write = write_operations(&operation, true)
            .iter()
            .any(|o| conn.controller.evaluates(o));
        if !evaluates_reads && !may_write {
            return GuardResult::allow();
        }

        let empty_schema = SchemaContext::default();
        let schema = ctx.schema.unwrap_or(&empty_schema);

        let statement = match queryflux_translation::extract_resources(ctx.sql, ctx.dialect, schema)
        {
            Ok(r) => r,
            // A query that can't be analyzed has no resources to put in a request, so it can't
            // be judged at all. `onMissingSchema` is about unresolved *columns* of tables that
            // were found — not about failing to find them — so this denies in both modes;
            // allowing it would let anything the parser can't read skip access control.
            Err(e) => {
                return GuardResult::deny(
                    format!("access control: could not analyze query: {e}"),
                    "ACCESS_ANALYSIS_FAILED",
                )
            }
        };

        let embeds_reads = statement.embeds_reads;
        let (targets, reads): (Vec<_>, Vec<_>) = statement
            .resources
            .into_iter()
            .partition(|r| r.is_write_target);
        let check_reads =
            evaluates_reads && !reads.is_empty() && (operation.is_read() || embeds_reads);
        let write_ops: Vec<Operation> = write_operations(&operation, statement.replaces)
            .into_iter()
            .filter(|o| conn.controller.evaluates(o))
            .collect();
        // `classify_operation` (a lightweight, independent classifier) and `extract_resources`
        // (sqlglot-based) are expected to agree on every operation `Operation::SUPPORTED`
        // allows enabling: whenever the former says this statement is one of those write
        // kinds, the latter should have found its target. If they ever disagree — a future
        // operation added to one classifier and not the other, a dialect-specific parse gap —
        // this must fail closed, the same as an outright parse failure above: an enabled
        // write operation silently proceeding unauthorized (because its target was empty, not
        // because it isn't tracked at all) would be a worse bug than a false-positive deny.
        if !write_ops.is_empty() && targets.is_empty() {
            return GuardResult::deny(
                format!(
                    "access control: could not identify the target of {}",
                    operation.as_str()
                ),
                "ACCESS_ANALYSIS_FAILED",
            );
        }
        let check_write = !write_ops.is_empty() && !targets.is_empty();
        if !check_reads && !check_write {
            // No base tables (e.g. `SELECT 1`), or nothing this connection evaluates.
            return GuardResult::allow();
        }

        if check_reads
            && conn.on_missing_schema == OnMissingSchema::Deny
            && reads.iter().any(|r| matches!(r.columns, Columns::All))
        {
            return GuardResult::deny(
                "access control: table columns could not be resolved (onMissingSchema=deny)",
                "ACCESS_SCHEMA_UNRESOLVED",
            );
        }

        let session_params: BTreeMap<String, String> = conn
            .session_param_keys
            .iter()
            .filter_map(|k| ctx.session_extra.get(k).map(|v| (k.clone(), v.clone())))
            .collect();

        let build_request = |operation: Operation, tables: &[ExtractedResource]| AccessRequest {
            identity: Identity {
                user: ctx.user.unwrap_or("anonymous").to_string(),
                groups: ctx.groups.to_vec(),
                roles: ctx.roles.to_vec(),
                attributes: ctx.attributes.clone(),
            },
            operation,
            resources: tables
                .iter()
                .map(|r| AccessResource {
                    kind: r.kind,
                    catalog: r.catalog.clone(),
                    schema: r.schema.clone(),
                    table: r.table.clone(),
                    columns: r.columns.clone(),
                })
                .collect(),
            context: RequestContext {
                cluster_group: ctx.cluster_group.0.clone(),
                engine: engine_name(ctx.engine_type).to_string(),
                query_id: String::new(),
                session_params: session_params.clone(),
            },
        };

        let deny_first = |decision: &AccessDecision| {
            let (table, reason) = decision.first_denied().unwrap_or(("", "access denied"));
            GuardResult::deny(
                format!("access denied for {table}: {reason}"),
                "ACCESS_DENIED",
            )
        };

        // Row filters for the write target: the policy's own decision for the statement's
        // operation, kept apart from any `table.select` filters on the tables it reads.
        let mut write_scope: Option<(String, Vec<String>)> = None;
        if check_write {
            for write_op in &write_ops {
                let decision = conn
                    .controller
                    .evaluate(&build_request(write_op.clone(), &targets))
                    .await;
                if !decision.is_allowed() {
                    return deny_first(&decision);
                }
                if decision.has_ucast_filter() {
                    return GuardResult::deny(
                        "access control: structured (ucast) row filters are not implemented",
                        "ACCESS_UCAST_UNIMPLEMENTED",
                    );
                }
                // A mask changes what a read returns; there is nothing to mask on a write.
                if decision.column_masks().next().is_some() {
                    return GuardResult::deny(
                        format!(
                            "access control: the policy returned column masks for {}, which \
                             apply to reads only",
                            write_op.as_str()
                        ),
                        "ACCESS_REWRITE_UNSUPPORTED_FOR_WRITE",
                    );
                }
                let filters: Vec<String> = decision
                    .row_filters()
                    .map(|(_, expr)| expr.to_string())
                    .collect();
                if !filters.is_empty() {
                    // UPDATE/DELETE are scoped by ANDing the filter into their WHERE, and MERGE
                    // by ANDing it into each WHEN MATCHED / WHEN NOT MATCHED BY SOURCE clause's
                    // condition (queryflux-translation's apply_write_filters). Every other write
                    // (INSERT, TRUNCATE, DDL) has no existing row for a filter to restrict, so
                    // deny rather than silently skip a restriction the policy meant to apply.
                    if matches!(
                        write_op.as_str(),
                        "table.update" | "table.delete" | "table.merge"
                    ) {
                        write_scope = Some((targets[0].table.clone(), filters));
                    } else {
                        return GuardResult::deny(
                            format!(
                                "access control: the policy returned row filters for {}, which \
                                 are only supported for table.select, table.update, \
                                 table.delete and table.merge",
                                write_op.as_str()
                            ),
                            "ACCESS_REWRITE_UNSUPPORTED_FOR_WRITE",
                        );
                    }
                }
            }
        }

        if !check_reads && write_scope.is_none() {
            return GuardResult::allow();
        }

        // Filters + masks for the tables the statement reads, grouped by table.
        let mut policies: Vec<TablePolicy> = Vec::new();
        if check_reads {
            let decision = conn
                .controller
                .evaluate(&build_request(Operation::table_select(), &reads))
                .await;

            if !decision.is_allowed() {
                return deny_first(&decision);
            }

            if decision.has_ucast_filter() {
                return GuardResult::deny(
                    "access control: structured (ucast) row filters are not implemented",
                    "ACCESS_UCAST_UNIMPLEMENTED",
                );
            }

            let mut by_table: HashMap<String, TablePolicy> = HashMap::new();
            for (table, expr) in decision.row_filters() {
                by_table
                    .entry(table.to_string())
                    .or_insert_with(|| TablePolicy {
                        table: table.to_string(),
                        row_filters: Vec::new(),
                        masked_columns: Vec::new(),
                    })
                    .row_filters
                    .push(expr.to_string());
            }
            for (table, mask) in decision.column_masks() {
                let rendered = match render_mask(mask, &mask.column, ctx.dialect) {
                    Ok(s) => s,
                    Err(e) => {
                        return GuardResult::deny(
                            format!("access control: {e}"),
                            "ACCESS_MASK_RENDER_FAILED",
                        )
                    }
                };
                by_table
                    .entry(table.to_string())
                    .or_insert_with(|| TablePolicy {
                        table: table.to_string(),
                        row_filters: Vec::new(),
                        masked_columns: Vec::new(),
                    })
                    .masked_columns
                    .push((mask.column.clone(), rendered));
            }
            policies = by_table.into_values().collect();
        }

        if policies.is_empty() && write_scope.is_none() {
            return GuardResult::allow();
        }

        let write_table = write_scope.as_ref().map(|(table, _)| table.clone());
        let mut meta = HashMap::new();
        meta.insert(
            "tables".to_string(),
            policies
                .iter()
                .map(|p| p.table.clone())
                .chain(write_table.clone())
                .collect::<Vec<_>>()
                .join(","),
        );
        meta.insert(
            "row_filtered".to_string(),
            policies
                .iter()
                .filter(|p| !p.row_filters.is_empty())
                .map(|p| p.table.clone())
                .chain(write_table)
                .collect::<Vec<_>>()
                .join(","),
        );
        meta.insert(
            "masked_columns".to_string(),
            policies
                .iter()
                .flat_map(|p| {
                    p.masked_columns
                        .iter()
                        .map(|(c, _)| format!("{}.{c}", p.table))
                })
                .collect::<Vec<_>>()
                .join(","),
        );

        let rewritten =
            rewrite_table_scans(ctx.sql, ctx.dialect, schema, &policies).and_then(|sql| {
                match &write_scope {
                    Some((_, filters)) => apply_write_filters(&sql, ctx.dialect, filters),
                    None => Ok(sql),
                }
            });
        match rewritten {
            Ok(sql) => GuardResult::Rewrite {
                sql,
                metadata: Some(meta),
            },
            Err(e) => GuardResult::deny(
                format!("access control: could not apply row filters/masks: {e}"),
                "ACCESS_REWRITE_FAILED",
            ),
        }
    }
}

/// Classify the query's namespaced operation from its first parsed statement.
///
/// Deliberately narrow: only true DQL maps to `table.select` and only DML writes map to
/// `table.insert/update/delete/merge/truncate`. Everything else — DDL, `Expression::Command` (BEGIN/COMMIT/
/// CREATE/ALTER/DROP/anything the parser doesn't model precisely — see
/// `sql_classify::is_read_stmt`'s doc comment), `SHOW`, `DESCRIBE` — maps to `statement.other`,
/// which is never in the default `operations` allowlist, so the stage is skipped for it
/// rather than mistakenly treating (say) a `CREATE TABLE orders (...)` as a read of `orders`
/// eligible for row-filter rewriting. Reads *inside* such a statement (`INSERT … SELECT`,
/// `CREATE TABLE … AS`, …) are still policy-checked — see `check`.
///
/// When `polyglot-sql` couldn't parse the statement at all, fall back to the same string
/// heuristic the built-in guards use (`is_read_like_fallback`) rather than defaulting to
/// "skip the stage" — a query that heuristically looks like a read should still be
/// evaluated even if this particular parser choked on it (resource extraction uses a
/// different, Python-sqlglot-based parser, so a `polyglot-sql` failure here doesn't imply
/// extraction will also fail).
fn classify_operation(stmts: Option<&[Expression]>, sql: &str) -> Operation {
    match stmts.and_then(|s| s.first()) {
        Some(
            Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Subquery(_),
        ) => Operation::table_select(),
        Some(Expression::Insert(_)) => Operation("table.insert".to_string()),
        Some(Expression::Update(_)) => Operation("table.update".to_string()),
        Some(Expression::Delete(_)) => Operation("table.delete".to_string()),
        Some(Expression::Merge(_)) => Operation("table.merge".to_string()),
        Some(Expression::CreateTable(_)) => Operation("table.create".to_string()),
        Some(Expression::DropTable(_)) => Operation("table.drop".to_string()),
        Some(Expression::AlterTable(_)) => Operation("table.alter".to_string()),
        Some(Expression::CreateView(_)) => Operation("view.create".to_string()),
        Some(Expression::DropView(_)) => Operation("view.drop".to_string()),
        Some(Expression::AlterView(_)) => Operation("view.alter".to_string()),
        Some(Expression::CreateSchema(_)) => Operation("schema.create".to_string()),
        Some(Expression::DropSchema(_) | Expression::DropNamespace(_)) => {
            Operation("schema.drop".to_string())
        }
        Some(Expression::CreateDatabase(_)) => Operation("catalog.create".to_string()),
        Some(Expression::DropDatabase(_)) => Operation("catalog.drop".to_string()),
        Some(Expression::Truncate(_) | Expression::TruncateTable(_)) => {
            Operation("table.truncate".to_string())
        }
        Some(_) => Operation("statement.other".to_string()),
        None if queryflux_core::sql_classify::is_read_like_fallback(sql) => {
            Operation::table_select()
        }
        None => Operation("statement.other".to_string()),
    }
}

/// The operations a statement's write targets are evaluated under: its own, plus — for a
/// `CREATE OR REPLACE` — the matching drop, since replacing destroys the existing object.
/// Reads have none (their tables are evaluated as `table.select`).
fn write_operations(operation: &Operation, replaces: bool) -> Vec<Operation> {
    if operation.is_read() {
        return Vec::new();
    }
    let mut ops = vec![operation.clone()];
    if replaces {
        ops.extend(operation.drop_counterpart());
    }
    ops
}

fn engine_name(e: &EngineType) -> String {
    format!("{e:?}").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use queryflux_core::access_config::{
        AccessConnectionConfig, AccessControlConfig, CerbosProviderConfig, GroupOverride,
        OpaProviderConfig, ProviderKind,
    };
    use queryflux_core::query::SqlDialect;
    use queryflux_core::sql_classify::SqlParseCache;
    use queryflux_core::tags::QueryTags;
    use tokio::net::TcpListener;

    async fn classify(sql: &str) -> Operation {
        let cache = SqlParseCache::new(sql.to_string(), SqlDialect::Postgres);
        let stmts = cache.statements_async().await.map(<[_]>::to_vec);
        classify_operation(stmts.as_deref(), sql)
    }

    #[tokio::test]
    async fn select_classifies_as_table_select() {
        assert_eq!(
            classify("SELECT * FROM orders").await,
            Operation::table_select()
        );
        assert_eq!(
            classify("WITH x AS (SELECT 1) SELECT * FROM x").await,
            Operation::table_select()
        );
    }

    #[tokio::test]
    async fn dml_classifies_by_kind() {
        assert_eq!(
            classify("INSERT INTO orders VALUES (1)").await.as_str(),
            "table.insert"
        );
        assert_eq!(
            classify("UPDATE orders SET x = 1").await.as_str(),
            "table.update"
        );
        assert_eq!(
            classify("DELETE FROM orders WHERE id = 1").await.as_str(),
            "table.delete"
        );
        assert_eq!(
            classify("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE")
                .await
                .as_str(),
            "table.merge"
        );
        assert_eq!(
            classify("TRUNCATE TABLE orders").await.as_str(),
            "table.truncate"
        );
    }

    /// Regression test: a `CREATE TABLE ... (col_list)` must never be classified as
    /// `table.select` — the referenced table name in the DDL is a definition target, not
    /// something to extract-and-rewrite as if it were a read. Caught by an e2e test that
    /// tried to apply a row filter to a `CREATE TABLE` statement and mangled it.
    #[tokio::test]
    async fn ddl_is_not_table_select() {
        let op = classify("CREATE TABLE orders (id INTEGER, amount INTEGER)").await;
        assert_ne!(op, Operation::table_select());
        assert_eq!(op.as_str(), "table.create");
    }

    #[tokio::test]
    async fn ddl_classifies_by_object_and_verb() {
        for (sql, expected) in [
            ("CREATE TABLE t (id INT)", "table.create"),
            ("CREATE TABLE t AS SELECT 1", "table.create"),
            ("DROP TABLE t", "table.drop"),
            ("ALTER TABLE t ADD COLUMN c INT", "table.alter"),
            ("CREATE VIEW v AS SELECT 1", "view.create"),
            ("DROP VIEW v", "view.drop"),
            ("ALTER VIEW v RENAME TO w", "view.alter"),
            ("CREATE SCHEMA s", "schema.create"),
            ("DROP SCHEMA s", "schema.drop"),
            ("CREATE DATABASE d", "catalog.create"),
            ("DROP DATABASE d", "catalog.drop"),
            // Not modeled: stays unevaluated rather than guessed at.
            ("CREATE INDEX i ON t (a)", "statement.other"),
            ("GRANT SELECT ON t TO r", "statement.other"),
            ("SET search_path = s", "statement.other"),
        ] {
            assert_eq!(classify(sql).await.as_str(), expected, "{sql}");
        }
    }

    #[test]
    fn create_or_replace_also_needs_the_drop() {
        let create = Operation("table.create".to_string());
        let names = |ops: Vec<Operation>| {
            ops.iter()
                .map(|o| o.as_str().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(write_operations(&create, false)), ["table.create"]);
        assert_eq!(
            names(write_operations(&create, true)),
            ["table.create", "table.drop"]
        );
        // Only creates have a drop counterpart; reads have no write operations at all.
        let insert = Operation("table.insert".to_string());
        assert_eq!(names(write_operations(&insert, true)), ["table.insert"]);
        assert!(write_operations(&Operation::table_select(), true).is_empty());
    }

    #[tokio::test]
    async fn show_and_describe_are_not_table_select() {
        assert_eq!(classify("SHOW TABLES").await.as_str(), "statement.other");
        assert_eq!(
            classify("DESCRIBE orders").await.as_str(),
            "statement.other"
        );
    }

    /// Starts a tiny in-process OPA stub that always allows `orders` with a row filter
    /// tagging which stub answered (`source = '<tag>'`), so a test can tell which HTTP
    /// endpoint actually received the request.
    async fn start_tagged_stub(tag: &'static str) -> String {
        async fn handler(
            axum::extract::State(tag): axum::extract::State<&'static str>,
            Json(_body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "result": {
                    "resources": [{
                        "table": "orders",
                        "allow": true,
                        "rowFilters": [{ "expression": format!("source = '{tag}'") }],
                    }]
                }
            }))
        }
        let app = Router::new()
            .route("/v1/data/queryflux/access", post(handler))
            .with_state(tag);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    fn opa_connection(url: &str) -> AccessConnectionConfig {
        AccessConnectionConfig {
            opa: Some(OpaProviderConfig {
                url: url.to_string(),
                decision_path: "/v1/data/queryflux/access".to_string(),
                timeout_ms: 2_000,
                bearer_token: None,
                client_credentials: None,
            }),
            cache_ttl_ms: 0,
            ..AccessConnectionConfig::default()
        }
    }

    /// Starts a tiny in-process Cerbos `CheckResources` stub that always allows `orders`
    /// with a row-filter output tagged so a test can tell which endpoint answered.
    async fn start_cerbos_stub(tag: &'static str) -> String {
        async fn handler(
            axum::extract::State(tag): axum::extract::State<&'static str>,
            Json(_body): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "results": [{
                    "actions": {"table.select": "EFFECT_ALLOW"},
                    "outputs": [{
                        "src": "resource.table.default#row_filter",
                        "val": {"kind": "row_filter", "expression": format!("source = '{tag}'")}
                    }]
                }]
            }))
        }
        let app = Router::new()
            .route("/api/check/resources", post(handler))
            .with_state(tag);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    fn cerbos_connection(url: &str) -> AccessConnectionConfig {
        AccessConnectionConfig {
            provider: ProviderKind::Cerbos,
            opa: None,
            cerbos: Some(CerbosProviderConfig {
                url: url.to_string(),
                check_resources_path: "/api/check/resources".to_string(),
                timeout_ms: 2_000,
                bearer_token: None,
            }),
            cache_ttl_ms: 0,
            ..AccessConnectionConfig::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_ctx<'a>(
        sql: &'a str,
        dialect: &'a SqlDialect,
        engine_type: &'a EngineType,
        group: &'a ClusterGroupName,
        attributes: &'a BTreeMap<String, serde_json::Value>,
        query_tags: &'a QueryTags,
        session_extra: &'a HashMap<String, String>,
        sql_parse: &'a SqlParseCache,
        roles: &'a [String],
    ) -> GuardContext<'a> {
        GuardContext {
            sql,
            original_sql: None,
            dialect,
            engine_type,
            cluster_group: group,
            user: Some("alice"),
            groups: &[],
            roles,
            attributes,
            agent_context: None,
            query_tags,
            session_extra,
            schema: None,
            sql_parse: Some(sql_parse),
        }
    }

    /// Proves a cluster group actually reaches a *different* HTTP endpoint when
    /// `groups.<name>.connection` names a non-default connection — not just that the
    /// config resolves the right name in memory, but that the guard dispatches the real
    /// request to the right server.
    #[tokio::test]
    async fn group_routes_to_its_configured_connection() {
        let default_url = start_tagged_stub("default").await;
        let eu_url = start_tagged_stub("eu").await;

        let cfg = AccessControlConfig {
            enabled: true,
            default_connection: Some("default".to_string()),
            connections: HashMap::from([
                ("default".to_string(), opa_connection(&default_url)),
                ("eu".to_string(), opa_connection(&eu_url)),
            ]),
            groups: HashMap::from([(
                "eu-group".to_string(),
                GroupOverride {
                    enabled: None,
                    fail_open: None,
                    connection: Some("eu".to_string()),
                },
            )]),
        };
        let guard = OpaAccessGuard::try_from_config(&cfg).expect("build guard");

        assert_eq!(
            guard.connection_name_for_group("trino-prod"),
            Some("default")
        );
        assert_eq!(guard.connection_name_for_group("eu-group"), Some("eu"));

        let dialect = SqlDialect::Postgres;
        let engine_type = EngineType::Trino;
        let sql = "SELECT id FROM orders";
        let sql_parse = SqlParseCache::new(sql.to_string(), dialect.clone());
        let attributes = BTreeMap::new();
        let query_tags = QueryTags::new();
        let session_extra = HashMap::new();

        let default_group = ClusterGroupName("trino-prod".to_string());
        let default_ctx = plan_ctx(
            sql,
            &dialect,
            &engine_type,
            &default_group,
            &attributes,
            &query_tags,
            &session_extra,
            &sql_parse,
            &[],
        );
        match guard.check(&default_ctx).await {
            GuardResult::Rewrite { sql, .. } => {
                assert!(sql.contains("source = 'default'"), "got: {sql}")
            }
            other => panic!("expected a rewrite from the default connection, got {other:?}"),
        }

        let eu_group = ClusterGroupName("eu-group".to_string());
        let eu_ctx = plan_ctx(
            sql,
            &dialect,
            &engine_type,
            &eu_group,
            &attributes,
            &query_tags,
            &session_extra,
            &sql_parse,
            &[],
        );
        match guard.check(&eu_ctx).await {
            GuardResult::Rewrite { sql, .. } => {
                assert!(sql.contains("source = 'eu'"), "got: {sql}")
            }
            other => panic!("expected a rewrite from the eu connection, got {other:?}"),
        }
    }

    /// With no `defaultConnection` set, a group that doesn't explicitly opt into a named
    /// connection gets no access control at all — `check()` allows without ever calling the
    /// stub, distinct from an explicit allow decision.
    #[tokio::test]
    async fn group_with_no_resolvable_connection_is_allowed_without_a_call() {
        let stub_url = start_tagged_stub("only").await;
        let cfg = AccessControlConfig {
            enabled: true,
            default_connection: None,
            connections: HashMap::from([("only".to_string(), opa_connection(&stub_url))]),
            groups: HashMap::from([(
                "opted-in".to_string(),
                GroupOverride {
                    enabled: None,
                    fail_open: None,
                    connection: Some("only".to_string()),
                },
            )]),
        };
        let guard = OpaAccessGuard::try_from_config(&cfg).expect("build guard");

        assert_eq!(guard.connection_name_for_group("unrelated-group"), None);
        assert_eq!(guard.connection_name_for_group("opted-in"), Some("only"));

        let dialect = SqlDialect::Postgres;
        let engine_type = EngineType::Trino;
        let sql = "SELECT id FROM orders";
        let sql_parse = SqlParseCache::new(sql.to_string(), dialect.clone());
        let attributes = BTreeMap::new();
        let query_tags = QueryTags::new();
        let session_extra = HashMap::new();

        let unrelated_group = ClusterGroupName("unrelated-group".to_string());
        let ctx = plan_ctx(
            sql,
            &dialect,
            &engine_type,
            &unrelated_group,
            &attributes,
            &query_tags,
            &session_extra,
            &sql_parse,
            &[],
        );
        match guard.check(&ctx).await {
            GuardResult::Allow { .. } => {}
            other => panic!("expected a plain allow (no connection resolved), got {other:?}"),
        }
    }

    /// `enabled: false` (globally, or for one group) must stop `check()` before it talks to
    /// the provider, even though a connection resolves — a disabled group must never receive
    /// a rewrite or a denial.
    #[tokio::test]
    async fn disabled_group_is_allowed_without_a_call() {
        let stub_url = start_tagged_stub("stub").await;
        let group_off = || GroupOverride {
            enabled: Some(false),
            fail_open: None,
            connection: None,
        };
        let group_on = || GroupOverride {
            enabled: Some(true),
            fail_open: None,
            connection: None,
        };
        let connections = || HashMap::from([("default".to_string(), opa_connection(&stub_url))]);

        // Enabled globally, off for `off-group`.
        let per_group = AccessControlConfig {
            enabled: true,
            default_connection: Some("default".to_string()),
            connections: connections(),
            groups: HashMap::from([("off-group".to_string(), group_off())]),
        };
        // Off globally, on for `on-group`.
        let globally_off = AccessControlConfig {
            enabled: false,
            default_connection: Some("default".to_string()),
            connections: connections(),
            groups: HashMap::from([("on-group".to_string(), group_on())]),
        };

        let dialect = SqlDialect::Postgres;
        let engine_type = EngineType::Trino;
        let sql = "SELECT id FROM orders";
        let sql_parse = SqlParseCache::new(sql.to_string(), dialect.clone());
        let attributes = BTreeMap::new();
        let query_tags = QueryTags::new();
        let session_extra = HashMap::new();

        for (cfg, group, expect_rewrite) in [
            (&per_group, "off-group", false),
            (&per_group, "other-group", true),
            (&globally_off, "any-group", false),
            (&globally_off, "on-group", true),
        ] {
            let guard = OpaAccessGuard::try_from_config(cfg).expect("build guard");
            let group = ClusterGroupName(group.to_string());
            let ctx = plan_ctx(
                sql,
                &dialect,
                &engine_type,
                &group,
                &attributes,
                &query_tags,
                &session_extra,
                &sql_parse,
                &[],
            );
            match (guard.check(&ctx).await, expect_rewrite) {
                (GuardResult::Rewrite { .. }, true) => {}
                (GuardResult::Allow { .. }, false) => {}
                (other, _) => panic!(
                    "group {:?}: expected rewrite={expect_rewrite}, got {other:?}",
                    group.0
                ),
            }
        }
    }

    /// End-to-end through the real guard path with `provider: cerbos` — proves the whole
    /// chain (`OpaAccessGuard::check` -> `AccessController::evaluate` -> `CerbosProvider` ->
    /// a real HTTP `CheckResources` call) round-trips a row filter carried through Cerbos's
    /// `outputs` mechanism into an actual scan-site rewrite, not just that the wire-level
    /// unit tests in `queryflux-access-control` parse the shape correctly in isolation.
    #[tokio::test]
    async fn cerbos_provider_rewrites_via_outputs_row_filter() {
        let stub_url = start_cerbos_stub("cerbos-tag").await;
        let cfg = AccessControlConfig {
            enabled: true,
            default_connection: Some("default".to_string()),
            connections: HashMap::from([("default".to_string(), cerbos_connection(&stub_url))]),
            groups: HashMap::new(),
        };
        let guard = OpaAccessGuard::try_from_config(&cfg).expect("build guard");

        let dialect = SqlDialect::Postgres;
        let engine_type = EngineType::Trino;
        let sql = "SELECT id FROM orders";
        let sql_parse = SqlParseCache::new(sql.to_string(), dialect.clone());
        let attributes = BTreeMap::new();
        let query_tags = QueryTags::new();
        let session_extra = HashMap::new();
        let group = ClusterGroupName("trino-prod".to_string());
        let roles = vec!["analyst".to_string()];
        let ctx = plan_ctx(
            sql,
            &dialect,
            &engine_type,
            &group,
            &attributes,
            &query_tags,
            &session_extra,
            &sql_parse,
            &roles,
        );

        match guard.check(&ctx).await {
            GuardResult::Rewrite { sql, .. } => {
                assert!(sql.contains("source = 'cerbos-tag'"), "got: {sql}")
            }
            other => panic!("expected a rewrite from the cerbos connection, got {other:?}"),
        }
    }

    /// Starts a Cerbos `CheckResources` stub that always returns a fixed, caller-supplied
    /// response body — for scenarios where the exact per-resource shape (multiple
    /// resources, mixed allow/deny, several output kinds) matters more than tagging which
    /// endpoint answered.
    async fn start_cerbos_stub_with_body(body: serde_json::Value) -> String {
        async fn handler(
            axum::extract::State(body): axum::extract::State<serde_json::Value>,
            Json(_req): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            Json(body)
        }
        let app = Router::new()
            .route("/api/check/resources", post(handler))
            .with_state(body);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    async fn check_via_cerbos(sql: &str, response_body: serde_json::Value) -> GuardResult {
        let stub_url = start_cerbos_stub_with_body(response_body).await;
        let cfg = AccessControlConfig {
            enabled: true,
            default_connection: Some("default".to_string()),
            connections: HashMap::from([("default".to_string(), cerbos_connection(&stub_url))]),
            groups: HashMap::new(),
        };
        let guard = OpaAccessGuard::try_from_config(&cfg).expect("build guard");

        let dialect = SqlDialect::Postgres;
        let engine_type = EngineType::Trino;
        let sql_parse = SqlParseCache::new(sql.to_string(), dialect.clone());
        let attributes = BTreeMap::new();
        let query_tags = QueryTags::new();
        let session_extra = HashMap::new();
        let group = ClusterGroupName("trino-prod".to_string());
        let roles = vec!["analyst".to_string()];
        let ctx = plan_ctx(
            sql,
            &dialect,
            &engine_type,
            &group,
            &attributes,
            &query_tags,
            &session_extra,
            &sql_parse,
            &roles,
        );
        guard.check(&ctx).await
    }

    /// Complex scenario: a join across two tables where Cerbos allows one and denies the
    /// other in the *same* `CheckResources` response. The whole query must be denied
    /// (`ACCESS_DENIED`), naming the actually-denied table — proves per-resource results
    /// really do correlate to the right table through the real extraction + guard path,
    /// not just in the isolated wire-mapping unit tests.
    #[tokio::test]
    async fn cerbos_denies_a_join_when_one_of_two_tables_is_denied() {
        // `extract_resources` walks tables in appearance order: orders, then customers.
        let sql = "SELECT a.id FROM orders a JOIN customers b ON a.cid = b.id";
        let response = serde_json::json!({
            "results": [
                {"actions": {"table.select": "EFFECT_ALLOW"}, "outputs": []},
                {"actions": {"table.select": "EFFECT_DENY"}, "outputs": []}
            ]
        });
        match check_via_cerbos(sql, response).await {
            GuardResult::Deny { reason, code } => {
                assert_eq!(code, Some("ACCESS_DENIED".to_string()));
                assert!(reason.contains("customers"), "got: {reason}");
            }
            other => panic!("expected a deny naming customers, got {other:?}"),
        }
    }

    /// Complex scenario: Cerbos returns a row filter *and* a column mask for the same
    /// table from one `CheckResources` call — the same combined-outputs shape a real
    /// analyst-tier policy would emit (row-restricted + masked column together) — and the
    /// guard must apply both in a single rewrite.
    #[tokio::test]
    async fn cerbos_applies_combined_row_filter_and_column_mask() {
        let sql = "SELECT name, ssn FROM customers";
        let response = serde_json::json!({
            "results": [{
                "actions": {"table.select": "EFFECT_ALLOW"},
                "outputs": [{
                    "src": "resource.table.default#analyst_policy",
                    "val": [
                        {"kind": "row_filter", "expression": "region = 'EU'"},
                        {"kind": "column_mask", "column": "ssn", "type": "SHOW_LAST_4"}
                    ]
                }]
            }]
        });
        match check_via_cerbos(sql, response).await {
            GuardResult::Rewrite { sql, .. } => {
                let lower = sql.to_lowercase();
                assert!(lower.contains("region = 'eu'"), "got: {sql}");
                assert!(lower.contains("substr"), "mask must be applied, got: {sql}");
            }
            other => panic!("expected a rewrite with filter + mask, got {other:?}"),
        }
    }
}
