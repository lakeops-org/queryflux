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
use queryflux_core::access_model::AccessRequest;
use queryflux_core::query::EngineType;
use queryflux_core::query::{ClusterGroupName, SqlDialect};
use queryflux_core::schema_context::SchemaContext;
use queryflux_core::session::SessionContext;
use queryflux_core::tags::QueryTags;
use queryflux_guardrails::built_in::Guard;
use queryflux_guardrails::context::{GuardContext, GuardLayer, GuardResult};
use queryflux_guardrails::result_to_action;
use queryflux_persistence::GuardAction;
use queryflux_translation::{render_mask, rewrite_table_scans, TablePolicy};

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
    global_enabled: bool,
    group_enabled: HashMap<String, Option<bool>>,
    /// Only groups with an explicit `groups.<name>.connection` override; absence falls back
    /// to `default_connection`.
    group_connection: HashMap<String, String>,
    /// `accessControl.defaultConnection` — the connection a group without an explicit
    /// override resolves to. `None` means such a group gets no access control at all.
    default_connection: Option<String>,
}

impl OpaAccessGuard {
    /// Whether access control is administratively enabled for `group`. Does **not** by
    /// itself mean access control runs for it — see [`Self::connection_name_for_group`].
    pub fn enabled_for_group(&self, group: &str) -> bool {
        if let Some(enabled) = self.group_enabled.get(group).and_then(|enabled| *enabled) {
            return enabled;
        }
        self.global_enabled
    }

    /// The named connection `group` resolves to: its own override, else
    /// `default_connection`. `None` means access control does not apply to this group.
    pub fn connection_name_for_group<'a>(&'a self, group: &str) -> Option<&'a str> {
        self.group_connection
            .get(group)
            .map(String::as_str)
            .or(self.default_connection.as_deref())
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
        let group_enabled = cfg
            .groups
            .iter()
            .map(|(name, ov)| (name.clone(), ov.enabled))
            .collect();
        let group_connection = cfg
            .groups
            .iter()
            .filter_map(|(name, ov)| ov.connection.clone().map(|c| (name.clone(), c)))
            .collect();
        Ok(Arc::new(Self {
            connections,
            global_enabled: cfg.enabled,
            group_enabled,
            group_connection,
            default_connection: cfg.default_connection.clone(),
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
        let operation = classify_operation(stmts.as_deref(), ctx.sql);

        if !conn.controller.evaluates(&operation) {
            return GuardResult::allow();
        }

        let empty_schema = SchemaContext::default();
        let schema = ctx.schema.unwrap_or(&empty_schema);

        let extracted = match queryflux_translation::extract_resources(ctx.sql, ctx.dialect, schema)
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

        if extracted.is_empty() {
            // No base tables (e.g. `SELECT 1`) — nothing to decide.
            return GuardResult::allow();
        }

        if conn.on_missing_schema == OnMissingSchema::Deny
            && extracted.iter().any(|r| matches!(r.columns, Columns::All))
        {
            return GuardResult::deny(
                "access control: table columns could not be resolved (onMissingSchema=deny)",
                "ACCESS_SCHEMA_UNRESOLVED",
            );
        }

        let resources: Vec<AccessResource> = extracted
            .iter()
            .map(|r| AccessResource {
                catalog: r.catalog.clone(),
                schema: r.schema.clone(),
                table: r.table.clone(),
                columns: r.columns.clone(),
            })
            .collect();

        let session_params: BTreeMap<String, String> = conn
            .session_param_keys
            .iter()
            .filter_map(|k| ctx.session_extra.get(k).map(|v| (k.clone(), v.clone())))
            .collect();

        let request = AccessRequest {
            identity: Identity {
                user: ctx.user.unwrap_or("anonymous").to_string(),
                groups: ctx.groups.to_vec(),
                roles: ctx.roles.to_vec(),
                attributes: ctx.attributes.clone(),
            },
            operation,
            resources,
            context: RequestContext {
                cluster_group: ctx.cluster_group.0.clone(),
                engine: engine_name(ctx.engine_type).to_string(),
                query_id: String::new(),
                session_params,
            },
        };

        let decision = conn.controller.evaluate(&request).await;

        if !decision.is_allowed() {
            let (table, reason) = decision.first_denied().unwrap_or(("", "access denied"));
            return GuardResult::deny(
                format!("access denied for {table}: {reason}"),
                "ACCESS_DENIED",
            );
        }

        if decision.has_ucast_filter() {
            return GuardResult::deny(
                "access control: structured (ucast) row filters are not implemented",
                "ACCESS_UCAST_UNIMPLEMENTED",
            );
        }

        if !decision.has_rewrite() {
            return GuardResult::allow();
        }

        // Group filters + masks by table into TablePolicy.
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

        let policies: Vec<TablePolicy> = by_table.into_values().collect();
        let mut meta = HashMap::new();
        meta.insert(
            "tables".to_string(),
            policies
                .iter()
                .map(|p| p.table.clone())
                .collect::<Vec<_>>()
                .join(","),
        );
        meta.insert(
            "row_filtered".to_string(),
            policies
                .iter()
                .filter(|p| !p.row_filters.is_empty())
                .map(|p| p.table.clone())
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

        match rewrite_table_scans(ctx.sql, ctx.dialect, schema, &policies) {
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
/// `table.insert/update/delete`. Everything else — DDL, `Expression::Command` (BEGIN/COMMIT/
/// CREATE/ALTER/DROP/anything the parser doesn't model precisely — see
/// `sql_classify::is_read_stmt`'s doc comment), `SHOW`, `DESCRIBE` — maps to `statement.other`,
/// which is never in the default `operations` allowlist, so the stage is skipped for it
/// rather than mistakenly treating (say) a `CREATE TABLE orders (...)` as a read of `orders`
/// eligible for row-filter rewriting.
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
        Some(_) => Operation("statement.other".to_string()),
        None if queryflux_core::sql_classify::is_read_like_fallback(sql) => {
            Operation::table_select()
        }
        None => Operation("statement.other".to_string()),
    }
}

fn engine_name(e: &EngineType) -> String {
    format!("{e:?}").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use queryflux_core::access_config::{
        AccessConnectionConfig, AccessControlConfig, GroupOverride, OpaProviderConfig,
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
    }

    /// Regression test: a `CREATE TABLE ... (col_list)` must never be classified as
    /// `table.select` — the referenced table name in the DDL is a definition target, not
    /// something to extract-and-rewrite as if it were a read. Caught by an e2e test that
    /// tried to apply a row filter to a `CREATE TABLE` statement and mangled it.
    #[tokio::test]
    async fn ddl_is_not_table_select() {
        let op = classify("CREATE TABLE orders (id INTEGER, amount INTEGER)").await;
        assert_ne!(op, Operation::table_select());
        assert_eq!(op.as_str(), "statement.other");
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
    ) -> GuardContext<'a> {
        GuardContext {
            sql,
            dialect,
            engine_type,
            cluster_group: group,
            user: Some("alice"),
            groups: &[],
            roles: &[],
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
}
