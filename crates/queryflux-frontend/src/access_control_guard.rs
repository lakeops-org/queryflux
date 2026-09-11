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
use queryflux_core::schema_context::SchemaContext;
use queryflux_core::query::{ClusterGroupName, SqlDialect};
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
    let parse = queryflux_core::sql_classify::SqlParseCache::new(sql.to_string(), src_dialect.clone());
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

pub struct OpaAccessGuard {
    controller: Arc<AccessController>,
    session_param_keys: Vec<String>,
    on_missing_schema: OnMissingSchema,
}

impl OpaAccessGuard {
    pub fn new(
        controller: Arc<AccessController>,
        session_param_keys: Vec<String>,
        on_missing_schema: OnMissingSchema,
    ) -> Self {
        Self {
            controller,
            session_param_keys,
            on_missing_schema,
        }
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
        let stmts = match ctx.sql_parse {
            Some(cache) => cache.statements_async().await.map(<[_]>::to_vec),
            None => None,
        };
        let operation = classify_operation(stmts.as_deref());

        if !self.controller.evaluates(&operation) {
            return GuardResult::allow();
        }

        let empty_schema = SchemaContext::default();
        let schema = ctx.schema.unwrap_or(&empty_schema);

        let extracted =
            match queryflux_translation::extract_resources(ctx.sql, ctx.dialect, schema) {
                Ok(r) => r,
                Err(e) => {
                    return match self.on_missing_schema {
                        OnMissingSchema::Deny => GuardResult::deny(
                            format!("access control: could not analyze query: {e}"),
                            "ACCESS_ANALYSIS_FAILED",
                        ),
                        OnMissingSchema::Evaluate => GuardResult::allow(),
                    }
                }
            };

        if extracted.is_empty() {
            // No base tables (e.g. `SELECT 1`) — nothing to decide.
            return GuardResult::allow();
        }

        if self.on_missing_schema == OnMissingSchema::Deny
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

        let session_params: BTreeMap<String, String> = self
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

        let decision = self.controller.evaluate(&request).await;

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
                .flat_map(|p| p.masked_columns.iter().map(|(c, _)| format!("{}.{c}", p.table)))
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

fn classify_operation(stmts: Option<&[Expression]>) -> Operation {
    let kind = stmts
        .and_then(|s| s.first())
        .map(|e| {
            format!("{e:?}")
                .split(['(', ' ', '{'])
                .next()
                .unwrap_or("")
                .to_string()
        })
        .unwrap_or_default();
    match kind.as_str() {
        "Insert" => Operation("table.insert".to_string()),
        "Update" => Operation("table.update".to_string()),
        "Delete" => Operation("table.delete".to_string()),
        _ => Operation::table_select(),
    }
}

fn engine_name(e: &EngineType) -> String {
    format!("{e:?}").to_lowercase()
}
