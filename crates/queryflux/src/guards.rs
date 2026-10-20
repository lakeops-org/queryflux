//! Building auth, authorization, guard chains and access control from YAML or persisted config.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use queryflux_auth::{
    AllowAllAuthorization, LdapAuthProvider, NoneAuthProvider, OidcAuthProvider,
    OpenFgaAuthorizationClient, SimpleAuthorizationPolicy, StaticAuthProvider,
};
use queryflux_frontend::access_control_guard::OpaAccessGuard;
use queryflux_guardrails::{
    built_in::{
        Guard, NoCrossJoinGuard, PartitionPredicateGuard, ReadOnlyGuard, RequirePredicateGuard,
        RowLimitGuard, TimeRangeLimitGuard,
    },
    config::FailBehavior,
    external::{HttpWebhookGuard, MisconfiguredGuard, PythonScriptGuard},
    GuardChain,
};
use queryflux_persistence::{AdminStore, BackendStore, KIND_GUARD};
use tracing::info;

pub(crate) fn build_auth_provider(
    auth: &queryflux_core::config::AuthConfig,
) -> Result<Arc<dyn queryflux_auth::AuthProvider>> {
    use queryflux_core::config::AuthProviderConfig;
    let auth_required = auth.required;
    Ok(match &auth.provider {
        AuthProviderConfig::None => {
            info!("Auth provider: none (network-trust only)");
            Arc::new(NoneAuthProvider::new(auth_required))
        }
        AuthProviderConfig::Static => {
            let users = auth
                .static_users
                .as_ref()
                .context("auth.provider = static requires auth.staticUsers to be configured")?
                .users
                .clone();
            info!(user_count = users.len(), "Auth provider: static");
            Arc::new(StaticAuthProvider::new(users, auth_required))
        }
        AuthProviderConfig::Oidc => {
            let oidc_cfg = auth
                .oidc
                .clone()
                .context("auth.provider = oidc requires auth.oidc to be configured")?;
            info!(issuer = %oidc_cfg.issuer, "Auth provider: OIDC");
            Arc::new(OidcAuthProvider::new(oidc_cfg, auth_required))
        }
        AuthProviderConfig::Ldap => {
            let ldap_cfg = auth
                .ldap
                .clone()
                .context("auth.provider = ldap requires auth.ldap to be configured")?;
            info!(url = %ldap_cfg.url, "Auth provider: LDAP");
            Arc::new(LdapAuthProvider::new(ldap_cfg, auth_required))
        }
    })
}

pub(crate) fn operators_from_auth(
    auth: &queryflux_core::config::AuthConfig,
) -> queryflux_auth::OperatorPolicy {
    queryflux_auth::OperatorPolicy::from_lists(
        auth.operator_roles.clone(),
        auth.operator_groups.clone(),
    )
}

pub(crate) fn build_authorization(
    authz: &queryflux_core::config::AuthorizationConfig,
    cluster_groups: &HashMap<String, queryflux_core::config::ClusterGroupConfig>,
    operators: queryflux_auth::OperatorPolicy,
) -> Result<Arc<dyn queryflux_auth::AuthorizationChecker>> {
    use queryflux_core::config::AuthorizationProviderConfig;
    if !operators.roles.is_empty() || !operators.groups.is_empty() {
        info!(
            operator_roles = ?operators.roles,
            operator_groups = ?operators.groups,
            "Query operators configured (may cancel any query)"
        );
    }
    Ok(match &authz.provider {
        AuthorizationProviderConfig::None => {
            let policies = cluster_groups
                .iter()
                .map(|(name, cfg)| (name.clone(), cfg.authorization.clone()))
                .collect();
            let has_any_policy = cluster_groups.values().any(|cfg| {
                !cfg.authorization.allow_groups.is_empty()
                    || !cfg.authorization.allow_users.is_empty()
            });
            if has_any_policy {
                info!("Authorization: simple allow-list policy");
                Arc::new(SimpleAuthorizationPolicy::new(policies).with_operators(operators))
            } else {
                info!("Authorization: allow-all (no allow-lists configured)");
                Arc::new(AllowAllAuthorization::with_operators(operators))
            }
        }
        AuthorizationProviderConfig::OpenFga => {
            let openfga_cfg = authz.openfga.clone().context(
                "authorization.provider = openfga requires authorization.openfga to be configured",
            )?;
            info!(url = %openfga_cfg.url, store_id = %openfga_cfg.store_id, "Authorization: OpenFGA");
            Arc::new(OpenFgaAuthorizationClient::new(openfga_cfg).with_operators(operators))
        }
    })
}

/// Drops any persisted translation fixup script that fails
/// [`queryflux_translation::sqlglot::validate_fixup_script`] — most commonly one written
/// against the pre-this-PR `transform(ast, src, dst) -> None` contract rather than the
/// current `transform(sql: str, src: str, dst: str) -> str`. Scripts are user/admin-editable
/// (Studio), so a stale one must not block the whole proxy from starting or reloading;
/// dropping just that script (with a loud warning naming the group and index) means the
/// group's other fixups and translation still work, instead of every query for that group
/// failing at translation time on first use.
pub(crate) fn validate_group_translation_scripts(
    scripts: HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    scripts
        .into_iter()
        .map(|(group, group_scripts)| {
            let valid = group_scripts
                .into_iter()
                .enumerate()
                .filter_map(|(i, script)| {
                    match queryflux_translation::sqlglot::validate_fixup_script(&script) {
                        Ok(()) => Some(script),
                        Err(e) => {
                            tracing::error!(
                                group = %group, index = i,
                                "translation fixup script failed validation and will not run: {e}"
                            );
                            None
                        }
                    }
                })
                .collect();
            (group, valid)
        })
        .collect()
}

pub(crate) async fn load_guard_script_bodies(
    store: Option<&dyn AdminStore>,
) -> HashMap<i64, String> {
    let Some(store) = store else {
        return HashMap::new();
    };
    load_guard_script_bodies_from_admin(store).await
}

pub(crate) async fn load_guard_script_bodies_from_admin(
    admin: &dyn AdminStore,
) -> HashMap<i64, String> {
    admin
        .list_user_scripts(Some(KIND_GUARD))
        .await
        .map(|scripts| scripts.into_iter().map(|s| (s.id, s.body)).collect())
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to load guard scripts from persistence: {e}");
            HashMap::new()
        })
}

pub(crate) fn resolve_built_in_guard(
    name: Option<&str>,
    params: queryflux_core::config::BuiltInGuardParams,
) -> Box<dyn Guard> {
    if let Err(reason) = queryflux_core::config::validate_built_in_guard(name, &params) {
        return Box::new(MisconfiguredGuard {
            guard_name: "built_in",
            reason,
        });
    }
    let tables: Vec<(String, String)> = params
        .tables
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    match name {
        Some("read_only") => Box::new(ReadOnlyGuard),
        Some("row_limit") => Box::new(RowLimitGuard {
            max_rows: params.max_rows,
            inject_limit: params.inject_limit.unwrap_or(false),
            default_limit: params.default_limit,
        }),
        Some("require_predicate") => Box::new(RequirePredicateGuard {
            applies_to: params.applies_to.unwrap_or_default(),
        }),
        Some("partition_predicate_required") => Box::new(PartitionPredicateGuard { tables }),
        Some("time_range_limit") => Box::new(TimeRangeLimitGuard {
            tables,
            max_lookback: params
                .max_lookback
                .as_deref()
                .and_then(|l| queryflux_core::config::parse_lookback(l).ok())
                .unwrap_or_else(|| chrono::Duration::days(90)),
        }),
        Some("no_cross_join") => Box::new(NoCrossJoinGuard),
        Some(other) => Box::new(MisconfiguredGuard {
            guard_name: "built_in",
            reason: format!("unsupported built_in guard name \"{other}\""),
        }),
        None => Box::new(MisconfiguredGuard {
            guard_name: "built_in",
            reason: "built_in guard is missing required field \"name\"".to_string(),
        }),
    }
}

pub(crate) fn resolve_python_guard_script(
    inline_script: Option<String>,
    script_id: Option<i64>,
    timeout_ms: Option<u64>,
    guard_script_bodies: &HashMap<i64, String>,
) -> Box<dyn Guard> {
    if let Some(script) = inline_script.filter(|s| !s.trim().is_empty()) {
        return Box::new(PythonScriptGuard { script, timeout_ms });
    }
    if let Some(script_id) = script_id {
        if let Some(script) = guard_script_bodies.get(&script_id) {
            return Box::new(PythonScriptGuard {
                script: script.clone(),
                timeout_ms,
            });
        }
        return Box::new(MisconfiguredGuard {
            guard_name: "python_script",
            reason: format!("python_script guard references missing guard script id {script_id}"),
        });
    }
    Box::new(MisconfiguredGuard {
        guard_name: "python_script",
        reason: "python_script guard requires either script or script_id".to_string(),
    })
}

pub(crate) fn make_http_webhook_guard(
    url: String,
    timeout_ms: Option<u64>,
    retry_count: u32,
    fail_behavior: FailBehavior,
    headers: HashMap<String, String>,
) -> Box<dyn Guard> {
    let raw = url.trim();
    if raw.is_empty() {
        tracing::warn!("http_webhook guard has empty URL; using MisconfiguredGuard");
        return Box::new(MisconfiguredGuard {
            guard_name: "http_webhook",
            reason: "http_webhook guard is missing required field \"url\"".to_string(),
        });
    }
    match reqwest::Url::parse(raw) {
        Ok(parsed) => match parsed.scheme() {
            "http" | "https" => Box::new(HttpWebhookGuard {
                url: raw.to_string(),
                timeout_ms,
                retry_count,
                fail_behavior,
                headers,
                client: reqwest::Client::new(),
            }),
            other => {
                tracing::warn!(
                    scheme = other,
                    "http_webhook guard URL must use http or https; using MisconfiguredGuard"
                );
                Box::new(MisconfiguredGuard {
                    guard_name: "http_webhook",
                    reason: format!(
                        "http_webhook url must use http or https scheme, got \"{other}\""
                    ),
                })
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "http_webhook guard URL is invalid; using MisconfiguredGuard");
            Box::new(MisconfiguredGuard {
                guard_name: "http_webhook",
                reason: format!("http_webhook url is not a valid URL: {e}"),
            })
        }
    }
}

/// Build the data access-control guard at startup.
///
/// A persisted `access_control_config` (Studio / admin API) is authoritative when present —
/// the same contract reload and guardrails follow — so a restart doesn't leave the stored
/// policy inactive behind a missing or weaker YAML section. Without one, the `access_control:`
/// YAML section is used, and `None` when that is absent too.
///
/// Anything that stops a stored policy from being read or built aborts startup: serving
/// queries with a weaker policy than the operator saved would be a silent bypass. The YAML
/// was already validated by `ProxyConfig::validate_startup_security`, so a residual build
/// failure there aborts too rather than quietly disabling enforcement.
pub(crate) async fn build_access_control_guard(
    config: &queryflux_core::config::ProxyConfig,
    backend: Option<&dyn BackendStore>,
) -> Result<Option<Arc<OpaAccessGuard>>> {
    if let Some(store) = backend {
        match store.get_proxy_setting("access_control_config").await {
            Ok(Some(v)) => {
                return apply_stored_access_control(&v).map_err(|e| {
                    anyhow::anyhow!(
                        "stored access_control_config could not be applied; refusing to start \
                         with a weaker policy: {e}"
                    )
                });
            }
            Ok(None) => {}
            Err(e) => anyhow::bail!(
                "could not read stored access_control_config; refusing to start with a \
                 possibly weaker policy: {e}"
            ),
        }
    }
    let Some(cfg) = config.access_control.as_ref() else {
        return Ok(None);
    };
    OpaAccessGuard::try_from_config(cfg)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("access_control config failed to build: {e}"))
}

pub(crate) fn apply_stored_access_control(
    v: &serde_json::Value,
) -> std::result::Result<Option<Arc<OpaAccessGuard>>, String> {
    match queryflux_core::access_config::AccessControlConfig::from_admin_value(v)? {
        None => Ok(None),
        Some(cfg) => Ok(Some(OpaAccessGuard::try_from_config(&cfg)?)),
    }
}

/// Build YAML guard specs into a `GuardChain`. Returns `None` when the list is empty
/// or contains only unrecognised entries.
pub(crate) fn build_chain_from_yaml_specs(
    specs: &[queryflux_core::config::GuardSpecConfig],
    guard_script_bodies: &HashMap<i64, String>,
) -> Option<Arc<GuardChain>> {
    use queryflux_core::config::{GuardFailBehaviorConfig, GuardKindConfig};
    let mut guards: Vec<Box<dyn Guard>> = Vec::new();
    for spec in specs {
        match &spec.kind {
            GuardKindConfig::BuiltIn => {
                guards.push(resolve_built_in_guard(
                    spec.name.as_deref(),
                    spec.params.clone(),
                ));
            }
            GuardKindConfig::PythonScript => {
                let guard = resolve_python_guard_script(
                    spec.script.clone(),
                    spec.script_id,
                    spec.timeout_ms,
                    guard_script_bodies,
                );
                guards.push(guard);
            }
            GuardKindConfig::HttpWebhook => {
                guards.push(make_http_webhook_guard(
                    spec.url.clone().unwrap_or_default(),
                    spec.timeout_ms,
                    spec.retry_count.unwrap_or(0),
                    match spec.fail_behavior {
                        Some(GuardFailBehaviorConfig::Allow) => FailBehavior::Allow,
                        _ => FailBehavior::Deny,
                    },
                    spec.headers.clone().unwrap_or_default(),
                ));
            }
        }
    }
    if guards.is_empty() {
        None
    } else {
        Some(Arc::new(GuardChain::new(guards)))
    }
}

/// Build global + per-group guard chains from the YAML `guardrails:` section.
pub(crate) fn build_guard_chains(
    config: &queryflux_core::config::ProxyConfig,
    guard_script_bodies: &HashMap<i64, String>,
) -> (Option<Arc<GuardChain>>, HashMap<String, Arc<GuardChain>>) {
    let Some(cfg) = config.guardrails.as_ref() else {
        return (None, HashMap::new());
    };
    let global = build_chain_from_yaml_specs(&cfg.global, guard_script_bodies);
    let groups = cfg
        .groups
        .iter()
        .filter_map(|(name, specs)| {
            build_chain_from_yaml_specs(specs, guard_script_bodies)
                .map(|chain| (name.clone(), chain))
        })
        .collect();
    (global, groups)
}

/// Build DB guard specs (kind string format) into a `GuardChain`.
pub(crate) fn build_chain_from_db_specs(
    specs: &serde_json::Value,
    guard_script_bodies: &HashMap<i64, String>,
) -> Option<Arc<GuardChain>> {
    struct DbGuardSpec {
        kind: String,
        name: Option<String>,
        params: queryflux_core::config::BuiltInGuardParams,
        script_id: Option<i64>,
        script: Option<String>,
        url: Option<String>,
        timeout_ms: Option<u64>,
        retry_count: Option<u32>,
        fail_behavior: Option<String>,
        headers: Option<HashMap<String, String>>,
    }
    fn parse_spec(item: &serde_json::Value) -> Option<DbGuardSpec> {
        let o = item.as_object()?;
        Some(DbGuardSpec {
            kind: o.get("kind")?.as_str()?.to_string(),
            name: o
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            // Same keys as YAML. Malformed params fall back to defaults, which then fail validation for guards that need them.
            params: serde_json::from_value(item.clone()).unwrap_or_default(),
            script_id: o.get("script_id").and_then(|v| v.as_i64()),
            script: o.get("script").and_then(|v| v.as_str()).map(str::to_string),
            url: o.get("url").and_then(|v| v.as_str()).map(str::to_string),
            timeout_ms: o.get("timeout_ms").and_then(|v| v.as_u64()),
            retry_count: o
                .get("retry_count")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            fail_behavior: o
                .get("fail_behavior")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            headers: o.get("headers").and_then(|v| v.as_object()).map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            }),
        })
    }
    let arr = specs.as_array()?;
    let mut guards: Vec<Box<dyn Guard>> = Vec::new();
    for item in arr {
        let Some(spec) = parse_spec(item) else {
            guards.push(Box::new(MisconfiguredGuard {
                guard_name: "guard",
                reason: "guard spec is not a valid object with a \"kind\" field".to_string(),
            }));
            continue;
        };
        match spec.kind.as_str() {
            "built_in" => {
                guards.push(resolve_built_in_guard(spec.name.as_deref(), spec.params));
            }
            "http_webhook" => {
                guards.push(make_http_webhook_guard(
                    spec.url.unwrap_or_default(),
                    spec.timeout_ms,
                    spec.retry_count.unwrap_or(0),
                    match spec.fail_behavior.as_deref() {
                        Some("allow") => FailBehavior::Allow,
                        _ => FailBehavior::Deny,
                    },
                    spec.headers.unwrap_or_default(),
                ));
            }
            "python_script" => {
                let guard = resolve_python_guard_script(
                    spec.script,
                    spec.script_id,
                    spec.timeout_ms,
                    guard_script_bodies,
                );
                guards.push(guard);
            }
            other => guards.push(Box::new(MisconfiguredGuard {
                guard_name: "guard",
                reason: format!("unsupported guard kind \"{other}\""),
            })),
        }
    }
    if guards.is_empty() {
        None
    } else {
        Some(Arc::new(GuardChain::new(guards)))
    }
}

/// Build global + per-group guard chains from the flat JSON format stored by the Studio UI.
///
/// The DB format mirrors `GuardrailsConfig` from the TypeScript API types:
/// `{ global: GuardSpecDto[], groups: Record<string, GuardSpecDto[]> }`.
pub(crate) fn build_guard_chains_from_db_value(
    v: &serde_json::Value,
    guard_script_bodies: &HashMap<i64, String>,
) -> (Option<Arc<GuardChain>>, HashMap<String, Arc<GuardChain>>) {
    let Some(obj) = v.as_object() else {
        return (None, HashMap::new());
    };
    let global_val = obj
        .get("global")
        .cloned()
        .unwrap_or(serde_json::Value::Array(vec![]));
    let global = build_chain_from_db_specs(&global_val, guard_script_bodies);

    let groups = obj
        .get("groups")
        .and_then(|g| g.as_object())
        .map(|groups_obj| {
            groups_obj
                .iter()
                .filter_map(|(name, specs)| {
                    build_chain_from_db_specs(specs, guard_script_bodies)
                        .map(|chain| (name.clone(), chain))
                })
                .collect()
        })
        .unwrap_or_default();

    (global, groups)
}
